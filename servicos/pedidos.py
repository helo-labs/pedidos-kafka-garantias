#!/usr/bin/env python3
"""
Serviço de validação de pedidos.

Consome pedidos e produz pedidos-validados, pedidos-dlq e logs-eventos.
At-least-once: o offset só é confirmado depois do flush do lote.
A mensagem validada leva a partição e o offset de origem nos cabeçalhos.
"""
import argparse
import json
import math
import os
import re
import signal
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
RAIZ = Path(__file__).resolve().parent.parent
SCHEMAS = RAIZ / "schemas"
BANCO = RAIZ / "estado" / "pedidos.db"

SERVICO = "pedidos"
GRUPO = "grp-pedidos"
T_ENTRADA = "pedidos"
T_VALIDADOS = "pedidos-validados"
T_DLQ = "pedidos-dlq"
T_LOGS = "logs-eventos"
PARTICOES = 4

OBRIGATORIOS = ["pedido_id", "cliente_id", "sku", "quantidade", "valor", "timestamp"]
INTEIRO = re.compile(r"^[+-]?\d+$")
INT_MAX = 2**31 - 1   # limite do tipo int do Avro

parar = False


def _sinal(*_):
    global parar
    parar = True


# ---------------------------------------------------------------------------
# Infraestrutura
# ---------------------------------------------------------------------------
def garantir_topicos(nomes):
    """Cria os tópicos que ainda não existem."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existentes = admin.list_topics(timeout=10).topics
    novos = [NewTopic(t, num_partitions=PARTICOES, replication_factor=1) for t in nomes if t not in existentes]
    for futuro in admin.create_topics(novos).values() if novos else []:
        try:
            futuro.result()
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def schema_atual(registry, subject, arquivo_padrao):
    """Última versão registrada no subject. Se estiver vazio, registra o arquivo padrão."""
    try:
        reg = registry.get_latest_version(subject)
        return reg.schema.schema_str, reg.version
    except SchemaRegistryError as e:
        if e.http_status_code != 404:
            raise
    texto = (SCHEMAS / arquivo_padrao).read_text(encoding="utf-8")
    registry.register_schema(subject, Schema(texto, "AVRO"))
    return texto, registry.get_latest_version(subject).version


def abrir_banco():
    BANCO.parent.mkdir(exist_ok=True)
    banco = sqlite3.connect(BANCO)
    banco.execute("CREATE TABLE IF NOT EXISTS dlq (pedido_id TEXT PRIMARY KEY, particao INT, offset INT)")
    banco.commit()
    return banco


# ---------------------------------------------------------------------------
# Regras de validação
# ---------------------------------------------------------------------------
def vazio(v):
    return v is None or (isinstance(v, str) and v.strip() == "")


def quantidade_valida(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, str) and INTEIRO.match(v.strip()):
        v = int(v.strip())
    if isinstance(v, int):
        return v if 0 < v <= INT_MAX else None
    return None


def valor_valido(v):
    if isinstance(v, bool):
        return None
    try:
        n = float(v.strip()) if isinstance(v, str) else float(v)
    except (TypeError, ValueError):
        return None
    return n if math.isfinite(n) and n > 0 else None


def ts_em_ms(v):
    """ISO 8601 ou epoch para milissegundos UTC. None se não der para interpretar."""
    if vazio(v):
        return None
    try:
        n = float(v)
        if not math.isfinite(n):
            return None
        return int(n * 1000) if n < 1e11 else int(n)
    except (TypeError, ValueError):
        pass
    try:
        dt = datetime.fromisoformat(str(v).strip())
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def validar(registro):
    """(motivo, None) se inválido ou (None, evento) se válido."""
    if not isinstance(registro, dict) or any(vazio(registro.get(c)) for c in OBRIGATORIOS):
        return "CAMPO_AUSENTE", None
    ts = ts_em_ms(registro["timestamp"])
    if ts is None:
        return "CAMPO_AUSENTE", None          # timestamp ilegível conta como ausente
    quantidade = quantidade_valida(registro["quantidade"])
    if quantidade is None:
        return "QUANTIDADE_INVALIDA", None
    valor = valor_valido(registro["valor"])
    if valor is None:
        return "VALOR_INVALIDO", None
    evento = {
        "pedido_id": str(registro["pedido_id"]).strip(),
        "cliente_id": str(registro["cliente_id"]).strip(),
        "sku": str(registro["sku"]).strip(),
        "quantidade": quantidade,
        "valor": valor,
        "ts_evento": ts,
        "etapa": None,
        "motivo": None,
    }
    if not vazio(registro.get("canal_venda")):
        evento["canal_venda"] = str(registro["canal_venda"]).strip()
    return None, evento


# ---------------------------------------------------------------------------
# Serviço
# ---------------------------------------------------------------------------
def main():
    argparse.ArgumentParser(description="Serviço de validação de pedidos").parse_args()
    signal.signal(signal.SIGINT, _sinal)
    signal.signal(signal.SIGTERM, _sinal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _sinal)

    garantir_topicos([T_ENTRADA, T_VALIDADOS, T_DLQ, T_LOGS])
    registry = SchemaRegistryClient({"url": REGISTRY})
    schema_pedido, versao = schema_atual(registry, f"{T_VALIDADOS}-value", "PedidoEvento.v1.avsc")
    ser_pedido = AvroSerializer(registry, schema_pedido, conf={"auto.register.schemas": False})
    ser_log = AvroSerializer(registry, (SCHEMAS / "LogEvento.v1.avsc").read_text(encoding="utf-8"))
    print(f"[{SERVICO}] PedidoEvento versão {versao} | LogEvento v1", flush=True)

    banco = abrir_banco()
    consumidor = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GRUPO,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    # Produtor idempotente para que uma nova tentativa interna de envio não duplique a
    # mensagem no tópico. Duplicata por reprocessamento é outro caso, tratado no estoque.
    produtor = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all"})
    erros_entrega = []

    def ao_entregar(err, _msg):
        if err is not None:
            erros_entrega.append(err)

    def publicar_log(evento, nivel, pedido_id, ts):
        log = {"servico": SERVICO, "nivel": nivel, "evento": evento, "pedido_id": pedido_id, "ts_evento": ts}
        produtor.produce(T_LOGS, key=pedido_id,
                         value=ser_log(log, SerializationContext(T_LOGS, MessageField.VALUE)),
                         on_delivery=ao_entregar)

    def processar(msg):
        try:
            registro = json.loads(msg.value())
        except (TypeError, ValueError):
            registro = None
        motivo, evento = validar(registro)
        if evento is not None:
            # A origem vai no cabeçalho e não no evento porque o PedidoEvento é contrato fixo
            # do enunciado. Mudar o schema só para isso quebraria os outros consumidores.
            produtor.produce(T_VALIDADOS, key=evento["sku"],
                             value=ser_pedido(evento, SerializationContext(T_VALIDADOS, MessageField.VALUE)),
                             headers={"origem-particao": str(msg.partition()), "origem-offset": str(msg.offset())},
                             on_delivery=ao_entregar)
            publicar_log("PEDIDO_VALIDADO", "INFO", evento["pedido_id"], evento["ts_evento"])
            return
        registro = registro if isinstance(registro, dict) else {}
        pedido_id = "" if vazio(registro.get("pedido_id")) else str(registro["pedido_id"]).strip()
        if pedido_id:
            # Cópia de linha inválida já enviada à DLQ. Ninguém consome a DLQ, então uma
            # repetição ali não seria absorvida por nenhum serviço mais adiante.
            anterior = banco.execute("SELECT particao, offset FROM dlq WHERE pedido_id = ?", (pedido_id,)).fetchone()
            if anterior is not None and anterior != (msg.partition(), msg.offset()):
                print(f"[{SERVICO}] {pedido_id}: cópia de pedido inválido descartada", flush=True)
                return
            banco.execute("INSERT OR IGNORE INTO dlq VALUES (?, ?, ?)", (pedido_id, msg.partition(), msg.offset()))
        original = registro if registro else (msg.value() or b"").decode("utf-8", "replace")
        dlq = {"pedido_id": pedido_id or None, "motivo": motivo, "original": original}
        produtor.produce(T_DLQ, key=pedido_id or None, value=json.dumps(dlq, ensure_ascii=False).encode("utf-8"),
                         on_delivery=ao_entregar)
        publicar_log("PEDIDO_INVALIDO", "ERROR", pedido_id, ts_em_ms(registro.get("timestamp")))

    consumidor.subscribe([T_ENTRADA])
    try:
        while not parar:
            msgs = consumidor.consume(num_messages=100, timeout=1.0)
            if not msgs:
                continue
            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[{SERVICO}] erro de consumo: {msg.error()}", flush=True)
                    continue
                processar(msg)
            # O commit só depois do flush é o que faz ser at-least-once. Confirmar o offset
            # antes de publicar perderia o lote numa queda entre os dois passos.
            produtor.flush()
            if erros_entrega:
                raise KafkaException(erros_entrega[0])
            banco.commit()
            try:
                consumidor.commit(asynchronous=False)
            except KafkaException as e:
                if e.args[0].code() != KafkaError._NO_OFFSET:
                    raise
            print(f"[{SERVICO}] lote de {len(msgs)} mensagens confirmado", flush=True)
    finally:
        produtor.flush(10)
        consumidor.close()
        banco.close()
        print(f"[{SERVICO}] encerrado", flush=True)


if __name__ == "__main__":
    main()
