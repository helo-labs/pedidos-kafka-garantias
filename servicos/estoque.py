#!/usr/bin/env python3
"""
Serviço de reserva de estoque, com a compensação da saga.

Consome estoque-inicial, pedidos-validados e pedidos-recusados (etapa PAGAMENTO).
Produz estoque-reservado, pedidos-recusados (etapa ESTOQUE) e logs-eventos.
At-least-once e idempotente, com o estado em estado/estoque.db.

Ordem de cada lote:
    1. grava saldo e decisões no SQLite
    2. publica e espera o flush
    3. marca as decisões como publicadas
    4. confirma o offset

Gravar o estado antes de publicar é o que deixa o reprocessamento seguro. Se o serviço
cair entre 1 e 2, a decisão já está no banco e é só republicada, sem descontar o saldo de
novo. Na ordem inversa, publicar antes de gravar, uma queda no meio faria o pedido ser
decidido duas vezes. Ainda sobra uma janela entre 2 e 3, sem acesso à rede, em que uma
queda republicaria o evento.

Uso:
    python servicos/estoque.py
    python servicos/estoque.py --exportar
"""
import argparse
import csv
import json
import os
import signal
import sqlite3
from datetime import datetime
from pathlib import Path

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
RAIZ = Path(__file__).resolve().parent.parent
SCHEMAS = RAIZ / "schemas"
BANCO = RAIZ / "estado" / "estoque.db"

SERVICO = "estoque"
GRUPO = "grp-estoque"
T_INICIAL = "estoque-inicial"
T_VALIDADOS = "pedidos-validados"
T_RESERVADO = "estoque-reservado"
T_RECUSADOS = "pedidos-recusados"
T_LOGS = "logs-eventos"
PARTICOES = 4

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
    banco.executescript("""
        CREATE TABLE IF NOT EXISTS saldo (sku TEXT PRIMARY KEY, quantidade INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS pedidos (
            pedido_id  TEXT PRIMARY KEY,
            origem     TEXT NOT NULL,      -- partição:offset da 1a ocorrência no tópico pedidos
            sku        TEXT NOT NULL,
            quantidade INTEGER NOT NULL,
            resultado  TEXT NOT NULL,      -- RESERVADO ou RECUSADO
            evento     TEXT NOT NULL,      -- evento de saída em JSON
            publicado  INTEGER NOT NULL DEFAULT 0,
            devolvido  INTEGER NOT NULL DEFAULT 0
        );
    """)
    banco.commit()
    return banco


def em_ms(ts):
    """datetime lido do Avro para milissegundos."""
    if isinstance(ts, datetime):
        return int(ts.timestamp() * 1000)
    return ts


def inteiro(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def aplicar_estoque_inicial(banco, msg):
    """Reler o tópico não sobrescreve o saldo atual."""
    try:
        registro = json.loads(msg.value())
    except (TypeError, ValueError):
        return
    sku = str(registro.get("sku") or "").strip()
    quantidade = inteiro(registro.get("quantidade"))
    if sku and quantidade is not None:
        banco.execute("INSERT OR IGNORE INTO saldo VALUES (?, ?)", (sku, quantidade))


def carregar_estoque_inicial(banco):
    """Lê estoque-inicial do início ao fim antes de reservar qualquer pedido."""
    consumidor = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"{GRUPO}-carga",          # só assign, sem commit
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "isolation.level": "read_committed",
    })
    try:
        meta = consumidor.list_topics(T_INICIAL, timeout=10).topics[T_INICIAL]
        particoes = [TopicPartition(T_INICIAL, p, OFFSET_BEGINNING) for p in meta.partitions]
        consumidor.assign(particoes)
        pendentes = {tp.partition for tp in particoes}
        while pendentes and not parar:
            msg = consumidor.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pendentes.discard(msg.partition())
                    continue
                raise KafkaException(msg.error())
            aplicar_estoque_inicial(banco, msg)
        banco.commit()
    finally:
        consumidor.close()
    total = banco.execute("SELECT COUNT(*) FROM saldo").fetchone()[0]
    print(f"[{SERVICO}] estoque inicial carregado: {total} skus", flush=True)


def exportar(pasta):
    banco = abrir_banco()
    linhas = banco.execute("SELECT sku, quantidade FROM saldo ORDER BY sku").fetchall()
    banco.close()
    pasta.mkdir(parents=True, exist_ok=True)
    with open(pasta / "estoque_final.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["sku", "quantidade"])
        w.writerows(linhas)
    print(f"{pasta / 'estoque_final.csv'}: {len(linhas)} skus")


def commit_offsets(consumidor):
    try:
        consumidor.commit(asynchronous=False)
    except KafkaException as e:
        if e.args[0].code() != KafkaError._NO_OFFSET:
            raise


# ---------------------------------------------------------------------------
# Serviço
# ---------------------------------------------------------------------------
def rodar():
    signal.signal(signal.SIGINT, _sinal)
    signal.signal(signal.SIGTERM, _sinal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _sinal)

    garantir_topicos([T_INICIAL, T_VALIDADOS, T_RESERVADO, T_RECUSADOS, T_LOGS])
    registry = SchemaRegistryClient({"url": REGISTRY})
    sch_reservado, v_res = schema_atual(registry, f"{T_RESERVADO}-value", "PedidoEvento.v1.avsc")
    sch_recusado, v_rec = schema_atual(registry, f"{T_RECUSADOS}-value", "PedidoEvento.v1.avsc")
    serializadores = {
        T_RESERVADO: AvroSerializer(registry, sch_reservado, conf={"auto.register.schemas": False}),
        T_RECUSADOS: AvroSerializer(registry, sch_recusado, conf={"auto.register.schemas": False}),
        T_LOGS: AvroSerializer(registry, (SCHEMAS / "LogEvento.v1.avsc").read_text(encoding="utf-8")),
    }
    desserializador = AvroDeserializer(registry)       # schema de quem escreveu (v1 ou v2)
    print(f"[{SERVICO}] PedidoEvento versão {v_res} ({T_RESERVADO}) / {v_rec} ({T_RECUSADOS}) | LogEvento v1",
          flush=True)

    banco = abrir_banco()
    carregar_estoque_inicial(banco)

    consumidor = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GRUPO,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    produtor = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all"})
    erros_entrega = []

    def ao_entregar(err, _msg):
        if err is not None:
            erros_entrega.append(err)

    def log(saidas, evento, nivel, pedido_id, ts):
        saidas.append((T_LOGS, pedido_id, {"servico": SERVICO, "nivel": nivel, "evento": evento,
                                           "pedido_id": pedido_id, "ts_evento": ts}))

    def saidas_da_decisao(saidas, linha_evento, resultado):
        evento = json.loads(linha_evento)
        if resultado == "RESERVADO":
            saidas.append((T_RESERVADO, evento["cliente_id"], evento))
            log(saidas, "ESTOQUE_RESERVADO", "INFO", evento["pedido_id"], evento["ts_evento"])
        else:
            saidas.append((T_RECUSADOS, evento["pedido_id"], evento))
            log(saidas, "ESTOQUE_RECUSADO", "ERROR", evento["pedido_id"], evento["ts_evento"])

    def reservar(msg, saidas, a_marcar):
        ev = desserializador(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        ev["ts_evento"] = em_ms(ev["ts_evento"])
        # A origem no tópico pedidos separa cópia de reprocessamento. O offset deste tópico
        # não serviria, porque se o pedidos.py reprocessar a mesma linha ela ganha um offset
        # novo aqui e pareceria uma cópia.
        cab = {k: v.decode() for k, v in (msg.headers() or []) if v is not None}
        if "origem-particao" in cab and "origem-offset" in cab:
            origem = f"{cab['origem-particao']}:{cab['origem-offset']}"
        else:
            origem = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
        pid = ev["pedido_id"]
        linha = banco.execute("SELECT origem, resultado, evento, publicado FROM pedidos WHERE pedido_id = ?",
                              (pid,)).fetchone()
        if linha is not None:
            origem_ant, resultado, evento, publicado = linha
            if origem_ant != origem:                       # cópia da linha no arquivo
                log(saidas, "PEDIDO_DUPLICADO", "WARN", pid, ev["ts_evento"])
            elif not publicado:                            # caiu entre gravar e publicar
                saidas_da_decisao(saidas, evento, resultado)
                a_marcar.append(pid)
            return                                         # já publicado: nada a fazer

        saldo = banco.execute("SELECT quantidade FROM saldo WHERE sku = ?", (ev["sku"],)).fetchone()
        if saldo is None:
            resultado, motivo = "RECUSADO", "SKU_INEXISTENTE"
        elif saldo[0] < ev["quantidade"]:
            resultado, motivo = "RECUSADO", "SEM_ESTOQUE"
        else:
            resultado, motivo = "RESERVADO", None
            banco.execute("UPDATE saldo SET quantidade = quantidade - ? WHERE sku = ?",
                          (ev["quantidade"], ev["sku"]))
        if resultado == "RECUSADO":
            ev["etapa"], ev["motivo"] = "ESTOQUE", motivo
        evento = json.dumps(ev, ensure_ascii=False)
        banco.execute("INSERT INTO pedidos (pedido_id, origem, sku, quantidade, resultado, evento) "
                      "VALUES (?, ?, ?, ?, ?, ?)", (pid, origem, ev["sku"], ev["quantidade"], resultado, evento))
        saidas_da_decisao(saidas, evento, resultado)
        a_marcar.append(pid)

    def compensar(msg, saidas):
        ev = desserializador(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        if ev.get("etapa") != "PAGAMENTO":
            return
        pid = ev["pedido_id"]
        linha = banco.execute("SELECT sku, quantidade, resultado, devolvido FROM pedidos WHERE pedido_id = ?",
                              (pid,)).fetchone()
        if linha is None or linha[2] != "RESERVADO":
            print(f"[{SERVICO}] {pid}: recusa no pagamento sem reserva conhecida, ignorada", flush=True)
            return
        sku, quantidade, _, devolvido = linha
        if not devolvido:                                  # a devolução acontece uma vez só
            banco.execute("UPDATE saldo SET quantidade = quantidade + ? WHERE sku = ?", (quantidade, sku))
            banco.execute("UPDATE pedidos SET devolvido = 1 WHERE pedido_id = ?", (pid,))
        # O log sai de novo num reprocessamento. Não há problema, a métrica conta cada
        # fato uma vez, e isso evita guardar mais uma marca de publicação só para o log.
        log(saidas, "ESTOQUE_DEVOLVIDO", "INFO", pid, em_ms(ev["ts_evento"]))

    # Um consumidor só para os três tópicos. Com consumidores separados, a ordem entre
    # reserva e devolução dependeria de dois loops independentes.
    consumidor.subscribe([T_INICIAL, T_VALIDADOS, T_RECUSADOS])
    try:
        while not parar:
            msgs = consumidor.consume(num_messages=100, timeout=1.0)
            if not msgs:
                continue
            saidas, a_marcar = [], []
            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[{SERVICO}] erro de consumo: {msg.error()}", flush=True)
                    continue
                if msg.topic() == T_INICIAL:
                    aplicar_estoque_inicial(banco, msg)
                elif msg.topic() == T_VALIDADOS:
                    reservar(msg, saidas, a_marcar)
                else:
                    compensar(msg, saidas)
            banco.commit()                                  # 1
            for topico, chave, valor in saidas:             # 2
                produtor.produce(topico, key=chave,
                                 value=serializadores[topico](valor, SerializationContext(topico, MessageField.VALUE)),
                                 on_delivery=ao_entregar)
            produtor.flush()
            if erros_entrega:
                raise KafkaException(erros_entrega[0])
            if a_marcar:                                    # 3
                banco.executemany("UPDATE pedidos SET publicado = 1 WHERE pedido_id = ?", [(p,) for p in a_marcar])
                banco.commit()
            commit_offsets(consumidor)                      # 4
            print(f"[{SERVICO}] lote de {len(msgs)} mensagens confirmado ({len(saidas)} publicações)", flush=True)
    finally:
        produtor.flush(10)
        consumidor.close()
        banco.close()
        print(f"[{SERVICO}] encerrado", flush=True)


def main():
    ap = argparse.ArgumentParser(description="Serviço de reserva de estoque")
    ap.add_argument("--exportar", action="store_true", help="Grava saida/estoque_final.csv e termina")
    ap.add_argument("--saida", default="saida", help="Pasta de saída do --exportar (padrão: saida)")
    args = ap.parse_args()
    if args.exportar:
        exportar(Path(args.saida))
    else:
        rodar()


if __name__ == "__main__":
    main()
