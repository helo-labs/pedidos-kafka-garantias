#!/usr/bin/env python3
"""
Serviço de pagamento.

Consome clientes e estoque-reservado e produz pedidos-confirmados, pedidos-recusados
(etapa PAGAMENTO) e logs-eventos.
Exactly-once: cada lote é uma transação com os resultados, os logs e os offsets.
A tabela de clientes fica em memória e é recarregada do tópico a cada início.
"""
import argparse
import json
import os
import signal
from pathlib import Path

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TRANSACTIONAL_ID = os.environ.get("PAGAMENTO_TRANSACTIONAL_ID", "pagamento-1")
RAIZ = Path(__file__).resolve().parent.parent
SCHEMAS = RAIZ / "schemas"

SERVICO = "pagamento"
GRUPO = "grp-pagamento"
T_CLIENTES = "clientes"
T_RESERVADO = "estoque-reservado"
T_CONFIRMADOS = "pedidos-confirmados"
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


def aplicar_cliente(clientes, msg):
    try:
        registro = json.loads(msg.value())
    except (TypeError, ValueError):
        return
    cliente_id = str(registro.get("cliente_id") or "").strip()
    if cliente_id:
        clientes[cliente_id] = registro


def carregar_clientes():
    """Lê o tópico clientes do início ao fim."""
    clientes = {}
    consumidor = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"{GRUPO}-carga",          # só assign, sem commit
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "isolation.level": "read_committed",
    })
    try:
        meta = consumidor.list_topics(T_CLIENTES, timeout=10).topics[T_CLIENTES]
        particoes = [TopicPartition(T_CLIENTES, p, OFFSET_BEGINNING) for p in meta.partitions]
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
            aplicar_cliente(clientes, msg)
    finally:
        consumidor.close()
    print(f"[{SERVICO}] clientes carregados: {len(clientes)}", flush=True)
    return clientes


def decidir(clientes, evento):
    """Motivo da recusa, ou None se confirmado."""
    cliente = clientes.get(evento["cliente_id"])
    if cliente is None:
        return "CLIENTE_INEXISTENTE"
    if str(cliente.get("status") or "").strip().lower() != "ativo":
        return "CLIENTE_BLOQUEADO"
    try:
        limite = float(cliente.get("limite_pedido"))
    except (TypeError, ValueError):
        limite = 0.0
    if evento["valor"] > limite:
        return "LIMITE_EXCEDIDO"
    return None


def voltar_ao_ultimo_commit(consumidor):
    """Volta ao último offset confirmado depois de abortar.

    A posição do consumidor já tinha avançado sobre o lote abortado. Sem o seek, o lote
    seria pulado e os pedidos dele sumiriam.
    """
    for tp in consumidor.committed(consumidor.assignment(), timeout=10):
        if tp.offset < 0:
            tp.offset = OFFSET_BEGINNING
        consumidor.seek(tp)


def confirmar(produtor):
    """commit_transaction com nova tentativa nos erros temporários.

    Uma oscilação do broker no commit é retriable, e a própria biblioteca orienta chamar
    commit_transaction de novo. Encerrar o serviço também seria seguro, porque a transação
    seria abortada no reinício, mas deixaria o pipeline parado à toa.
    """
    while True:
        try:
            produtor.commit_transaction()
            return
        except KafkaException as e:
            if not e.args[0].retriable():
                raise
            print(f"[{SERVICO}] commit da transação falhou temporariamente, tentando de novo", flush=True)


# ---------------------------------------------------------------------------
# Serviço
# ---------------------------------------------------------------------------
def main():
    argparse.ArgumentParser(description="Serviço de pagamento (exactly-once)").parse_args()
    signal.signal(signal.SIGINT, _sinal)
    signal.signal(signal.SIGTERM, _sinal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _sinal)

    garantir_topicos([T_CLIENTES, T_RESERVADO, T_CONFIRMADOS, T_RECUSADOS, T_LOGS])
    registry = SchemaRegistryClient({"url": REGISTRY})
    sch_conf, v_conf = schema_atual(registry, f"{T_CONFIRMADOS}-value", "PedidoEvento.v1.avsc")
    sch_rec, v_rec = schema_atual(registry, f"{T_RECUSADOS}-value", "PedidoEvento.v1.avsc")
    serializadores = {
        T_CONFIRMADOS: AvroSerializer(registry, sch_conf, conf={"auto.register.schemas": False}),
        T_RECUSADOS: AvroSerializer(registry, sch_rec, conf={"auto.register.schemas": False}),
        T_LOGS: AvroSerializer(registry, (SCHEMAS / "LogEvento.v1.avsc").read_text(encoding="utf-8")),
    }
    desserializador = AvroDeserializer(registry)       # schema de quem escreveu (v1 ou v2)
    print(f"[{SERVICO}] PedidoEvento versão {v_conf} ({T_CONFIRMADOS}) / {v_rec} ({T_RECUSADOS}) | "
          f"LogEvento v1 | transactional.id={TRANSACTIONAL_ID}", flush=True)

    # A tabela de clientes é pequena e o tópico é a fonte da verdade, então recarregar a
    # cada início sai mais simples que guardar uma cópia em disco que poderia desatualizar.
    clientes = carregar_clientes()

    consumidor = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GRUPO,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    produtor = Producer({
        "bootstrap.servers": BOOTSTRAP,
        # Fixo por instância. Com um id aleatório a instância nova não teria como abortar a
        # transação que a anterior deixou aberta ao ser derrubada.
        "transactional.id": TRANSACTIONAL_ID,
        "enable.idempotence": True,
        "acks": "all",
    })
    produtor.init_transactions()                   # aborta a transação pendente de uma queda anterior

    def produzir(topico, chave, valor):
        produtor.produce(topico, key=chave,
                         value=serializadores[topico](valor, SerializationContext(topico, MessageField.VALUE)))

    def log(evento, nivel, ev):
        produzir(T_LOGS, ev["pedido_id"], {"servico": SERVICO, "nivel": nivel, "evento": evento,
                                           "pedido_id": ev["pedido_id"], "ts_evento": ev["ts_evento"]})

    def processar(msg):
        if msg.topic() == T_CLIENTES:
            aplicar_cliente(clientes, msg)
            return
        ev = desserializador(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        motivo = decidir(clientes, ev)
        if motivo is None:
            ev["etapa"], ev["motivo"] = None, None
            produzir(T_CONFIRMADOS, ev["pedido_id"], ev)
            log("PAGAMENTO_CONFIRMADO", "INFO", ev)
        else:
            ev["etapa"], ev["motivo"] = "PAGAMENTO", motivo
            produzir(T_RECUSADOS, ev["pedido_id"], ev)
            log("PAGAMENTO_RECUSADO", "ERROR", ev)

    consumidor.subscribe([T_CLIENTES, T_RESERVADO])
    try:
        while not parar:
            msgs = consumidor.consume(num_messages=100, timeout=1.0)
            if not msgs:
                continue
            produtor.begin_transaction()
            try:
                for msg in msgs:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            print(f"[{SERVICO}] erro de consumo: {msg.error()}", flush=True)
                        continue
                    processar(msg)
                # Os offsets vão dentro da transação, e não por consumidor.commit(). Assim o
                # avanço da leitura e as mensagens produzidas são confirmados juntos ou nenhum.
                posicoes = [tp for tp in consumidor.position(consumidor.assignment()) if tp.offset >= 0]
                produtor.send_offsets_to_transaction(posicoes, consumidor.consumer_group_metadata())
                confirmar(produtor)
            except KafkaException as e:
                erro = e.args[0]
                if not erro.txn_requires_abort():
                    raise                                  # fatal, por exemplo outra instância com o mesmo id
                print(f"[{SERVICO}] transação abortada: {erro}", flush=True)
                produtor.abort_transaction()
                voltar_ao_ultimo_commit(consumidor)
                continue
            print(f"[{SERVICO}] transação confirmada ({len(msgs)} mensagens)", flush=True)
    finally:
        consumidor.close()
        print(f"[{SERVICO}] encerrado", flush=True)


if __name__ == "__main__":
    main()
