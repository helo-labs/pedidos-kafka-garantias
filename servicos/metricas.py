#!/usr/bin/env python3
"""
Métricas dos logs por janela de 1 minuto (UTC, pelo ts_evento), servico e nivel.

At-least-once e idempotente, com o estado em estado/metricas.db. Cada fato
(servico, pedido_id, evento) conta uma vez só.

Uso:
    python servicos/metricas.py
    python servicos/metricas.py --exportar
"""
import argparse
import csv
import os
import signal
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
RAIZ = Path(__file__).resolve().parent.parent
SCHEMAS = RAIZ / "schemas"
BANCO = RAIZ / "estado" / "metricas.db"

SERVICO = "metricas"
GRUPO = "grp-metricas"
T_LOGS = "logs-eventos"
PARTICOES = 4
SEM_JANELA = "sem_janela"

parar = False


def _sinal(*_):
    global parar
    parar = True


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


def abrir_banco():
    BANCO.parent.mkdir(exist_ok=True)
    banco = sqlite3.connect(BANCO)
    banco.executescript("""
        CREATE TABLE IF NOT EXISTS vistos (
            servico TEXT NOT NULL, pedido_id TEXT NOT NULL, evento TEXT NOT NULL,
            PRIMARY KEY (servico, pedido_id, evento)
        );
        CREATE TABLE IF NOT EXISTS contagens (
            janela_minuto TEXT NOT NULL, servico TEXT NOT NULL, nivel TEXT NOT NULL,
            quantidade INTEGER NOT NULL,
            PRIMARY KEY (janela_minuto, servico, nivel)
        );
    """)
    banco.commit()
    return banco


def janela(ts):
    """ts_evento para 'AAAA-MM-DDTHH:MM' em UTC."""
    if ts is None:
        return SEM_JANELA
    if isinstance(ts, (int, float)):
        ts = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M")


def contar(banco, log):
    """Conta o log se o fato ainda não foi contado.

    A chave é (servico, pedido_id, evento) e não o offset da mensagem, porque o mesmo log
    pode ser publicado duas vezes pelo serviço de origem, em offsets diferentes.
    """
    novo = banco.execute("INSERT OR IGNORE INTO vistos VALUES (?, ?, ?)",
                         (log["servico"], log["pedido_id"], log["evento"])).rowcount
    if novo:
        banco.execute("INSERT INTO contagens VALUES (?, ?, ?, 1) ON CONFLICT (janela_minuto, servico, nivel) "
                      "DO UPDATE SET quantidade = quantidade + 1",
                      (janela(log.get("ts_evento")), log["servico"], log["nivel"]))
    return novo


def exportar(pasta):
    banco = abrir_banco()
    linhas = banco.execute("SELECT janela_minuto, servico, nivel, quantidade FROM contagens "
                           "ORDER BY janela_minuto, servico, nivel").fetchall()
    banco.close()
    pasta.mkdir(parents=True, exist_ok=True)
    with open(pasta / "metricas.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["janela_minuto", "servico", "nivel", "quantidade"])
        w.writerows(linhas)
    print(f"{pasta / 'metricas.csv'}: {len(linhas)} linhas")


def rodar():
    signal.signal(signal.SIGINT, _sinal)
    signal.signal(signal.SIGTERM, _sinal)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _sinal)

    garantir_topicos([T_LOGS])
    registry = SchemaRegistryClient({"url": REGISTRY})
    desserializador = AvroDeserializer(registry, schema_str=(SCHEMAS / "LogEvento.v1.avsc").read_text(encoding="utf-8"))
    banco = abrir_banco()
    consumidor = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GRUPO,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    consumidor.subscribe([T_LOGS])
    try:
        while not parar:
            msgs = consumidor.consume(num_messages=200, timeout=1.0)
            if not msgs:
                continue
            novos = 0
            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[{SERVICO}] erro de consumo: {msg.error()}", flush=True)
                    continue
                log = desserializador(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                novos += contar(banco, log)
            # Fatos e contadores no mesmo commit do SQLite, antes do offset. Se cair depois
            # disso, o lote é relido e os fatos já vistos não contam de novo.
            banco.commit()
            try:
                consumidor.commit(asynchronous=False)
            except KafkaException as e:
                if e.args[0].code() != KafkaError._NO_OFFSET:
                    raise
            print(f"[{SERVICO}] lote de {len(msgs)} logs ({novos} novos, {len(msgs) - novos} repetidos)", flush=True)
    finally:
        consumidor.close()
        banco.close()
        print(f"[{SERVICO}] encerrado", flush=True)


def main():
    ap = argparse.ArgumentParser(description="Métricas por janela de 1 minuto a partir dos logs")
    ap.add_argument("--exportar", action="store_true", help="Grava saida/metricas.csv e termina")
    ap.add_argument("--saida", default="saida", help="Pasta de saída do --exportar (padrão: saida)")
    args = ap.parse_args()
    if args.exportar:
        exportar(Path(args.saida))
    else:
        rodar()


if __name__ == "__main__":
    main()
