# Pipeline de pedidos em Kafka com garantias de entrega

Trabalho em dupla da disciplina DCE540. Uma loja fictícia recebe pedidos em CSV, e cada pedido passa por validação, reserva de estoque e pagamento até virar um resultado final. O objetivo era que nenhum pedido se perdesse nem fosse contado duas vezes, mesmo derrubando serviços no meio do processamento, com os eventos trafegando em Avro e um esquema que evolui sem parar o sistema.

Este repositório guarda só o que foi escrito por nós, os quatro serviços e o notebook que orquestra a execução. O kit da atividade (enunciado, dados, gabarito, esquemas e scripts de apoio) é material do professor e não está aqui.

## Os serviços

| Serviço | Garantia | Ideia principal |
|---|---|---|
| `servicos/pedidos.py` | at-least-once | valida cada linha e só confirma o offset depois que tudo foi entregue ao broker |
| `servicos/estoque.py` | at-least-once idempotente | grava saldo e decisão no SQLite antes de publicar, então reprocessar não desconta duas vezes |
| `servicos/pagamento.py` | exactly-once | cada lote é uma transação do Kafka com resultados, logs e offsets juntos |
| `servicos/metricas.py` | at-least-once idempotente | conta cada fato uma vez só, guardando os já contados no SQLite |

O fluxo segue uma saga. Quando o pagamento recusa um pedido, o estoque devolve a reserva, uma única vez por pedido.

## Decisões que valem destacar

- **Cópia ou reprocessamento.** O `pedidos.py` grava nos cabeçalhos de cada mensagem a partição e o offset de origem. Com isso o estoque distingue uma linha repetida no arquivo de uma mensagem relida depois de uma queda.
- **Ordem gravar, publicar, marcar, confirmar.** É o que deixa o estoque seguro para reprocessar. Os comentários do `estoque.py` explicam o que acontece em cada ponto de queda.
- **Evolução de esquema sem trocar código.** Ao subir, cada serviço escreve com a última versão registrada no Schema Registry e lê com o esquema de quem escreveu a mensagem. A passagem da v1 para a v2 exige só reiniciar os serviços, consumidores primeiro.
- **Tempo do evento.** As métricas usam o horário que vem do pedido, nunca o relógio da máquina, então o resultado é o mesmo em qualquer execução.

## Resultado

O verificador do kit compara a saída com um gabarito em 7 critérios. O notebook passou em 7 de 7 derrubando três serviços com `kill` no meio do primeiro lote. Num teste de estresse à parte, com três sementes e quatro rodadas de quedas aleatórias por lote, as três execuções também passaram em 7 de 7.

## Como rodar

Precisa do kit original da atividade, do Docker e de Python 3.10 ou mais novo.

1. Copie a pasta `servicos/` e o `orquestrador.ipynb` para dentro da pasta do kit.
2. Instale as dependências do kit com `pip install -r requirements.txt`.
3. Abra o `orquestrador.ipynb` e execute as células em ordem. Ele sobe o Kafka e o Schema Registry, publica os lotes, injeta as falhas e roda a verificação.

## Diferenças da versão entregue

Depois da entrega, o `pagamento.py` passou a tentar de novo o commit da transação quando o erro é temporário, em vez de encerrar o serviço. Os comentários também ficaram mais completos. O resultado da verificação não muda.

## Próximo passo

Estes conceitos estão sendo reaproveitados num projeto em Node.js com TypeScript, num caso de uso diferente, uma sequência de nutrição de 14 dias por email e SMS.
