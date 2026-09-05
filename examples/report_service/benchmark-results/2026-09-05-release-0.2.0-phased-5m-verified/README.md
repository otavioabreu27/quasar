# Benchmark verificado — Quasar 0.2.0

Data: 2026-09-05  
Imagem: `ghcr.io/otavioabreu27/report_service:sha-852f666`  
Digest: `sha256:937c290b7e34028565c0d9202f01222b3224b11186178737f8357a86a4a7fc21`

## Parecer executivo

A release é estável e elástica até 300 jobs/s neste ambiente. A fase de 500
jobs/s ultrapassou o envelope sem erros: 15 dos 69.000 submits receberam HTTP
503, 24 polls receberam HTTP 503 e 7 jobs aceitos precisaram de uma segunda
tentativa. Todos os 68.985 jobs aceitos terminaram, não houve status terminal
de falha, timeout de drenagem, rollback PostgreSQL ou restart de pod.

O resultado é promissor, mas 500 jobs/s não pode ser anunciado como capacidade
sustentada com SLO estrito. O limite operacional comprovado por este teste é
300 jobs/s; a faixa de 400–500 jobs/s deve ser tratada como zona de saturação.

## Metodologia

- gerador open-loop dentro do k3s, no `k3s-master`;
- aplicação isolada nos dois workers durante a carga;
- HPA iniciado em 2 pods e liberado até 8 pods;
- cinco fases de 60 segundos: 50, 150, 300, 500 e 150 jobs/s;
- cada job calcula a soma de 1 até 1.000.000 e persiste o relatório;
- PostgreSQL observado por um pod interno, sem túnel e sem port-forward;
- CPU e memória filtradas apenas pelos pods do `report_service`;
- amostragem temporal a cada 5 segundos;
- sem retry de submit no gerador: rejeições são parte do resultado.

## Resultado HTTP e jobs

| Métrica | Resultado |
|---|---:|
| Jobs programados | 69.000 |
| Jobs aceitos e concluídos | 68.985 |
| Submits HTTP 503 | 15 (0,0217%) |
| Polls HTTP 503 | 24 de 70.415 (0,0341%) |
| Jobs na primeira tentativa | 68.978 |
| Jobs na segunda tentativa | 7 (0,0101% dos aceitos) |
| Falhas terminais | 0 |
| Backlog máximo observado pelo cliente | 384 |
| Throughput médio do cenário | 229,87 jobs/s |
| Tempo total, incluindo drenagem | 300,11 s |

Latências agregadas das cinco fases:

| Caminho | p50 | p95 | p99 | máximo |
|---|---:|---:|---:|---:|
| Enqueue HTTP | 61,11 ms | 513,16 ms | 765,97 ms | 1.758,95 ms |
| Job no servidor | 16 ms | 178 ms | 465 ms | 30.480 ms |
| End-to-end observado | 59,28 ms | 813,47 ms | 1.126,52 ms | 31.478,24 ms |
| Poll HTTP | 7,73 ms | 485,44 ms | 748,54 ms | 1.596,51 ms |
| Atraso do gerador | 1,00 ms | 7.097,46 ms | 7.728,59 ms | 7.952,24 ms |

O atraso do gerador concentra-se na fase de 500 jobs/s. Ele recupera a agenda
durante a última fase de 150 jobs/s, portanto a média global de 230 jobs/s não
deve ser interpretada como capacidade de pico.

## Elasticidade e recursos

| Fase | Oferta | Pods disponíveis | CPU média | CPU máxima | RAM média | RAM máxima |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 50/s | 2 → 6 | 1,61 cores | 3,82 cores | 198,91 MiB | 396,70 MiB |
| 2 | 150/s | 6 → 8 | 4,43 cores | 5,34 cores | 445,37 MiB | 554,26 MiB |
| 3 | 300/s | 8 | 5,08 cores | 5,21 cores | 565,68 MiB | 572,21 MiB |
| 4 | 500/s | 8 | 5,16 cores | 5,31 cores | 575,99 MiB | 581,27 MiB |
| 5 | 150/s + recuperação | 8 | 4,96 cores | 5,36 cores | 584,47 MiB | 585,99 MiB |

O HPA disponibilizou ao menos 6 pods em 37,64 s e 8 pods em 97,64 s. A maior
amostra individual foi 0,83 core e 74,62 MiB. O HPA permaneceu em 8 durante a
recuperação por causa da janela de estabilização de scale-down de 300 segundos.

Na fase estável de 300 jobs/s, o custo aproximado foi 16,9 ms de CPU de
aplicação por job (`5,075 cores / 300 jobs/s`). Este valor inclui HTTP, execução
do cálculo, coordenação da fila e acesso ao banco, mas não o gerador nem o
PostgreSQL, que estão em nós/processos separados.

## PostgreSQL

As 60 amostras internas foram coletadas sem erro. Entre a primeira e a última
amostra houve:

- 68.707 conclusões observadas;
- 859.743 commits e zero rollbacks;
- 96.572.939 bytes de WAL;
- 82.397.064 buffer hits, 1.119 blocos lidos e zero crescimento de temporários;
- máximo de 76 conexões, 66 ativas, 13 jobs executando e 74 disponíveis.

Isso equivale aproximadamente a 12,51 commits e 1.406 bytes de WAL por job
concluído observado. Os contadores são do banco inteiro, e as fronteiras de
amostragem têm resolução de 5 segundos, portanto são estimativas conservadoras.

| Fase | Commits/job observados | WAL/job observado |
|---|---:|---:|
| 50/s | 35,72 | 1,35 KiB |
| 150/s | 20,64 | 1,33 KiB |
| 300/s | 11,55 | 1,35 KiB |
| 500/s | 7,43 | 1,38 KiB |
| Recuperação | 13,71 | 1,42 KiB |

O WAL por job é estável e baixo. Já os commits por job caem conforme a carga
aumenta, o que indica custo fixo relevante de polling/claims vazios e consultas
de status. A conclusão em lote não elimina enqueue, claim, efeito transacional,
polls HTTP e manutenção da fila; além disso, o worker transacional conclui junto
do efeito de negócio e não usa o buffer assíncrono de conclusão.

## Comparação com o benchmark anterior

Com 8 pods fixos e execuções curtas de 20 segundos, as três repetições antigas
em 300 jobs/s entregaram média de 298,33 jobs/s, sem erros, enqueue p95 médio de
7,84 ms e backlog máximo médio de 44,67. A fase atual de 300 jobs/s também teve
zero erros e backlog de aproximadamente 16 no seu final, confirmando esse nível.

Em 500 jobs/s, os testes curtos anteriores variaram entre 470,92 e 497,07
jobs/s, com enqueue p95 entre 25,55 e 317,37 ms e backlog máximo médio de 192.
O minuto sustentado atual expôs 503, backlog máximo de 384 e atraso de agenda
p95 de 7,10 s. Portanto, os ensaios curtos eram otimistas e já mostravam alta
variabilidade na borda de capacidade.

Uma primeira execução desta mesma matriz concluiu 69.000/69.000 sem erros, mas
teve enqueue p95 de 343,80 ms e atraso de agenda p95 de 11,30 s. A diferença
entre as duas repetições reforça que 500 jobs/s é uma região instável, não uma
meta segura.

## Próximas decisões

1. Tratar 300 jobs/s como envelope validado desta configuração.
2. Instrumentar contadores agregados de claims vazios, wakes e causa exata dos
   HTTP 503 para separar saturação do pool HTTP de erro do store.
3. Reduzir o efeito de thundering herd do `NOTIFY`: coalescer wakes por fila e
   permitir somente um claim em voo por scheduler/pod.
4. Testar pools PostgreSQL de forma coordenada. O máximo observado de 76
   conexões é compatível com 8 pools de 8 mais listeners e observadores; aumentar
   workers sem revisar esse orçamento tende a apenas deslocar o gargalo.
5. Repetir 5 minutos em 350, 400 e 450 jobs/s, três vezes por taxa, para localizar
   o joelho de capacidade com intervalo de confiança.

Não é metodologicamente correto comparar estes números diretamente com Sidekiq,
Oban, Celery ou filas JVM sem executar o mesmo job, banco, hardware, política de
durabilidade e gerador. O que este ensaio comprova é a capacidade do desenho
Quasar/Gleam neste ambiente específico.
