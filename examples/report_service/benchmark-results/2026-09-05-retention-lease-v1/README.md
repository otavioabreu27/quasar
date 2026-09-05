# Retenção e lease/prefetch — validação no k3s

Data: 2026-09-05

Ambiente: `report_service` no k3s, PostgreSQL 16 em VM separada, HPA de 2 a 8
réplicas e gerador de carga executado dentro do cluster.

## Implementação

- política agnóstica de banco em `quasar_jobs/retention`;
- retenção padrão do serviço: `completed` por 7 dias e
  `cancelled`/`discarded` por 30 dias;
- lotes de 1.000 linhas, pausa de 100 ms e ciclo de 60 s;
- `finished_at` registra o instante terminal exato para novos jobs;
- registros históricos usam `completed_at`, `attempted_at` ou `inserted_at` como
  fallback;
- três índices parciais frios, um por estado terminal;
- seleção com `FOR UPDATE SKIP LOCKED` e advisory lock não bloqueante;
- `example_reports.job_id` usa `ON DELETE CASCADE`;
- callbacks informam quantidade, duração, erro e conclusão do ciclo;
- claims frescos não renovam lease imediatamente;
- jobs atrasados no prefetch ainda precisam renovar com fencing antes do
  handler;
- o primeiro heartbeat é calculado pelo vencimento real e os seguintes ocorrem
  quando a lease entra no último terço;
- heartbeat que perde ownership encerra, evitando novas escritas inúteis.

## Plano da retenção

Com 10 mil jobs vencidos num PostgreSQL descartável, depois de `ANALYZE`:

```text
Index Scan using quasar_jobs_retention_completed
  Index Cond: COALESCE(finished_at, completed_at, inserted_at) < cutoff
  rows=1000
  Buffers: shared hit=23
Execution Time: 55.152 ms
```

O `EXPLAIN (ANALYZE, BUFFERS)` foi executado dentro de uma transação revertida.
O limite de 1.000 foi respeitado e nenhuma linha do ensaio foi perdida.

## Teste funcional

O serviço drenou 10 mil terminais vencidos em dez transações separadas. Ao
final permaneceram somente 51 jobs concluídos recentes, um cancelado recente,
um descartado recente e um job disponível. A FK do resultado foi confirmada
como cascata e validada.

No cluster, um ensaio adicional inseriu 100 mil jobs `completed` com 40 dias de
idade. A retenção removeu todos; `synthetic_expired_remaining = 0`. O intervalo
foi temporariamente reduzido para 5 s somente durante esse ensaio e restaurado
para 60 s ao final.

## Carga HTTP comparável

Cada rodada submeteu 4.500 jobs em 30 segundos, a 150 jobs/s, sem retry no
cliente. Todas terminaram com 4.500 completos, tentativa 1, zero erro e zero
timeout.

| Cenário | Aceitos/s | Enqueue p95 | Servidor E2E p95 | Poll p95 |
| --- | ---: | ---: | ---: | ---: |
| Antes | 150,017 | 3,708 ms | 10 ms | 2,863 ms |
| Depois, retenção estável | 150,015 | 3,758 ms | 7 ms | 3,048 ms |
| Durante remoção de 100 mil | 150,014 | 3,650 ms | 9 ms | 3,085 ms |

Entre o antes e o regime estável, o p95 de enqueue variou +1,3%, throughput
ficou efetivamente igual e o p95 servidor caiu 30%. Durante a remoção ativa, o
p95 de enqueue ficou abaixo da linha de base e não houve erro. O HPA chegou a
seis pods nessa rodada severa, portanto ela comprova ausência de degradação
operacional perceptível, mas não deve ser usada isoladamente para atribuir ganho
de latência ao cleaner.

## Estado final

- migrations PostgreSQL: 1–11;
- deployment: saudável, sem reinícios durante os testes;
- intervalo de retenção restaurado para 60 s;
- artefatos temporários do benchmark removidos;
- jobs sintéticos vencidos restantes: zero;
- `pg_stat_user_tables` logo após o teste: 967.170 tuplas vivas e 109.000
  mortas; autovacuum já havia executado 38 vezes.

As tuplas mortas são esperadas após `DELETE`: autovacuum recupera o espaço para
reuso, sem reduzir imediatamente o arquivo físico. A estabilização de longo
prazo no patamar equivalente à janela de sete dias só pode ser confirmada por
observação longitudinal; o mecanismo de limite foi validado funcionalmente e
sob carga, mas este ensaio não substitui sete dias de produção contínua.

## Arquivos

- `baseline.jsonl`: saída bruta antes;
- `steady-after.jsonl`: saída bruta após retenção e lease otimizada;
- `active-retention.jsonl`: saída bruta durante a remoção de 100 mil jobs;
- `database-after.txt`: contagens e autovacuum ao final;
- `cluster-after.yaml`: configuração/estado do cluster ao final.
