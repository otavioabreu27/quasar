# Validação dos índices parciais do Quasar

Data: 2026-09-05

Banco: PostgreSQL 16 remoto, acessado pelo túnel local `127.0.0.1:5433`

Base: `reports`, 953.670 jobs, todos com estado `completed`

## Alteração

Foram adicionadas migrations online e incrementais:

- `004`: cria `quasar_jobs_ready` sobre `(queue, priority DESC, id)` apenas para
  `available`, `scheduled` e `retryable`;
- `005`: cria `quasar_jobs_active_leases` sobre `(lease_expires_at)` apenas para
  `executing`;
- `006`: remove o índice amplo antigo `quasar_jobs_fetch`, depois da validação;
- `007`: remove o índice amplo antigo `quasar_jobs_leases`, depois da validação.

As operações de criação e remoção usam `CONCURRENTLY`. Para suportá-las, o
executor reconhece migrations `NNN_online_*.sql`, executa-as fora de transação
em uma única conexão e coordena instâncias concorrentes com o mesmo advisory
lock das migrations transacionais. A ordem global de versões é preservada.

## Ocupação

Antes:

| Índice | Tamanho |
| --- | ---: |
| `quasar_jobs_fetch` | 117 MB |
| `quasar_jobs_leases` | 13 MB |
| `quasar_jobs_pkey` | 21 MB |

Depois:

| Índice | Tamanho | Estado |
| --- | ---: | --- |
| `quasar_jobs_ready` | 8 KB | válido e pronto |
| `quasar_jobs_active_leases` | 8 KB | válido e pronto |
| `quasar_jobs_pkey` | 21 MB | válido e pronto |

Os 953.670 jobs concluídos não pertencem aos predicados dos dois índices
parciais e, portanto, não ocupam os índices quentes dos workers.

## Planos depois da remoção dos índices antigos

Claim de até 16 jobs da fila `reports`:

```text
Index Scan using quasar_jobs_ready on quasar_jobs
  Index Cond: (queue = 'reports'::text)
  Buffers: shared hit=2
Planning Time: 1.031 ms
Execution Time: 0.146 ms
```

Reaper de leases vencidos:

```text
Index Scan using quasar_jobs_active_leases on quasar_jobs
  Buffers: shared hit=1
Planning Time: 0.085 ms
Execution Time: 0.019 ms
```

Os planos foram obtidos com `EXPLAIN (ANALYZE, BUFFERS)`. Não havia jobs
claimáveis nem leases ativos durante a medição, então nenhuma linha foi
alterada. O resultado comprova a escolha dos índices e o custo de localizar um
conjunto vazio; não representa, isoladamente, a latência sob carga.

## Verificações

- histórico remoto contém exatamente as migrations 1–7;
- somente a chave primária e os dois índices parciais permanecem na tabela;
- ambos os índices parciais estão `indisvalid = true` e `indisready = true`;
- adaptador PostgreSQL em banco descartável: 3 testes, zero falhas;
- núcleo Quasar: 29 testes, zero falhas;
- report service: 4 testes Python e 3 testes Gleam, zero falhas;
- imagem Docker do report service construída com sucesso contendo `priv/004` a
  `priv/007`.

## Conclusão

O critério desta etapa foi atendido. O histórico concluído deixou de inflar os
índices consultados em cada polling, claim e reaper; os planos usam somente os
índices parciais, e os índices antigos foram removidos apenas após essa
confirmação.
