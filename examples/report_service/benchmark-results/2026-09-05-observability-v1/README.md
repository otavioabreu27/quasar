# Validação da observabilidade v1

Validação funcional curta, executada em PostgreSQL 16 descartável, quatro
instâncias e 100 jobs por cenário. Não é um novo resultado de capacidade.

| Cenário | Claims | Vazios | Retornados | Claim médio/p95 | Completion médio/p95 | Renewals | Executando pico | Pool checkout médio/p95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 × 2 workers, sem tracing | 163 | 68 | 100 | 5,804/10 ms | 4,40/7 ms | 100 | 8 | não coletado |
| 4 × 2 workers, com tracing | 161 | 64 | 100 | 5,267/10 ms | 3,89/6 ms | 100 | 8 | 0,054/0,2 ms |
| 4 × 8 workers, com tracing | 698 | 637 | 100 | 2,993/9 ms | 6,28/10 ms | 100 | 31 | 0,079/0,2 ms |

Todos os cenários terminaram com 100 jobs iniciados e concluídos, nenhuma falha
de persistência ou claim e zero jobs executando no fim. A instrumentação torna
visível que mais workers elevaram fortemente os claims vazios neste ensaio
curto; essa observação deve ser confirmada em carga sustentada antes de orientar
uma mudança de runtime.
