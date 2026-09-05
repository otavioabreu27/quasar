# Validação funcional do candidato

Arquivos `*-smoke.jsonl`: testes de poucos segundos, locais, com PostgreSQL 17
descartável e conexão direta. Não são o benchmark k3s de cinco minutos.
`buffered`/`transactional` identificam o modo da aplicação, e `batch4` usa quatro
jobs por requisição. Os cenários são executados separadamente.

`*-observer.jsonl`: verificação do coletor com `pg_stat_statements` habilitado
somente no banco descartável. CPU/cgroup local pode incluir outros processos;
não atribuir estes números ao microsserviço nem comparar com mercado.

As saídas preservam contagens, IDs, percentis e integridade do resultado.
O código/receita do teste sustentado está em `../BENCHMARK_CANDIDATO.md`.
