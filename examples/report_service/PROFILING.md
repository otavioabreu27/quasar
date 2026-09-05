# Perfil de latência — 2026-09-05

## Resultado

Há evidência de custo importante na escrita/sincronização do WAL e no caminho
completo de acesso ao PostgreSQL. Não apareceu espera relevante para obter
conexões do pool. Isso é mais específico do que dizer que “a query é lenta” ou
que “o scheduler é lento”; ambos os rótulos isolados escondem etapas distintas.

O arquivo [profiling-results.json](profiling-results.json) contém as seis rodadas
finais. Todas concluíram 1000 jobs com resultados corretos, distribuídos entre
quatro processos BEAM, com no máximo uma tentativa por job. Houve também uma
passagem exploratória anterior; seus números não são usados nesta comparação.

| Configuração | Lote pelos timestamps, s (duas rodadas) | Enqueue p95, ms | Fila p95, ms |
|---|---|---|---|
| 4 instâncias × 2 workers, sem tracing | 2,570 / 3,982 | 58,255 / 52,080 | 1156 / 2551 |
| 4 × 2, com tracing | 2,149 / 3,330 | 20,737 / 52,300 | 1318 / 2115 |
| 4 × 8, com tracing | 1,788 / 3,205 | 23,042 / 56,352 | 908 / 1718 |

Em todas as configurações, 12 conexões por instância, 16 clientes HTTP,
prefetch 1, a mesma regra de negócio e os mesmos pacotes publicados no Hex.
Não se deve interpretar essa tabela como prova de ganho percentual estável:
há apenas duas repetições e variação expressiva, inclusive no controle sem
tracing. Mais workers não multiplicaram a capacidade por quatro.

## O que foi medido

- **Pool checkout:** chamada real de `pgo_pool:checkout/2`, incluindo obtenção
  de uma conexão e overhead do pool. Nas rodadas instrumentadas, média de
  0,049–0,066 ms e limite superior do p95 de 0,1–0,2 ms.
- **Driver:** `pgo_handler:extended_query/5`, sem checkout, mas incluindo
  protocolo, ida/volta ao banco, execução e esperas até receber o resultado.
  P95 de 5,4–30,4 ms nas rodadas instrumentadas.
- **Claim:** operação inteira `quasar_jobs/store.claim`, que inclui múltiplas
  instruções dentro de uma transação. P95 de 9,8–34,8 ms.
- **Worker:** `job_executor.execute`, incluindo a renovação inicial da lease,
  a regra de negócio e a confirmação persistida. O contador confirmou exatamente
  1000 execuções por rodada. A soma dos picos locais foi 8 com 2 workers e 30
  com 8 workers por instância; essa soma **não** prova simultaneidade global.
- **SQL no servidor:** `pg_stat_statements`. A média do INSERT de enqueue foi
  0,125–0,192 ms; a consulta de claim, 0,500–0,616 ms. São médias por instrução,
  não p95, nem o tempo integral de um pedido/commit visto pelo cliente.
- **Espera no PostgreSQL:** snapshots de `pg_stat_activity` durante a carga.
  Das 1361 observações não classificadas como `idle`, 1007 eram
  `LWLock:WALWrite` e 115 eram `IO:WALSync` — cerca de 82,4% somadas.
  São observações de backends amostrados, **não** uma decomposição exata do
  tempo dos requests; houve 4591 observações `idle`, excluídas desse percentual.

`WALSync` indica espera para o WAL chegar ao armazenamento durável;
`LWLock:WALWrite` indica espera pela escrita dos buffers de WAL.
[Definições oficiais do PostgreSQL 16](https://www.postgresql.org/docs/16/monitoring-stats.html).

Um job deste exemplo provoca vários acessos: INSERT, transação de claim
(BEGIN, recuperação de leases, claim, COMMIT), renovação inicial da lease,
persistência do relatório e confirmação. A instrumentação observou cerca de
8,1–8,8 mil chamadas de driver para 1000 jobs, incluindo claims vazios no intervalo.
Logo, mesmo instruções individualmente rápidas acumulam viagens e commits.
Os claims são processados por um scheduler por fila em cada instância, o que
também limita quanto mais workers conseguem aproveitar.

## Método e limitações

- PostgreSQL 16-alpine descartável, com `pg_stat_statements`,
  `track_io_timing=on`, configurações normais de durabilidade e porta aleatória
  restrita ao localhost. Não desligamos `fsync` nem `synchronous_commit`.
- Quatro instâncias de teste nas portas 19181–19184. As quatro instâncias do
  usuário e seu banco na porta 15432 não foram reiniciados nem modificados.
- Antes de cada rodada: limpeza **somente das tabelas do banco descartável**,
  20 jobs de aquecimento, VACUUM ANALYZE e reset das estatísticas SQL temporárias.
- Distribuição round-robin dos POSTs; verificação de conclusão por uma consulta
  SQL agregada, em vez de milhares de GETs HTTP sequenciais.
- O intervalo do lote usa os timestamps gravados pelo Quasar, em milissegundos.
  Eles são capturados antes de certas operações de banco terminarem; não são
  timestamps exatos de commit. O tempo observado por polling é reportado à parte.
- Histogramas de tracing têm buckets de 100 μs, teto de 60 s e são obtidos pela
  diferença entre snapshots. A janela pode incluir alguns claims vazios após
  a conclusão. O tracing acrescenta overhead; o controle serve para expor isso.
- A coleta de estatísticas também tem custo. Todos os testes compartilham a
  máquina com outros processos. O PostgreSQL temporário pode ter comportamento
  de armazenamento diferente de um deploy real; não isolamos hardware, Docker,
  escalonamento do SO, transporte ou custo exato de cada flush de WAL.

## Próximos experimentos, não implementados

1. Repetir mais vezes, alternando a ordem, com carga sustentada e armazenamento
   representativo do ambiente de produção.
2. Medir agrupamento de claims e redução de viagens/transações, preservando
   fencing, leases e durabilidade; não remover garantias para melhorar números.
3. Medir o intervalo até despertar instâncias ociosas separadamente. O fallback
   de polling de 1 s existe, mas esta carga distribui enqueues entre todos os nós;
   os dados não provam que ele seja a causa principal aqui.

Não alteramos o runtime Quasar nem o adaptador PostgreSQL com base neste perfil.

## Reproduzir

Requer Python 3, Docker, Gleam, mise/Rebar3 e OTP 27+ (módulo `json` do Erlang).
Não execute em ambiente de produção. O script usa nome único para o container,
verifica portas e encerra seus próprios processos e banco em `finally`.
Interromper à força o próprio harness pode impedir a limpeza automática.

```bash
cd /home/oabreu/Repos/quasar/examples/report_service
mise exec rebar@3.27.0 -- gleam build
python3 profile_test.py --jobs 1000 --clients 16 --repeats 2
```

Os resultados são impressos como JSON (`RESULT` e `PROFILE_RESULTS`).
O fixture `test/profile_service.gleam` habilita tracing somente quando solicitado
pelo harness; a aplicação normal não recebe instrumentação nem endpoints extras.
O coletor não escreve argumentos SQL, resultados SQL ou credenciais nos logs.

```bash
python3 -m unittest discover -s test -p '*_test.py'
mise exec rebar@3.27.0 -- gleam test
```

Validação: três testes do agregador Python e três testes Gleam passaram.
