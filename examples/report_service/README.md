# Microsserviço de relatórios: Quasar + Mist + PostgreSQL

Exemplo executável com dependências publicadas no Hex, sem referências locais
aos repositórios. Requer Gleam >= 1.18, Erlang/OTP, Rebar3 e Docker Compose.
O teste de carga usa Python 3, sem bibliotecas extras.

## Rodar uma instância

Na pasta deste exemplo:

```bash
cd /home/oabreu/Repos/quasar/examples/report_service
docker compose up -d --wait
export DATABASE_URL='postgresql://reports:reports_local_only@127.0.0.1:15432/reports'
INSTANCE_ID=reports-a PORT=8080 mise exec rebar@3.27.0 -- gleam run
```

O Compose inicia apenas PostgreSQL, com um volume persistente. O processo Gleam
roda na máquina. A aplicação cria as tabelas na inicialização; aguarde a primeira
instância ficar pronta antes de iniciar a segunda, evitando corrida de DDL.
Se Rebar3 já estiver configurado, `gleam run` é suficiente.

Em outro terminal:

```bash
curl -i http://127.0.0.1:8080/health/ready
curl -i -X POST http://127.0.0.1:8080/reports/100
```

O POST não recebe corpo: `100` é o tamanho do relatório (1 até 1000000).
Retorna `202`, `Location: /jobs/<id>` e `{"job_id":"<id>"}`. Consulte o ID real:

```bash
curl http://127.0.0.1:8080/jobs/1
```

Quando terminar:

```json
{"job_id":"1","status":"completed","attempt":1,"total":5050,"processed_by":"reports-a"}
```

O relatório calcula a soma de 1 até N. `total` e `processed_by` são `null`
enquanto não existir resultado persistido. Resultado e confirmação do job são
transações separadas: o resultado pode aparecer pouco antes de `completed`.

## Duas ou mais instâncias e scheduling distribuído

Mantenha a primeira instância ligada. No segundo terminal:

```bash
cd /home/oabreu/Repos/quasar/examples/report_service
export DATABASE_URL='postgresql://reports:reports_local_only@127.0.0.1:15432/reports'
INSTANCE_ID=reports-b PORT=8081 mise exec rebar@3.27.0 -- gleam run
```

Para uma terceira, use `INSTANCE_ID=reports-c PORT=8082` com a mesma URL de banco.
Cada instância tem 2 workers duráveis, prefetch 1 e um pool HTTP separado com
8 workers e buffer 32. Com N instâncias são até 2 × N execuções duráveis ativas.

- Use **o mesmo PostgreSQL**, fila `reports` e worker `reports.sum.v1`.
- As instâncias não precisam de nomes de nós Erlang nem de conexão BEAM entre si.
- O PostgreSQL coordena os claims com `FOR UPDATE SKIP LOCKED` e leases.
- Constellation controla capacidade **local**; não faz o scheduling entre nós.
- Não há promessa de divisão 50/50 ou de throughput linear com mais instâncias.
- Qualquer instância pode consultar um job criado ou executado pela outra.

No terceiro terminal, envie todos os pedidos somente à instância A:

```bash
cd /home/oabreu/Repos/quasar/examples/report_service
python3 load_test.py --url http://127.0.0.1:8080 --jobs 1000 --clients 16
```

O teste confirma os resultados, agrupa `processed_by` e mostra tentativas,
latência p95 de enqueue e throughput observado. Ver trabalho executado por B,
mesmo sem receber POSTs, demonstra o consumo distribuído da fila.
O teste sai com erro em HTTP inesperado, job descartado, resultado errado ou
timeout. Se observar apenas uma instância, emite um aviso; isso sozinho não
prova um defeito no scheduler.

Para distribuir também os pedidos HTTP, repita `--url`. Os POSTs e as consultas
de status usam round-robin no cliente; não é necessário instalar um proxy.
O relatório separa `posts_by_api` / `status_queries_by_api` de `processed_by`:
a instância que recebe o POST não precisa ser aquela que executa o job.

Por exemplo, com quatro instâncias nas portas abaixo:

```bash
python3 load_test.py --jobs 1000 --clients 16 \
  --url http://127.0.0.1:8080 \
  --url http://127.0.0.1:8081 \
  --url http://127.0.0.1:18083 \
  --url http://127.0.0.1:18084
```

Inicie a quarta com o mesmo `DATABASE_URL` e
`INSTANCE_ID=reports-d PORT=18084 mise exec rebar@3.27.0 -- gleam run`.
Usar só um `--url` preserva o teste de scheduling sem distribuição HTTP.

Para comparar capacidade, rode o mesmo comando com uma instância e depois com
duas, mantendo hardware, PostgreSQL, número de jobs e clientes iguais. Aumente
gradualmente `--clients`; `503` indica saturação/indisponibilidade e o script
interrompe em vez de esconder falhas com retries. **Este é um teste de carga
demonstrativo**, não um benchmark de capacidade máxima: o trabalho é barato e
a medição inclui consultas HTTP de acompanhamento. Use uma carga representativa
do seu domínio para dimensionar produção.

Também é possível inspecionar o banco (acumulado de todas as execuções):

```bash
docker compose exec postgres psql -U reports -d reports -c \
  'SELECT processed_by, count(*) FROM example_reports GROUP BY processed_by;'
docker compose exec postgres psql -U reports -d reports -c \
  'SELECT status, attempt, count(*) FROM quasar_jobs GROUP BY status, attempt;'
```

Para produtores em lote, `POST /reports/batch/:size/:count` insere de 1 a 1.000
jobs atomicamente por chamada usando `quasar.enqueue_many`. Requisições isoladas
continuam usando `POST /reports/:size` sem aguardar formação de lote.

## Retenção

O serviço habilita retenção por estado terminal: `completed` por 7 dias e
`cancelled`/`discarded` por 30 dias. A limpeza usa lotes de 1.000 linhas, pausa
100 ms entre lotes e inicia um ciclo a cada minuto. Todos esses valores podem
ser alterados com `RETENTION_COMPLETED_DAYS`, `RETENTION_CANCELLED_DAYS`,
`RETENTION_DISCARDED_DAYS`, `RETENTION_BATCH_SIZE`, `RETENTION_PAUSE_MS` e
`RETENTION_INTERVAL_MS`.

## Wake distribuído

Cada instância mantém uma conexão PostgreSQL dedicada em `LISTEN quasar_jobs`.
O enqueue publica a fila com `pg_notify` na mesma transação do `INSERT`, e cada
pod transforma a notificação em um wake local. A notificação é apenas uma
otimização: `QUEUE_POLL_INTERVAL_MS` controla o polling de recuperação e usa
5 segundos por padrão.

`example_reports.job_id` usa `ON DELETE CASCADE`: remover o job também remove o
resultado demonstrativo. Em uma aplicação real, escolha explicitamente entre
cascata, limpeza prévia ou arquivamento da informação de negócio.

## Durabilidade e falhas

Os jobs são **at-least-once**: após uma falha, uma tentativa pode ser repetida
quando a lease expirar. Este exemplo usa `quasar_postgres.transactional_worker`:
a gravação do relatório e a conclusão cercada pelo token do job acontecem na
mesma transação PostgreSQL. O `ON CONFLICT DO NOTHING` permanece como defesa
adicional, mas não é necessário para cobrir uma queda entre efeito e ACK.
Isso não dá exatamente-uma-vez a e-mails, pagamentos ou chamadas externas.
Cada novo POST cria outro job; não há chave de idempotência HTTP neste exemplo.
Um timeout HTTP também não prova que o enqueue deixou de acontecer.

Para observar recuperação, mantenha carga suficiente para haver backlog,
interrompa uma instância e deixe a outra ligada. Jobs disponíveis podem ser
consumidos pela sobrevivente; jobs em execução dependem da expiração da lease.
A conta em `processed_by` registra quem gravou o resultado, não um histórico
de todas as tentativas. Os testes deste exemplo não simulam esse crash.

## GHCR e deploy no k3s

O parecer consolidado da execução no k3s está em
[`PARECER_TECNICO_K3S.md`](PARECER_TECNICO_K3S.md), incluindo resultados,
especificações, elasticidade, estimativas amortizadas por request e comparação
com outros ecossistemas.

O workflow [`report-service-image.yml`](../../.github/workflows/report-service-image.yml)
roda os testes e publica a imagem no GHCR em:

```text
ghcr.io/<OWNER>/report_service:latest
ghcr.io/<OWNER>/report_service:sha-<COMMIT>
```

Ele é acionado em pushes para `main` que alterem este exemplo e também pode ser
executado manualmente. A imagem será publicada como
`ghcr.io/otavioabreu27/report_service`. Depois da primeira publicação, torne o pacote público em
**GitHub → Profile → Packages → report_service → Package settings → Change
visibility → Public**.

Os manifests em [`deploy/k8s`](deploy/k8s) assumem que o PostgreSQL já existe
no cluster e que o Metrics Server do k3s está disponível. Crie o Secret sem
commitar a senha e aplique:

```bash
cd /home/oabreu/Repos/quasar/examples/report_service
kubectl apply -k deploy/k8s
kubectl -n reports create secret generic report-service \
  --from-literal=DATABASE_URL='postgresql://USER:PASSWORD@postgresql.database.svc.cluster.local:5432/reports' \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n reports rollout status deployment/report-service
```

O HPA usa CPU e memória, escala de 2 até 20 pods, sobe rapidamente e espera
cinco minutos antes de reduzir capacidade. Ajuste `maxReplicas`, requests/limits
e o pool do PostgreSQL conforme a capacidade real dos nós e do banco; cada pod
abre até 10 conexões de pool e executa até 8 workers. Para visualizar o serviço
internamente, use `kubectl -n reports port-forward svc/report-service 8080:8080`.

Para volumetria representativa, gere carga em etapas e observe erros, latência,
backlog em `quasar_jobs`, CPU/memória dos pods e conexões do PostgreSQL. O HPA
não mede o backlog da fila: se a carga dominante for POSTs rápidos que geram
jobs, a próxima evolução deve adicionar métrica externa de jobs disponíveis.
Esta aplicação continua sem autenticação e TLS; não a exponha diretamente à
Internet sem um Ingress/API gateway com acesso e limites.

## Organização e limites

- `config.gleam`: variáveis de ambiente; `HOST` padrão `127.0.0.1`.
- `api.gleam`: rotas, validação e respostas HTTP; sem acesso a sockets no worker.
- `reports.gleam`: regra de negócio e definição tipada do worker.
- `database.gleam`: SQL parametrizado e persistência idempotente.
- `report_service.gleam`: composição e inicialização dos processos ligados.
- `/health/live`: liveness sem banco; `/health/ready`: consulta PostgreSQL pelo pool HTTP.

Esta é uma aplicação demonstrativa, sem autenticação, TLS ou quota global de
jobs. Não exponha as portas publicamente. Para produção: adicione
autorização, quotas, observabilidade, migrations versionadas
executadas uma única vez no deploy, supervisão/gerenciador de processos e
shutdown coordenado (parar HTTP, drenar Quasar, encerrar o pool PostgreSQL).
A inicialização aqui é fail-fast; não implementa recuperação completa de toda
a aplicação nem drenagem via SIGTERM. A senha do Compose é somente local.

## Testes e encerramento

Para separar tempos de SQL, obtenção de conexões, claims e execução dos workers,
veja o [perfil de latência e seu harness isolado](PROFILING.md).

```bash
mise exec rebar@3.27.0 -- gleam test
gleam format --check
```

Esses testes unitários não precisam do banco; o `load_test.py` testa HTTP e
PostgreSQL com as instâncias em execução. As dependências Mist/Gramps podem
emitir os avisos conhecidos sobre o alias `Header` do `gleam_http`.

Encerre os processos Gleam nos respectivos terminais e execute:

```bash
docker compose down
```

O volume e os dados são preservados. Não use `down -v` se quiser manter os jobs.
