# Candidato: correções e próximo benchmark

Preparação de 05/09/2026. Código ainda não publicado no Hex nem aplicado no k3s.
Os testes locais descritos abaixo são funcionais; não são evidência de capacidade
de produção nem substituem o benchmark de cinco minutos.

## O que mudou

1. **Scheduler:** soma a capacidade dos grants pendentes e faz um único claim.
   Distribui o retorno entre os grants, preservando seus offsets. Um drain de
   1ms agrupa wakes locais/NOTIFY e eventos de demanda. Consulta vazia não
   continua fazendo uma consulta por grant. Wakes recebidos/agrupados são métricas.
   Timeout de distribuição é entrega incerta: não libera o token automaticamente;
   a lease permite recuperação sem autorizar duas execuções simultâneas.
2. **PostgreSQL:** claim é uma instrução SQL atômica, sem BEGIN/reaper/COMMIT
   explícitos. Reaper é um processo separado: intervalo padrão 1s, até 500 jobs,
   pausa de 100ms após lote cheio, advisory lock não bloqueante e SKIP LOCKED.
   **É obrigatório iniciar `quasar_postgres/reaper` nas aplicações existentes.**
3. **Lease:** não inicia handler com claim já expirado, nem ressuscita lease
   expirada no PostgreSQL. Renew/complete/fail verificam a validade pelo relógio
   do banco, além de owner/generation. Handler roda em processo cancelável;
   um guardião impõe o prazo mesmo se a renovação ficar bloqueada em I/O.
   O heartbeat continua enquanto o worker espera a tentativa de persistência.
4. **Conclusão em lote:** mantém o agrupamento de 100 resultados/5ms; tenta
   novamente falhas transitórias até três vezes, com esperas de 20/40/60ms.
   Um token obsoleto provoca isolamento por item, não abandono dos jobs válidos.
   Esgotamento é reportado como falha; nunca emite JobCompleted sem sucesso.
   A lease continua sendo a recuperação durável, não a memória do buffer.
5. **Listener:** mantém reconexão de socket e passa a reconstruir também o
   processo de notificações quando ele cai. Polling de 5s permanece ativo.
6. **Serviço:** pools separados: `DB_POOL_SIZE=3` para API/enqueue/status,
   `DB_WORKER_POOL_SIZE=4` para efeitos/conclusões e `DB_CONTROL_POOL_SIZE=1`
   para claim/heartbeat/manutenção/readiness. Mesmo total de oito conexões do
   deployment anterior, mais uma conexão exclusiva LISTEN. Status usa um JOIN
   em vez de duas queries. Readiness não entra na fila de execução HTTP.
7. **Cenários explícitos:** `COMPLETION_MODE=transactional` é o padrão; efeito
   de negócio e conclusão usam a mesma transação. `buffered` usa INSERT
   idempotente e conclusão pelo buffer, em transações distintas. Enqueue em lote
   usa `/reports/batch/:size/:count`; não atribuir seus ganhos ao endpoint individual.

Não alterei a semântica de grants do Constellation nem adicionei broker/KEDA.
Não aumentei o orçamento total de conexões para disfarçar contenção.

## Limites de segurança e de interpretação

- At-least-once permanece. Efeitos externos já realizados não são desfeitos pela
  interrupção do handler; precisam de idempotência. A execução transacional só
  protege efeitos no mesmo PostgreSQL.
- Cancelamento administrativo não é instantâneo para código já em execução:
  a perda de posse é detectada na renovação; expiração tem guardião independente.
- Callbacks de store precisam ter I/O limitado. Timeouts podem ter commit incerto.
  O buffer não transforma uma falha prolongada de banco em sucesso; a recuperação
  pode repetir o handler. Não há promessa de throughput antes da medição.
- Memory/SQLite mantêm relógios sintéticos/recuperação próprios para testes e uso
  local. O contrato de manutenção e o relógio de fencing descritos aqui são do PG.
- Retenção/reaper compartilham o pool de controle: a duração desses lotes será
  observada. Se houver contenção, redistribuir o orçamento medindo o efeito.
- Com 8 pods: 72 conexões da aplicação/listeners. Um nono pod de rollout leva
  a 81, **sem contar pods ainda terminando**, observer, administração e outros
  clientes. Conferir o orçamento da VM antes de subir concurrency/pools/HPA.
- `size=1000000` usa a fórmula `n*(n+1)/2`: trabalho O(1), não um milhão de
  iterações. Este benchmark mede fila durável + persistência + polling HTTP;
  não mede supremacia de CPU da linguagem.

## Métricas e arquivos

`/internal/metrics` publica contadores locais de claims, retornos, claims vazios,
wakes, leases, conclusões, falhas, reaper, espera HTTP, run queue da BEAM e
CPU/throttling de cgroup v2 quando disponível. Sem IDs/payloads/credenciais.
É um endpoint de diagnóstico **interno**: não publicar essa rota em ingress público.

`BENCHMARK_POOL_TRACE=1` habilita contagem/latência de checkout do PGO, incluindo
espera e falhas. É um cenário diagnóstico separado, pois tracing tem custo e
usa o tracer da VM. Deixe desligado para os resultados de capacidade.
`pg_stat_activity.application_name` pode não distinguir os pools com este driver;
não inferir distribuição por pool a partir desse campo.

O observer mede somente os índices ativos para backlog; terminações e tamanho
total são estimativas de catálogo, sem COUNT periódico da tabela inteira.
Coleta waits, WAL, estatísticas/reset e, se previamente disponível,
`pg_stat_statements` por operação/queryid, **sem SQL ou parâmetros nos arquivos**.
Não instala extensão nem reseta estatísticas no banco compartilhado.

Arquivos por execução: `load.jsonl`, `postgresql-observer.jsonl`,
`kubernetes-observer.jsonl`, snapshots de deployment/HPA/pods, identidade da
imagem, hashes dos scripts e `checksums.sha256`. A coleta de runtime é por pod,
não via service balanceado. O tráfego de carga e PostgreSQL fica dentro do cluster;
o API server só transporta a telemetria pequena, nunca a carga de teste.

O gerador schema 3 registra carga programada, tentativas efetivas e descartes
por saturação/atraso do cliente. Não acumula backlog de envio nem reproduz
requisições atrasadas. Erros não são retentados na ingestão. IDs aceitos ficam
no summary para reconciliação; HTTP de erro/timeout pode ter persistência incerta.
Valida total, job_id e processed_by dos jobs concluídos. Percentis são separados
por fase/coorte de envio. Taxa configurada é **requisições/s**; jobs/s = taxa × lote.

`observed_end_to_end` agora vai do horário programado à observação terminal;
inclui enqueue e polling. Não comparar diretamente com o mesmo campo schema 2,
que começava após o aceite. `server_end_to_end` termina no timestamp de conclusão,
que é anterior ao commit. O runner exige amostras antes da carga e após a drenagem.

## Execução após publicação/deploy do candidato

Primeiro decidir/publicar a próxima versão: há mudança de inicialização do PG
e novo evento público; **não republicar 0.2.0 nem misturar código e pacote antigo**.
Construir e aplicar a imagem correspondente, fixada por `@sha256:...`, e conferir
as variáveis do deployment. O runner não publica nem troca a imagem.

```bash
cd /home/oabreu/Repos/quasar/examples/report_service
.benchmark-venv/bin/pip install 'urllib3==2.5.0' 'psycopg[binary]==3.2.10'

# Usar o digest real da imagem já aplicada.
export EXPECTED_IMAGE='ghcr.io/otavioabreu27/report_service@sha256:DIGEST_REAL'

# Controle com duas réplicas fixas: 5 fases de 60s, 69.000 requisições.
REPLICAS=2 MAX_REPLICAS=2 SCENARIO=candidate-fixed2 \
  ./run_release_benchmark.sh benchmark-results/candidate-fixed2-r1

# Elasticidade 2→8: execução separada, mesmas fases.
REPLICAS=2 MAX_REPLICAS=8 SCENARIO=candidate-elastic \
  ./run_release_benchmark.sh benchmark-results/candidate-elastic-r1

# Lote de 10, mantendo a mesma taxa de JOBS, não 10× a carga.
REPLICAS=2 MAX_REPLICAS=2 BATCH_SIZE=10 \
  STAGES=60:5,60:15,60:30,60:50,60:15 SCENARIO=candidate-batch10 \
  ./run_release_benchmark.sh benchmark-results/candidate-batch10-r1

.benchmark-venv/bin/python analyze_release_benchmark.py \
  benchmark-results/candidate-fixed2-r1 > benchmark-results/candidate-fixed2-r1/analysis.json
```

O runner verifica baseline, exclui o nó gerador da aplicação preservando os
termos existentes de afinidade, e restaura afinidade e HPA ao sair. Não usar em
paralelo com outro operador alterando esse mesmo deployment. Se restauração
falhar, a saída é não-zero e `restore.log` + snapshots permitem recuperação.
O diretório de resultado deve ser novo. Os hashes são da coleta original;
`analysis.json` é um derivado criado depois e não integra aquele manifesto.

Para controle anterior, repetir o **mesmo gerador** com a imagem antiga e seu
pool original de 8, hardware/carga/estado de banco equivalentes. Só nesse caso
usar `REQUIRE_RUNTIME_METRICS=0`, pois 0.2.0 não tem o endpoint novo. A falta dessas
métricas fica explícita. A comparação não deve reutilizar silenciosamente o
gerador antigo com atraso/compensação de carga.

Depois repetir com `COMPLETION_MODE=buffered` já configurado no deployment,
sem mudar mais nada. Fazer pelo menos três repetições por configuração e
alternar a ordem dos controles para reduzir influência de cache/autovacuum.
Cada execução continua tendo 5 minutos de carga, mais preparo/drenagem; não 30.

## Critérios de avaliação

- Zero descartes do gerador para afirmar que uma taxa foi sustentada.
- Separar aceitos, concluídos, erros de envio/poll, resultados incorretos,
  tentativas >1 e backlog ao fim. Não chamar HTTP 202 de trabalho concluído.
- Comparar p95/p99 por fase, claims/job, fração vazia, checkout, falhas,
  waits/CPU/throttling, duração do reaper, consultas e WAL por operação.
- `xact_commit` inclui leituras; não significa commits de escrita nem fsyncs.
  WAL é da instância; sem isolamento, normalizá-lo por jobs é um custo observado
  da janela, não atribuição exata à aplicação.
- CPU/job usa contadores de cgroup de pods com amostras válidas nos dois extremos.
  Sob HPA, falta de limites para pods recém-criados/encerrados produz resultado
  parcial, não custo inventado. Réplicas fixas são o controle para custo.
- RAM residente é compartilhada. Não dividir pico de RAM pelo total de jobs e
  chamar isso de alocação por requisição.

## Verificações locais

Resultado final: **70 testes passaram** (41 núcleo, 9 PostgreSQL com banco real,
3 SQLite, 5 Mist, 3 serviço, 9 Python). Adaptadores também foram executados com
dependência local do núcleo; seus manifests de publicação não foram alterados.
Build de produção (`gleam export erlang-shipment`) passou. Saídas dos smokes
estão em `benchmark-validation/`: 30/30 jobs no modo transacional individual e
120/120 no modo buffered com lote de quatro, sem erros HTTP/resultados inválidos.

Ao alternar uma dependência Hex/local de mesma versão, rodar `gleam clean` antes
de testar. A validação encontrou FFI antigo em cache no SQLite; após rebuild
limpo os testes passaram. Não distribuir mistura de BEAM novo e FFI antigo.

Regressões automatizadas cobrem demanda fragmentada/wakes, claim expirado,
cancelamento durante handler, heartbeat de job longo, retry de conclusão,
isolamento de token obsoleto, fencing/reaper PostgreSQL, reconexão do listener,
enqueue atômico e execução transacional. Há testes do gerador para saturação,
atraso, batch/coortes, resultado incorreto e ausência/reset de métricas.

Smokes locais usam um PostgreSQL temporário separado. Não usar seus tempos
como comparação de mercado: host, rede e cgroup não são o ambiente k3s.
