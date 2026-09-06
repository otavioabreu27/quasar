# Quasar 0.3.0 — benchmark elástico de cinco minutos

Execução em 05/09/2026, horário de São Paulo (06/09 em UTC).

## Parecer

A release foi publicada, implantada e exercitada com sucesso funcional. Os
**66.203 jobs enviados foram concluídos corretamente, todos na primeira tentativa**.
Não houve erros HTTP, resultados inválidos, falhas terminais, reinícios de pods
durante a carga ou backlog ao terminar. A drenagem levou aproximadamente 0,1 s.

Isso **não significa que 500 requisições/s foram sustentadas**: das 69.000
programadas, 2.797 não foram enviadas. O gerador tem capacidade e atraso máximos
explícitos; não acumula requisições atrasadas para compensá-las depois. Seu pod
terminou com código 2 e o runner com código 1, corretamente sinalizando que a
carga solicitada não foi integralmente entregue. A coleta e a restauração do
cluster terminaram normalmente.

O sinal mais importante é a separação entre execução durável e caminho HTTP:
na fase de 500/s, o job no servidor teve p95 de **16 ms**, enquanto o enqueue
HTTP teve p95 de **302 ms**. O gargalo observado não pode ser atribuído somente
ao PostgreSQL ou ao handler. É necessário separar gerador, transporte, admissão
HTTP e espera do cliente antes de fazer mais alterações estruturais na fila.

## Versões e ambiente

- `quasar_jobs`, `quasar_postgres` e `quasar_mist`: **0.3.0 obtidas do Hex**, com
  versões exatas e checksums no manifest. SQLite 0.3.0 também publicado, mas não
  utilizado neste serviço. Constellation permanece na linha 0.2.
- Commit da imagem: `92f4f45bf2e915730b508c745232bd357cdb76c5`.
- Actions: https://github.com/otavioabreu27/quasar/actions/runs/34006200458
- Imagem implantada:
  `ghcr.io/otavioabreu27/report_service@sha256:1177329ebbd53243b6c2b7763b26a59fb964e659683af3a3239c99fe52443a38`.
- k3s `v1.36.3+k3s1`, três nós amd64. Aplicação somente nos dois workers durante
  a medição; gerador e observer PostgreSQL no master. A afinidade original foi
  restaurada depois. Os dois pods iniciais ficaram no worker-2; a distribuição
  entre os workers mudou com o HPA, outra variável desta execução elástica.
- HPA 2–8 pods, CPU alvo 70% de request 250m; memória alvo 80% de request 256Mi.
  Limite por pod: 2 CPUs / 1GiB. Escalar por CPU/request não implica consumir
  70% do limite de duas CPUs.
- Por pod: 8 workers, prefetch 1; pool PostgreSQL API/execução/controle = 3/4/1,
  mais um listener dedicado; polling 5 s; reaper 1 s; conclusão transacional.
- PostgreSQL **15.19** na VM `192.168.15.210:5432`, limite 100 conexões.
  Não foi usado túnel. O pico observado foi 73 conexões no banco da aplicação;
  esse número não inclui clientes de outros bancos da mesma instância.
- Banco compartilhado, sem limpeza/reset: estimativa inicial de 1.105.611 jobs,
  aproximadamente 162 MB de tabela e 64 MB de índices. Jobs de outras filas
  foram preservados. Retenção existente permanece ativa.
- Job: soma aritmética de 1 até 1.000.000 por fórmula O(1), persistência do
  relatório e conclusão transacional. Não é um teste de um milhão de iterações.
- Gerador Python/urllib3, 128 slots de submit e 128 de polling; atraso máximo
  admitido de 50 ms; lote 1; polling inicial após 50 ms; sem retry de submit.

## Resultados por fase

Cada fase dura 60 s. Conclusões são agrupadas pela fase de envio, mesmo se a
observação terminar depois dela. “Aceitos/s” é aceitos divididos pelos 60 s da
fase, não um pico nem uma garantia de taxa constante.

| Oferta req/s | Programados | Aceitos/concluídos | Não enviados | Aceitos/s | Enqueue p95 | Job servidor p95 | E2E observado p95 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 50 | 3.000 | 3.000 | 0 | 50 | 3,535 ms | 7 ms | 56,303 ms |
| 150 | 9.000 | 9.000 | 0 | 150 | 4,304 ms | 13 ms | 57,497 ms |
| 300 | 18.000 | 18.000 | 0 | 300 | 5,280 ms | 13 ms | 59,343 ms |
| 500 | 30.000 | 27.207 | 2.793 | 453,45 | 302,353 ms | 16 ms | 650,330 ms |
| 150, recuperação | 9.000 | 8.996 | 4 | 149,93 | 5,864 ms | 14 ms | 61,015 ms |

Os descartes totais foram 2.628 por falta de slot de submit, 154 por atraso no
executor e 15 por atraso do agendador. Portanto, não são 2.797 recusas do servidor:
são requisições que não chegaram a ser enviadas. Na fase de 500/s, faltaram
**9,31%** dos envios programados; quatro descartes adicionais ocorreram na
transição/recuperação.

Foram 132.407 operações HTTP: 66.203 POSTs e 66.204 consultas de status. Backlog
máximo do cliente: 157 jobs aguardando observação. Isso não equivale ao backlog
persistido da fila. Nas amostras de 5 s, a fila disponível chegou a 2 jobs e a
execução a 6; picos entre amostras podem não ter sido capturados.

## Recursos, PostgreSQL e custo interno

Valores de CPU abaixo são médias aritméticas das amostras do metrics-server,
com suas próprias janelas móveis. Não são integração precisa de CPU por fase.

| Fase req/s | Pods prontos observados | CPU média aplicação | RAM máxima aplicação |
|---|---:|---:|---:|
| 50 | 2 | 0,177 core | 143,9 MiB |
| 150 | 2–6 | 0,793 core | 428,6 MiB |
| 300 | 6–8 | 2,427 cores | 583,4 MiB |
| 500 | 8 | 3,937 cores | 598,3 MiB |
| Recuperação | 8 | 2,771 cores | 602,7 MiB |

O gerador atingiu aproximadamente 1,16 core nas amostras, contra limite de três
CPUs. Isso não elimina gargalo de cliente: GIL, agendamento e limite de slots
não se resolvem automaticamente com CPUs disponíveis. A latência HTTP maior
também mantém slots ocupados. A medição atual não separa essas contribuições.

PostgreSQL: 63 amostras sem erro, zero incremento de rollbacks, até 10 conexões
ativas nas amostras da fase de 500/s. Houve esperas de WAL; predominou ClientRead,
incluindo conexões ociosas. Contar amostras de espera não mede o tempo total
dessas esperas. Não há `pg_stat_statements` instalado; nenhuma extensão foi
instalada nem estatística compartilhada foi resetada.

Na janela de 305 s que enquadra carga e drenagem:

- 8,650 transações PostgreSQL por job concluído, **incluindo leituras**;
- 2.280 bytes de WAL da instância por job concluído;
- 2,520 sincronizações de WAL da instância por job concluído.

Esses quocientes incluem atividade externa e folga de amostragem. Não são
commits de escrita por job, atribuição exclusiva à aplicação ou custo monetário.
O `analysis.json` deixa CPU/job indisponível para o ensaio completo: seis pods
foram criados durante a carga e não têm amostras nos dois extremos. Não estimamos
alocação de RAM por requisição dividindo memória residente pelo total de jobs.

No último snapshot das oito instâncias, os contadores acumulados desde seus
respectivos inícios (incluindo espera pré-carga) mostraram:

- 371.618 claims, dos quais 307.701 vazios: **82,8%**;
- 66.203 jobs concluídos; zero renovações de lease, falhas de persistência,
  falhas de claim ou jobs recuperados pelo reaper;
- 525.748 wakes recebidos, 185.717 coalescidos;
- 132.407 eventos de espera na fila HTTP, todos nos buckets de até 5 ms
  (132.298 no bucket de até 1 ms).

Logo, a remoção da renovação inicial está visível, mas o custo de acordar todos
os pods ainda é relevante. Os ~302 ms do enqueue não são explicados pela espera
instrumentada da fila HTTP, que cobre apenas uma parte do caminho da requisição.
Não temos os mesmos contadores no teste antigo para afirmar a redução percentual
de claims vazios.

## Comparação honesta com 0.2.0

A execução anterior registrada em `2026-09-05-release-0.2.0-phased-5m-verified`
teve 15 erros de submit, 24 erros de polling e sete jobs com segunda tentativa.
Nesta execução, esses três indicadores foram zero. Entretanto, o gerador antigo
acumulava atraso e compensava envios: seu atraso p95 foi 7,1 s. O novo descarta
envios atrasados e contabiliza a carga que não entregou. Portanto, comparar
68.985 conclusões antigas com 66.203 novas **não demonstra regressão de throughput**,
assim como zero erros novos não comprova sucesso sob toda a oferta de 500/s.

Os sinais de menor CPU e de jobs mais rápidos são favoráveis, mas não formam um
A/B controlado: número de pods por fase, população/cache do banco e gerador
diferem. O WAL observado por job também ficou maior (aproximadamente 2.280 contra
1.406 bytes), com janelas e população diferentes. Não há evidência para declarar
uma melhoria uniforme de todos os custos, nem comparação direta com outras
linguagens ou produtos.

**Conclusão:** houve melhora funcional e um minuto de 300/s integralmente
entregue com baixa latência neste ensaio. A região de 500/s continua sem validação.
O próximo ensaio deve separar o limite do cliente/caminho HTTP: réplicas fixas,
mesmo gerador/mesma imagem, variar somente a capacidade ou distribuição do
gerador e medir novamente 400–500/s. Depois, repetir o controle antigo com o
mesmo gerador para atribuir ganhos à release. Não aumentar pools nem introduzir
outro componente de fila com base apenas neste resultado.

## Evidências e estado final

- `load.jsonl`: amostras e summary completo, inclusive IDs aceitos.
- `postgresql-observer.jsonl`, `kubernetes-observer.jsonl`: séries temporais.
- `analysis.json`: análise conservadora produzida pelo script versionado.
- `resource-details.json`: agregações complementares das amostras por fase e
  contadores de vida dos pods, explicitamente distintos de deltas da carga.
- `experiment.json`, `scripts.sha256`, snapshots: configuração e identidade.
- `checksums.sha256`: hashes originais, verificados após execução.
- `restore.log`: restauração bem-sucedida da afinidade e do HPA 2–8.
- `../2026-09-05-release-0.3.0-deployment`: snapshots do deploy e smoke funcional.

A aplicação permanece na 0.3.0. Os pods e ConfigMaps temporários do benchmark
foram removidos; as evidências locais permanecem disponíveis. Os jobs/relatórios
gerados continuam no banco sob a política de retenção existente. O HPA mantém
sua estabilização de redução; não foi forçada uma redução de réplicas ao encerrar.
