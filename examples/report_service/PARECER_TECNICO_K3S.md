# Parecer técnico — Report Service no k3s

**Data:** 05/09/2026  
**Escopo:** deployment do `report_service`, elasticidade horizontal e teste de
carga executado dentro do cluster.  
**Resultado:** 10.000 jobs duráveis concluídos sem descarte e sem retry; a
segunda rodada mediu 263,5 jobs/s observados ponta a ponta.

## 1. Sistema e ambiente avaliados

| Componente | Especificação observada |
|---|---|
| Aplicação | Gleam 1.18 / Erlang OTP 27 / Mist / Quasar Jobs |
| Imagem | `ghcr.io/otavioabreu27/report_service:latest` |
| Cluster | K3s v1.36.3+k3s1, 1 master + 2 workers |
| Nós | 4 vCPU por nó; master com 6 GiB e workers com 7 GiB |
| Banco | PostgreSQL 16 em `192.168.15.210`, VM/LXC separado |
| Serviço | `ClusterIP`, porta 8080, namespace `reports` |
| Mínimo/máximo HPA | 2 / 20 pods |
| Requests por pod | 250m CPU e 256Mi RAM |
| Limits por pod | 2 CPU e 1Gi RAM |
| Pool PostgreSQL | 10 conexões por pod |
| Workers por pod | 8 workers duráveis; prefetch 2 |
| HTTP por pod | 32 workers; buffer 256 |

O Deployment usa `INSTANCE_ID` derivado do nome do pod. O HPA escala por CPU
(alvo 70%) ou memória (alvo 80%), sobe sem janela de estabilização e reduz no
máximo 25% por minuto, com janela de estabilização de cinco minutos.

## 2. Método

O gerador foi executado como pod dentro do cluster, acessando diretamente
`http://report-service.reports.svc.cluster.local:8080`, sem `kubectl
port-forward`.

```text
suite: single
jobs: 10.000
clientes de submissão: 64
clientes de polling: 64
timeout: 600 s
```

O script foi executado com `urllib3` habilitado para reutilização de conexões.
Uma tentativa preliminar sem essa dependência terminou em `Cannot assign
requested address` após 5.725 jobs; ela é um diagnóstico do gerador e não deve
ser usada como benchmark da aplicação.

## 3. Resultados medidos

| Métrica | Resultado |
|---|---:|
| Jobs enviados/concluídos | 10.000 / 10.000 |
| Tentativa 1 | 10.000 (100%) |
| Jobs descartados/erros | 0 / 0 |
| Requisições HTTP | 48.304 |
| Tempo observado total | 37,949 s |
| Throughput de jobs | 263,51 jobs/s |
| Throughput HTTP médio | 1.272,8 req/s |
| Enqueue médio | 43,61 ms |
| Enqueue P50 / P95 / P99 | 35,39 / 107,04 / 150,47 ms |
| Latência E2E P50 / P95 / P99 | 29,92 / 35,42 / 36,74 s |
| Polling P95 | 16,29 ms |

O lote gerou aproximadamente 4,83 requisições HTTP por job, pois cada POST é
acompanhado por consultas de status até a conclusão.

### Distribuição por réplica

| Pod | Jobs processados | Participação |
|---|---:|---:|
| `report-service-5b4f75bfcb-bjrs5` | 4.217 | 42,17% |
| `report-service-5b4f75bfcb-4b4gc` | 4.177 | 41,77% |
| `report-service-5b4f75bfcb-wngfk` | 1.606 | 16,06% |

A distribuição não foi uniforme porque o HPA criou réplicas durante a carga;
os pods que entraram depois tiveram menos tempo para consumir a fila. Isso é
evidência de elasticidade, mas não de balanceamento perfeito.

Saídas completas: [rodada com identificação por pod](resultado-carga-k3s-10000-pods.txt)
e [rodada anterior](resultado-carga-k3s-10000-completo.txt).

## 4. Elasticidade e recursos

Durante o teste, o HPA saiu de 2 pods e chegou a 6 réplicas. Em uma amostra de
`kubectl top`, três pods ativos apresentavam aproximadamente:

| Recurso | Soma observada | Média por pod observado |
|---|---:|---:|
| CPU | 2,605 cores | 868m |
| RSS | 212Mi | 70,7Mi |

Esses valores são amostras, não médias temporais. O script de carga reporta
`resource_utilization: {}` porque o `ResourceSampler` procura processos BEAM
locais e não processos remotos nos pods.

### Valores amortizados por job e requisição

São aproximações para dimensionamento, não alocações individuais. RSS é memória
residente e não memória criada por cada request.

| Indicador | Cálculo | Valor aproximado |
|---|---|---:|
| CPU por job | 2,605 cores / 263,51 jobs/s | 9,89 ms CPU/job |
| CPU por request HTTP | 2,605 cores / 1.272,8 req/s | 2,05 ms CPU/req |
| RSS por job no lote | 212Mi / 10.000 | 21,7KiB/job amortizado |
| RSS por request HTTP | 212Mi / 48.304 | 4,49KiB/req amortizado |
| Memória residente por pod | 212Mi / 3 | 70,7Mi/pod |

As estimativas de CPU vêm de uma fotografia; as estimativas de memória apenas
amortizam RSS sobre o lote. Para SLO ou custo, deve-se coletar médias e p95
temporais com Prometheus/VictoriaMetrics.

Há um limite operacional: 20 pods × 10 conexões podem solicitar 200 conexões,
enquanto o PostgreSQL está configurado com `max_connections=100`. Com os demais
sistemas usando o banco, não é seguro buscar 20 réplicas sem reduzir o pool,
usar PgBouncer ou aumentar a capacidade do banco.

## 5. Comparação com outras linguagens

Não foi medido Rust, Go, Node.js, Python, Java ou Ruby com o mesmo código,
hardware, banco, fila, polling e configuração. Comparar HTTP puro com este
serviço seria enganoso, pois o teste inclui transações PostgreSQL, leases,
persistência idempotente, polling e scheduling distribuído.

O [TechEmpower Framework Benchmarks](https://www.techempower.com/benchmarks/)
separa testes de plaintext, JSON, consultas e atualizações de banco; a
[descrição dos testes](https://github.com/TechEmpower/FrameworkBenchmarks/wiki/Project-Information-Framework-Tests-Overview)
explica que plaintext mede fundamentos HTTP, enquanto fortunes e updates
exercitam banco e persistência.

As faixas abaixo são **referências de engenharia, não medições deste ambiente**.
Representam RSS típico de um processo/container pequeno antes de adicionar
banco, fila e observabilidade; framework, flags e workload podem mudar tudo.

| Ecossistema | RSS inicial indicativo | CPU/req indicativo | Observação |
|---|---:|---:|---|
| Rust async | 5–30Mi | 0,1–1,0 ms | baixo overhead; DB pode dominar |
| Go | 10–50Mi | 0,2–1,5 ms | runtime simples; GC sob alocação |
| Gleam/BEAM | 40–100Mi | 0,5–3,0 ms | isolamento/processos; DB domina este teste |
| Node.js | 40–150Mi | 0,5–4,0 ms | event loop; cuidado com bloqueios |
| Python async | 40–180Mi | 1,0–8,0 ms | grande variação por framework/serialização |
| Ruby | 50–250Mi | 2,0–12,0 ms | overhead maior por processo, em geral |
| JVM | 150–500Mi | 1,0–8,0 ms após warm-up | RSS inicial maior; JIT aquece |

Essas faixas não permitem afirmar que uma linguagem é mais rápida neste
workload. O resultado medido é do sistema completo, e não apenas do runtime.

## 6. Parecer e recomendações

O deployment demonstrou elasticidade horizontal funcional e processamento
durável sem perda nos 10.000 jobs. O throughput é válido para uma rodada de
homologação, mas não constitui capacidade de produção: houve uma rodada válida
com o gerador corrigido, o banco é compartilhado e a coleta de recursos não foi
temporalmente completa.

Antes de chamar o resultado de volumetria de produção:

1. reduzir `DB_POOL_SIZE` ou limitar `maxReplicas` para respeitar as 100
   conexões do PostgreSQL;
2. repetir em regime sustentado por 15–30 minutos, com três repetições por nível;
3. coletar CPU, RSS, conexões, locks, WAL, latência e backlog temporalmente;
4. adicionar backlog de `quasar_jobs` ao HPA, pois CPU/memória pode não subir
   enquanto a fila cresce;
5. comparar linguagens somente com o mesmo contrato, banco, conexões, retry e
   método de medição.

**Conclusão:** o serviço está operacional e elástico para testes controlados,
com evidência de expansão de 2 para 6 pods. A principal restrição observada é
o PostgreSQL, não a ausência de autoscaling. O limite de 20 pods é teto
configuracional, não capacidade já validada.
