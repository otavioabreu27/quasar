# Diagnóstico: gerador, HTTP e PostgreSQL

Ensaios de cinco minutos cada, iniciados na noite de 05/09/2026 e encerrados na
madrugada de 06/09, horário de São Paulo. Evidências do teste anterior foram
preservadas. Não houve mudança de pacotes Hex, workers, prefetch, pools ou banco.

## Resultado principal

Com **oito pods fixos e dois processos geradores**, a fase de 500 req/s entregou
29.692 dos 30.000 envios programados: **494,87 req/s efetivas**, com enqueue p95
de **9,320 ms e 9,258 ms**, respectivamente. Foram **68.683 jobs concluídos** no
ensaio completo, todos na primeira tentativa, sem erros HTTP ou resultados
inválidos. O problema de centenas de milissegundos do ensaio anterior não
apareceu nessa configuração.

Ainda houve 317 descartes: 316 por atraso de agendamento/execução do gerador e
um por falta de slot. Portanto, **500 req/s integrais e sem descartes continuam
sem validação**. Também houve nove descartes na fase de 300/s; não devemos
escondê-los arredondando a taxa entregue.

O ensaio diagnóstico posterior, com **um gerador e tracing no serviço**, também
não reproduziu os 302 ms originais: seu enqueue p95 em 500/s foi **56,644 ms**.
Não houve erro HTTP ou falha de job; 67.848 jobs concluíram na primeira tentativa.
Os 1.152 descartes foram todos por atraso do gerador, sem falta de slots.

Isso sustenta investigar o gerador/caminho HTTP antes de aumentar recursos do
PostgreSQL. **Não prova que Python, GIL ou uma biblioteca específica sejam a
única causa.** Há variabilidade entre execuções, e o diagnóstico com tracing
não é um controle de capacidade sem instrumentação.

## Experimentos executados

Ambos usaram cinco fases de 60 s: **50 → 150 → 300 → 500 → 150 req/s**, lote 1,
oito pods fixos, quatro em cada worker, e carga/observer de banco dentro do k3s.
O nó master ficou reservado aos geradores e à coleta durante os testes.

### A. Dois geradores, sem tracing

- Imagem original da release, sem modificações: digest
  `sha256:1177329ebbd53243b6c2b7763b26a59fb964e659683af3a3239c99fe52443a38`.
- Dois processos Python independentes no mesmo pod. O orçamento total continua
  **3 CPUs e 1 GiB**, não três CPUs por processo.
- Cada processo envia metade da taxa: 25 → 75 → 150 → 250 → 75 req/s.
- Cada processo tem 128 slots de submit e 128 de polling. Logo, o agregado passa
  a 256/256; aumentamos também a capacidade de requisições simultâneas, não
  isolamos somente o efeito de múltiplos interpretadores.
- Início coordenado: diferença registrada de **0 ms** entre os processos.
- As amostras e summaries individuais estão identificados pelo gerador.
  Não calculamos percentis globais fazendo média dos percentis individuais.

| Oferta total | Aceitos/concluídos | Descartados | Enqueue p95 gerador 1 | Enqueue p95 gerador 2 |
|---|---:|---:|---:|---:|
| 50/s | 3.000 | 0 | 4,652 ms | 4,645 ms |
| 150/s | 9.000 | 0 | 4,467 ms | 4,450 ms |
| 300/s | 17.991 | 9 | 6,081 ms | 6,128 ms |
| 500/s | 29.692 | 308 | 9,320 ms | 9,258 ms |
| 150/s | 9.000 | 0 | 4,433 ms | 4,458 ms |

### B. Um gerador, com diagnóstico

- Imagem do commit `fc9ffc7`, com métricas opcionais do serviço; mesmos pacotes
  Hex e mesma lógica de negócio. Digest:
  `sha256:67c55d7717c6fb5cb31580f8887b5f5724a990b8e0b82d66ff2554b2732f053d`.
- Ativados `BENCHMARK_HTTP_TIMING=1` e `BENCHMARK_POOL_TRACE=1`.
- Um processo, 128 slots de submit e 128 de polling. Mesmo limite 3 CPUs/1 GiB.
- Handler medido da entrada no atendimento Gleam até a construção da resposta.
  **Não inclui envio de socket, rede, recebimento e parsing no cliente.**
- Enqueue e consulta de status medidos separadamente. Checkout PGO observado
  de forma agregada, incluindo os pools de API, execução e controle.
- Histogramas de cardinalidade fixa. Não foram armazenados SQL, argumentos,
  respostas, payloads ou credenciais no tracing.

| Oferta | Aceitos/concluídos | Descartados | Enqueue HTTP p95 | Job servidor p95 |
|---|---:|---:|---:|---:|
| 50/s | 3.000 | 0 | 3,694 ms | 7 ms |
| 150/s | 8.971 | 29 | 5,755 ms | 19 ms |
| 300/s | 17.737 | 263 | 5,558 ms | 20 ms |
| 500/s | 29.140 | 860 | 56,644 ms | 26 ms |
| 150/s | 9.000 | 0 | 5,919 ms | 20 ms |

Na janela que enquadra a fase de 500/s, oito pods têm métricas nos dois extremos:

| Trecho instrumentado no servidor | Média registrada | Limite superior do bucket do p95 |
|---|---:|---:|
| Handler POST | 6,230 ms | 20 ms |
| Enqueue no serviço | 5,913 ms | 20 ms |
| Handler GET | 1,507 ms | 5 ms |
| Consulta de status | 1,186 ms | 5 ms |
| Aquisição de conexão PGO, todos os pools | — | 1 ms |

Os tempos do handler são medidos com resolução de milissegundos. O checkout é
convertido para milissegundos inteiros: sua média muito baixa não deve ser
apresentada como uma medição precisa em microssegundos. O p95 agregado também
pode esconder eventos raros ou diferenças entre pools.

As janelas dos histogramas incluem a folga entre amostras de 5 s e não são
exatamente a mesma população dos percentis HTTP por coorte de envio. Não é
correto subtrair os percentis e chamar a diferença de “tempo de rede”. Ainda
assim, o contraste mostra que a espera medida dentro do handler não explica
sozinha o tempo observado no cliente.

## Recursos e claims vazios

No controle sem tracing, as oito instâncias mantiveram identidade e contadores
nos dois extremos da carga. A soma de CPU dos containers foi aproximadamente
630,77 CPU-segundos na janela enquadrante, ou **9,184 ms de CPU da aplicação por
job concluído**. Inclui HTTP, fila, execução e manutenção; exclui gerador,
PostgreSQL e componentes externos aos containers. Inclui a pequena folga da
amostragem antes/depois da carga. Não houve incremento de throttling nos
contadores dos containers observados.

Nessa mesma análise:

- 336.299 claims; 279.023 vazios, aproximadamente **83,0%**;
- 68.683 jobs retornados/concluídos: cerca de **4,90 claims por job**;
- zero renovações de lease, falhas de persistência ou jobs recuperados;
- 7,939 transações de banco por job, **incluindo leituras**;
- 1.660 bytes de WAL da instância por job, aproximadamente;
- 2,323 sincronizações de WAL da instância por job, aproximadamente.

Os quocientes PostgreSQL incluem a janela de amostragem e atividade externa.
Não são commits de escrita exclusivos dos jobs. Não estimamos alocação de RAM
por requisição dividindo memória residente pelo número de jobs.

A implementação explica parte dos claims vazios: o enqueue individual notifica
todos os listeners; cada pod acordado e com demanda tenta um claim. Somente
alguns encontram trabalho. O agrupamento de wakes é local, com janela de 1 ms;
ele não elege um único pod para consumir cada notificação. Novos grants de
capacidade também acionam tentativas. Isso é custo de coordenação verificável,
mas não explica por si só a demora de centenas de milissegundos no cliente.

## Como avançar sem mudar a arquitetura prematuramente

1. **Gerador:** manter processos independentes e métricas de descartes. Para
   isolar a causa, comparar um e dois processos com o mesmo total de slots,
   mesma imagem sem tracing e oito pods fixos; repetir e alternar a ordem.
   Medir atraso do agendador, CPU/throttling do gerador e etapas HTTP do cliente.
   O teste executado mudou número de processos e total de slots juntos.
2. **HTTP:** medir separadamente aquisição de conexão HTTP, envio, espera pela
   primeira resposta, leitura e parsing; relacionar com tempos do servidor.
   A nova medição termina antes do envio efetivo do socket. GIL/agendamento,
   conexão reutilizada e transporte são hipóteses, não causas já demonstradas.
3. **Fila:** testar em alteração separada um pequeno backoff com jitter depois
   de claim vazio e agrupamento adaptativo dos wakes. Manter polling de
   recuperação e revalidar latência de fila vazia, bursts, fairness e falhas.
   Não desabilitar notificações nem introduzir um líder/broker sem necessidade.

Não aumentamos workers nem conexões nesta execução. Nenhuma otimização
especulativa do scheduler foi aplicada ou publicada no Hex.

## Evidências e restauração

- `../2026-09-05-release-0.3.0-fixed8-dual-r1/`: controle sem tracing;
- `../2026-09-05-release-0.3.0-fixed8-single-diagnostic-r1/`: diagnóstico;
- em cada diretório: logs, summaries, IDs aceitos, PostgreSQL, Kubernetes,
  `analysis.json`, `phase-details.json`, snapshots e hashes;
- `analyze_generator_diagnosis.py` reproduz as agregações de histogramas;
- ambos os runners retornaram falha por descartes da carga programada, **não
  por falha do serviço**. Toda a carga aceita drenou e a afinidade/HPA foram
  restaurados; pods e ConfigMaps temporários foram removidos;
- depois do diagnóstico, a imagem original foi restaurada e as duas variáveis
  de diagnóstico foram removidas. HPA volta à configuração 2–8 e mantém sua
  janela de estabilização normal. Jobs e relatórios permanecem no banco sob a
  retenção existente; não houve exclusão manual de dados de negócio.

Verificações locais: 12 testes Python, quatro testes Gleam, formatação Gleam e
sintaxe shell. O teste novo de instrumentação verifica preservação do retorno,
propagação da exceção, contagem e ausência de coleta quando desativada.
