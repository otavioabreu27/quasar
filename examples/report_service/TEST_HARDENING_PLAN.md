# 📋 Plano de Fortalecimento de Testes de Performance e Resiliência

**Data de Criação:** 05 de Setembro de 2026  
**Sistema:** Quasar Report Service (**Gleam / BEAM OTP 27 / Mist / PostgreSQL 16**)  
**Objetivo:** Guia passo a passo para execução de testes avançados de caos, carga realista de I/O, latência de rede em nuvem, longevidade (soak testing) e volume massivo de banco de dados.

---

## 📑 Sumário dos Processos

1. [Processo 1: Engenharia de Caos (Queda de Nó no Meio da Carga)](#processo-1-engenharia-de-caos-queda-de-nó-no-meio-da-carga)
2. [Processo 2: Carga de Trabalho Realista (I/O e CPU Simulado)](#processo-2-carga-de-trabalho-realista-io-e-cpu-simulado)
3. [Processo 3: Simulação de Rede em Nuvem (Latência com `tc/netem`)](#processo-3-simulação-de-rede-em-nuvem-latência-com-tcnetem)
4. [Processo 4: Teste de Longevidade e Estabilidade (Soak Testing)](#processo-4-teste-de-longevidade-e-estabilidade-soak-testing)
5. [Processo 5: Teste com Volume de Histórico no Banco (Data Bloat)](#processo-5-teste-com-volume-de-histórico-no-banco-data-bloat)

---

## Processo 1: Engenharia de Caos (Queda de Nó no Meio da Carga)

### 🎯 Objetivo:
Validar se o Quasar recupera automaticamente jobs interrompidos no meio da execução quando um nó sofre um `crash` abrupto (`SIGKILL`), sem intervenção humana e sem perda de dados.

### 🛠️ Procedimento:
1. Iniciar a carga de 5.000 jobs:
   ```bash
   python3 load_test.py --jobs 5000 --clients 32 \
     --url http://127.0.0.1:8080 \
     --url http://127.0.0.1:8081 \
     --url http://127.0.0.1:18083 \
     --url http://127.0.0.1:18084 &
   ```
2. Após 3 a 5 segundos de execução, derrubar o processo de uma das instâncias (`reports-c` na porta 18083):
   ```bash
   kill -9 $(ss -tulpn | grep 18083 | awk -F "pid=" "{print \$2}" | awk -F "," "{print \$1}")
   ```
3. **Critérios de Sucesso e Verificação:**
   * Todos os 5.000 jobs devem concluir como `completed` (os nós restantes assumem os jobs órfãos após expirar a lease).
   * O relatório em `attempts` deve registrar jobs concluídos na tentativa `2` (`"attempts": {"1": 4800, "2": 200}`).

---

## Processo 2: Carga de Trabalho Realista (I/O e CPU Simulado)

### 🎯 Objetivo:
Avaliar a capacidade da arquitetura quando cada job executa regras de negócio com tempo de espera real (simulando chamadas bancárias externas de 20 ms a 50 ms).

### 🛠️ Procedimento:
1. Iniciar as instâncias com a variável `SIMULATED_WORK_MS` configurada:
   ```bash
   # Exemplo para uma instância simulando 25ms por job:
   SIMULATED_WORK_MS=25 WORKER_CONCURRENCY=16 WORKER_PREFETCH=2 DB_POOL_SIZE=25 \
     INSTANCE_ID=reports-a PORT=8080 gleam run
   ```
2. Executar o teste de carga:
   ```bash
   python3 load_test.py --jobs 2000 --clients 32 --target-tps 200 \
     --url http://127.0.0.1:8080 \
     --url http://127.0.0.1:8081 \
     --url http://127.0.0.1:18083 \
     --url http://127.0.0.1:18084
   ```
3. **Critérios de Sucesso e Verificação:**
   * Validar se o tempo médio de vida do job (`end_to_end_job_latency`) reflete os 25 ms de trabalho real com paralelismo correto.

---

## Processo 3: Simulação de Rede em Nuvem (Latência com `tc/netem`)

### 🎯 Objetivo:
Remover o benefício da latência irreal de `localhost` (0.05 ms) e simular a latência real de uma VPC de Cloud (ex.: AWS EKS conectando ao RDS PostgreSQL com 2 ms de ida e volta).

### 🛠️ Procedimento:
1. Injetar latência artificial de `2.0 ms` com jitter de `± 0.5 ms` na interface local:
   ```bash
   sudo tc qdisc add dev lo root netem delay 2ms 0.5ms
   ```
2. Executar a bateria de testes de homologação:
   ```bash
   python3 load_test.py --jobs 3000 --clients 32 --target-tps 300 \
     --url http://127.0.0.1:8080 \
     --url http://127.0.0.1:8081 \
     --url http://127.0.0.1:18083 \
     --url http://127.0.0.1:18084
   ```
3. **Limpar a configuração de rede ao término do teste:**
   ```bash
   sudo tc qdisc del dev lo root
   ```

---

## Processo 4: Teste de Longevidade e Estabilidade (Soak Testing)

### 🎯 Objetivo:
Submeter o cluster a tráfego ininterrupto por **30 a 60 minutos** para identificar degradação de performance, vazamentos de memória (memory leaks) e contenção de Checkpoints / WAL no PostgreSQL.

### 🛠️ Procedimento:
1. Executar um teste contínuo com 100.000 jobs @ 50 TPS sustentados (~35 minutos):
   ```bash
   python3 load_test.py --jobs 100000 --clients 16 --target-tps 50 --timeout 3600 \
     --url http://127.0.0.1:8080 \
     --url http://127.0.0.1:8081 \
     --url http://127.0.0.1:18083 \
     --url http://127.0.0.1:18084
   ```
2. Monitorar em paralelo as métricas do banco de dados:
   ```bash
   docker stats report_service-postgres-1
   ```
3. **Critérios de Sucesso e Verificação:**
   * A memória RAM dos nós BEAM (`cluster_ram_mb`) deve permanecer plana do início ao fim.
   * Não deve ocorrer acúmulo infinito de conexões órfãs no PostgreSQL.

---

## Processo 5: Teste com Volume de Histórico no Banco (Data Bloat)

### 🎯 Objetivo:
Avaliar se os índices e as queries com `SKIP LOCKED` sofrem lentidão quando as tabelas do PostgreSQL já possuem centenas de milhares ou milhões de registros antigos persistidos.

### 🛠️ Procedimento:
1. Popular 1 milhão de registros históricos no banco:
   ```bash
   docker compose exec postgres psql -U reports -d reports -c "
     INSERT INTO quasar_jobs (queue, payload, status, attempt, inserted_at, completed_at)
     SELECT 'reports', '100', 'completed', 1, NOW() - interval '30 days', NOW() - interval '30 days'
     FROM generate_series(1, 1000000);
     VACUUM ANALYZE quasar_jobs;
   "
   ```
2. Executar o teste de carga sobre o banco volumoso:
   ```bash
   python3 load_test.py --jobs 5000 --clients 32 \
     --url http://127.0.0.1:8080 \
     --url http://127.0.0.1:8081 \
     --url http://127.0.0.1:18083 \
     --url http://127.0.0.1:18084
   ```
3. **Critérios de Sucesso e Verificação:**
   * A latência de `claim` e `enqueue` deve permanecer baixa, comprovando o uso adequado dos índices parciais da fila.
