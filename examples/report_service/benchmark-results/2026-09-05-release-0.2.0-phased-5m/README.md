# Execução preliminar

Esta execução de 5 minutos foi preservada porque a carga HTTP é válida, mas não
deve ser usada para custos PostgreSQL ou recursos da aplicação:

- o túnel em `localhost:5433` estava indisponível durante toda a coleta;
- o observador Kubernetes antigo somava também o pod gerador.

O cenário concluiu 69.000 de 69.000 jobs, sem erros. A repetição corrigida e o
parecer estão em `../2026-09-05-release-0.2.0-phased-5m-verified/`.
