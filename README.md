# Case IPCA-ETL

Este projeto tem como objetivo **capturar os dados do IPCA (Índice Nacional de Preços ao Consumidor Amplo)** diretamente da API do IBGE processá-los e armazená-los em formato parquet, ele segue uma abordagem simples de pipeline de dados com Python, demonstrando conceitos de **ETL/ELT** de forma prática.

---

## Estrutura do Projeto


ipca_etl/ # Pasta principal do projeto com script e arquivos salvos

     data/ # Onde os arquivos Parquet serão salvos

Questão 1 - Arquitetura ELT.txt

Questão 2 - Solução para captura de dados.txt

README.md # Este arquivo


---

## Como Funciona

O pipeline segue a lógica:

1. **Extract (Extração):** Captura dos dados da API do IBGE.
2. **Load (Carga):** Armazenamento dos dados brutos em formato Parquet na pasta `data/`.
3. **Transform (Transformação):** Estruturação dos dados em DataFrame, renomeando colunas e tratando datas.
4. **Analytics (Análise):** (Opcional) Os dados ficam prontos para análise, dashboards ou modelos preditivos.

---

## Requisitos

- Python 3.8+
- Bibliotecas:

```bash
pip install pandas requests pyarrow
