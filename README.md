# 🚀 Engenharia de IA & MLOps: Pipeline de Dados Medalhão

Este repositório faz parte da minha especialização em **Engenharia de IA e MLOps na PUC Minas**. O objetivo aqui é demonstrar a construção de um pipeline de dados robusto, utilizando a arquitetura medalhão para processar dados financeiros da bolsa de valores (Apple - AAPL).

## 🏗️ Arquitetura do Projeto (Medallion Architecture)

O projeto foi estruturado em camadas para garantir a qualidade e a governança dos dados que alimentam os modelos de Machine Learning:

* **Camada Bronze (Raw):** Ingestão dos dados brutos extraídos via API AlphaVantage e armazenados em formato original no AWS S3.
* **Camada Silver (Trusted):** Limpeza de dados, tratamento de nulos e padronização de tipos.
* **Camada Gold (Refined):** Engenharia de Recursos (Feature Engineering). Aqui são calculados indicadores técnicos como Médias Móveis (SMA) e RSI, além da aplicação de `StandardScaler` para preparar os dados para o treinamento de IA.

## 🛠️ Tecnologias e Ferramentas

* **Linguagem:** Python 3.x
* **Manipulação de Dados:** Pandas, NumPy
* **Infraestrutura Cloud:** AWS S3 (armazenamento de objetos) e Boto3
* **Machine Learning:** Scikit-Learn (Pré-processamento)
* **Versionamento:** Git & GitHub

## 📂 Estrutura de Arquivos

* `app-ingestion.py`: Script responsável pela extração (Bronze).
* `app-process-silver.py`: Script de saneamento e limpeza (Silver).
* `app-process-gold.py`: Script de Feature Engineering e preparação para ML (Gold).
* `.gitignore`: Configurado para proteger chaves de acesso e evitar upload de arquivos binários pesados.

---
💡 *Este projeto demonstra habilidades em Engenharia de Dados, Cloud Computing e preparação de datasets para modelos produtivos de Inteligência Artificial.*
