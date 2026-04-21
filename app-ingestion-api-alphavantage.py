import requests
import pandas as pd
import boto3
import json
from datetime import datetime

def Show_raw_data(dados_json):
    """Imprime o JSON completo de forma formatada na tela."""
    try:
        print(json.dumps(dados_json, ensure_ascii=False, indent=4))
    except Exception as e:
        print(f"Erro ao imprimir JSON: {e}")


def Write_to_Bronze(dados_json, bucket_name, object_key):
    """Escreve os dados brutos em JSON no S3 (camada Bronze)."""
    try:
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(dados_json),
            ContentType='application/json',
        )
        print(f"Dados brutos enviados para s3://{bucket_name}/{object_key}")
    except Exception as e:
        print(f"Erro ao enviar dados para o S3: {e}")

# Configuração
api_key = "WG8SAFGOEXAT40IG"
symbol = "AAPL"
outputsize = "compact"
url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize={outputsize}"

# Requisição
try:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    if not data or "Error Message" in data:
        print(f"Erro na resposta da API: {data.get('Error Message', 'Resposta vazia ou inválida')}")
        data = None
except requests.exceptions.RequestException as e:
    print(f"Erro na requisição HTTP: {e}")
    data = None
except Exception as e:
    print(f"Erro inesperado ao obter dados da API: {e}")
    data = None

# Imprime parte do JSON bruto
if data:
    Show_raw_data(data)

    # Escreve os dados brutos no S3
    bucket_name = "puc-bronze-637423238497"  # Substitua pelo nome do seu bucket
    now = datetime.now()
    object_key = f"alphavantage/{symbol.lower()}/data_{now.strftime('%Y%m%d_%H%M%S')}.json"
    Write_to_Bronze(data, bucket_name, object_key)
else:
    print("Dados não disponíveis para gravação no S3.")