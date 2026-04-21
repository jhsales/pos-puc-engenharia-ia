import requests
import pandas as pd
import boto3
import json
from datetime import datetime, timezone, timedelta

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

# Extrai apenas o dado do dia atual
if data:
    # Identifica a chave correta do time series
    time_series_key = None
    for k in data.keys():
        if "Time Series" in k:
            time_series_key = k
            break
    if not time_series_key:
        print("Chave de série temporal não encontrada no JSON.")
        data = None
    else:
        # Data do dia anterior em UTC (D-1)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        daily_data = data[time_series_key]
        if yesterday in daily_data:
            # Monta um JSON apenas com o dado do dia anterior
            data_yesterday = {yesterday: daily_data[yesterday]}
            # Mostra o dado do dia anterior
            Show_raw_data(data_yesterday)
            # Escreve no S3
            bucket_name = "pucbronze-730335250343"  # Substitua pelo nome do seu bucket
            now = datetime.now()
            object_key = f"alphavantage/{symbol.lower()}/data_{yesterday}.json"
            Write_to_Bronze(data_yesterday, bucket_name, object_key)
        else:
            print(f"Não há dados disponíveis para a data D-1 ({yesterday}) na resposta da API.")
else:
    print("Dados não disponíveis para gravação no S3.")