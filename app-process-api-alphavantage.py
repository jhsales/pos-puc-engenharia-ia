import boto3
import pandas as pd
import json
from datetime import datetime
import io
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import numpy as np

# Configurações
def get_configs():
    bronze_bucket = "pucbronze-730335250343"
    bronze_prefix = "alphavantage/aapl/"
    silver_bucket = "pucsilver-730335250343"
    silver_prefix = "alphavantage/aapl/"
    return bronze_bucket, bronze_prefix, silver_bucket, silver_prefix

def list_json_files(s3, bucket, prefix):
    print("Listando arquivos JSON no bucket bronze...")
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]
        print(f"{len(files)} arquivos encontrados.")
        return files
    except Exception as e:
        print(f"Erro ao listar arquivos do S3: {e}")
        return []

def extract_rows_from_files(s3, bucket, files):
    print("Extraindo dados dos arquivos JSON...")
    all_rows = []
    for file_key in files:
        try:
            obj = s3.get_object(Bucket=bucket, Key=file_key)
            data = json.loads(obj['Body'].read())
            meta = data.get("Meta Data", {})
            symbol = meta.get("2. Symbol", "UNKNOWN")
            time_series = None
            for k in data.keys():
                if "Time Series" in k:
                    time_series = data[k]
                    break
            if not time_series:
                continue
            for date, values in time_series.items():
                row = {
                    "date": date,
                    "symbol": symbol,
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"])
                }
                all_rows.append(row)
        except Exception as e:
            print(f"Erro ao processar arquivo {file_key}: {e}")
    print(f"Total de registros extraídos: {len(all_rows)}")
    return all_rows

def create_dataframe(rows):
    print("Criando DataFrame...")
    try:
        df = pd.DataFrame(rows)
        now = datetime.now()
        process_time = now.strftime("%Y-%m-%d %H:%M:%S")
        data_execucao = now.strftime("%Y-%m-%d")
        df["process_time"] = process_time
        df["data"] = data_execucao
        print("DataFrame criado com sucesso.")
        return df
    except Exception as e:
        print(f"Erro ao criar DataFrame: {e}")
        return pd.DataFrame()

def write_parquet_to_silver(df, silver_bucket, silver_prefix):
    print("Escrevendo DataFrame em Parquet no bucket silver...")
    try:
        now = datetime.now()
        file_name = f"prices.parquet"
        parquet_path = f"s3://{silver_bucket}/{silver_prefix}{file_name}"
        df.to_parquet(
            parquet_path,
            engine="pyarrow",
            partition_cols=["data"],
            index=False,
            storage_options={"anon": False}
        )
        print(f"Arquivo Parquet salvo em: {parquet_path}")
    except Exception as e:
        print(f"Erro ao escrever Parquet no S3: {e}")

def data_cleaning(df):
    print("Iniciando data cleaning e pré-processamento com pipeline...")
    try:
        # Define as colunas de preços e volume
        all_cols = ["open", "high", "low", "close", "volume"]
        volume_col = ["volume"]

        # Remove linhas com valores nulos nas colunas de preço
        n_before = len(df)
        df = df.dropna(subset=all_cols)
        n_after = len(df)
        if n_before != n_after:
            print(f"{n_before - n_after} linhas removidas por valores nulos em colunas de preço.")

        # Pipeline para volume: imputação da média
        volume_pipeline = Pipeline([
            ("imputer", SimpleImputer(strategy="mean"))
        ])
        # Aplica imputação na coluna volume
        df[volume_col] = volume_pipeline.fit_transform(df[volume_col])
        print("Data cleaning e pré-processamento concluídos.")
    except Exception as e:
        print(f"Erro no data cleaning: {e}")
    return df

def main():
    print("Iniciando processamento dos dados AlphaVantage...")
    bronze_bucket, bronze_prefix, silver_bucket, silver_prefix = get_configs()
    s3 = boto3.client('s3')
    files = list_json_files(s3, bronze_bucket, bronze_prefix)
    if not files:
        print("Nenhum arquivo encontrado para processar.")
        return
    rows = extract_rows_from_files(s3, bronze_bucket, files)
    if not rows:
        print("Nenhum dado extraído dos arquivos JSON.")
        return
    df = create_dataframe(rows)
    if df.empty:
        print("DataFrame vazio. Nada será gravado.")
        return
    df = data_cleaning(df)
    write_parquet_to_silver(df, silver_bucket, silver_prefix)
    print("Processamento finalizado.")

if __name__ == "__main__":
    main()
