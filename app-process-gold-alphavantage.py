import pandas as pd
import numpy as np
import boto3
from datetime import datetime
import os
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

def get_configs():
    silver_bucket = "pucsilver-730335250343"
    silver_prefix = "alphavantage/aapl/"
    gold_bucket = "pucgold-730335250343"
    gold_prefix = "alphavantage/aapl/"
    return silver_bucket, silver_prefix, gold_bucket, gold_prefix

def list_parquet_files(s3, bucket, prefix):
    print("Listando arquivos Parquet no bucket silver...")
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]
        print(f"{len(files)} arquivos encontrados.")
        return files
    except Exception as e:
        print(f"Erro ao listar arquivos do S3: {e}")
        return []

def read_all_parquet_from_s3(s3, bucket, files):
    print("Lendo todos os arquivos Parquet do bucket silver...")
    dfs = []
    for file_key in files:
        try:
            parquet_path = f"s3://{bucket}/{file_key}"
            df = pd.read_parquet(parquet_path, engine="pyarrow", storage_options={"anon": False})
            dfs.append(df)
        except Exception as e:
            print(f"Erro ao ler {file_key}: {e}")
    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
        print(f"Total de registros lidos: {len(df_all)}")
        return df_all
    else:
        print("Nenhum dado lido dos arquivos Parquet.")
        return pd.DataFrame()

def calculate_indicators(df):
    print("Calculando indicadores analíticos...")
    # Médias móveis
    df['sma_10'] = df['close'].rolling(window=10).mean()
    df['sma_15'] = df['close'].rolling(window=15).mean()
    df['sma_25'] = df['close'].rolling(window=25).mean()

    # RSI/IFR (são o mesmo indicador, mas vamos calcular ambos para garantir)
    window_rsi = 14
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window_rsi).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window_rsi).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    df['ifr'] = df['rsi']  # IFR é o nome em português para RSI

    # Remove valores nulos após cálculo dos indicadores
    df = df.dropna(subset=['sma_10', 'sma_15', 'sma_25', 'rsi', 'ifr'])
    print("Indicadores calculados e valores nulos removidos.")
    return df

def write_parquet_to_gold(df, gold_bucket, gold_prefix):
    print("Escrevendo DataFrame analítico em Parquet no bucket gold...")
    try:
        now = datetime.now()
        file_name = f"analytics_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
        parquet_path = f"s3://{gold_bucket}/{gold_prefix}{file_name}"
        df.to_parquet(
            parquet_path,
            engine="pyarrow",
            index=False,
            storage_options={"anon": False}
        )
        print(f"Arquivo Parquet salvo em: {parquet_path}")
    except Exception as e:
        print(f"Erro ao escrever Parquet no S3: {e}")

def write_parquet_for_ml(df, gold_bucket, gold_prefix):
    print("Preparando e salvando DataFrame para Machine Learning...")
    try:
        # Define novo prefixo com sufixo _machine_learning
        ml_prefix = gold_prefix.rstrip('/') + '_machine_learning/'
        # Imputação de valores ausentes em 'volume' com média
        if 'volume' in df.columns:
            imputer = SimpleImputer(strategy='mean')
            df['volume'] = imputer.fit_transform(df[['volume']])
        # Padronização apenas das colunas numéricas
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        scaler = StandardScaler()
        df_scaled = df.copy()
        df_scaled[numeric_cols] = scaler.fit_transform(df[numeric_cols])
        now = datetime.now()
        file_name = f"analytics_ml_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
        parquet_path = f"s3://{gold_bucket}/{ml_prefix}{file_name}"
        print(f"Salvando Parquet para ML em: {parquet_path}")
        print("DataFrame para ML:")
        print(df_scaled.head(20))
        df_scaled.to_parquet(
            parquet_path,
            engine="pyarrow",
            index=False,
            storage_options={"anon": False}
        )
        print(f"Arquivo Parquet para ML salvo em: {parquet_path}")
    except Exception as e:
        print(f"Erro ao salvar Parquet para ML: {e}")

def main():
    print("Iniciando processamento analítico dos dados AlphaVantage...")
    silver_bucket, silver_prefix, gold_bucket, gold_prefix = get_configs()
    s3 = boto3.client('s3')
    files = list_parquet_files(s3, silver_bucket, silver_prefix)
    if not files:
        print("Nenhum arquivo Parquet encontrado para processar.")
        return
    df = read_all_parquet_from_s3(s3, silver_bucket, files)
    if df.empty:
        print("DataFrame vazio. Nada será processado.")
        return
    df = calculate_indicators(df)
    print("DataFrame analítico:")
    print(df.head(20))
    write_parquet_to_gold(df, gold_bucket, gold_prefix)
    write_parquet_for_ml(df, gold_bucket, gold_prefix)
    print("Processamento analítico finalizado.")

if __name__ == "__main__":
    main()
