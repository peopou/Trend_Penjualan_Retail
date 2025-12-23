from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch, helpers 
import csv
import re

default_args = {
    'owner': 'Ahmad',
    'start_date': datetime(2011, 1, 1),
    'retries': 1
}

def extract():
    conn = psycopg2.connect(
        host="postgres", port="5432", database="airflow", 
        user="airflow", password="airflow"
    )
    
    df_raw = pd.read_sql("SELECT * FROM sales", conn)
    df_raw.to_csv('/opt/airflow/dags/P2M3_ahmad_kurniawan_data_raw.csv', index=False)
    conn.close()
    
    print(f"Berhasil extract {len(df_raw)} rows dari PostgreSQL")

def transform():
    # Baca dari file raw
    df = pd.read_csv('/opt/airflow/dags/P2M3_ahmad_kurniawan_data_raw.csv')
    
    # 1. REMOVE DUPLICATES
    initial_count = len(df)
    df = df.drop_duplicates()
    duplicates_removed = initial_count - len(df)
    print(f"Duplicates removed: {duplicates_removed}")
    
    # 2. COLUMN NAME NORMALIZATION
    def normalize_column_name(col_name):
        col_name = col_name.lower()
        col_name = re.sub(r'[^\w]', '_', col_name)
        col_name = re.sub(r'_+', '_', col_name).strip('_')
        col_name = col_name.replace('?', '')
        return col_name
    
    df.columns = [normalize_column_name(col) for col in df.columns]
    print(f"Normalized columns: {df.columns.tolist()}")
    
    # 3. HANDLE MISSING VALUES
    def handle_missing_values(df):
        numeric_cols = ['discount', 'sales', 'profit', 'quantity']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        text_cols = ['order_id', 'customer_name', 'country', 'state', 'city', 
                    'region', 'segment', 'ship_mode', 'category', 'sub_category', 'product_name']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')
        
        if 'feedback' in df.columns:
            df['feedback'] = df['feedback'].fillna(False)
            
        return df
    
    df = handle_missing_values(df)
    
    # 4. DATA TYPE CLEANING
    def clean_col(value):
        if pd.isna(value): 
            return 0.0
        if isinstance(value, str):
            cleaned = re.sub(r'[^\d.-]', '', str(value))
            if cleaned == '' or cleaned == '-':
                return 0.0
            return float(cleaned) if cleaned else 0.0
        return float(value)
    
    df['discount'] = df['discount'].apply(clean_col)
    df['sales'] = df['sales'].apply(clean_col)
    df['profit'] = df['profit'].apply(clean_col)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
    df['order_date'] = pd.to_datetime(df['order_date'], format='%m/%d/%Y', errors='coerce')
    df['feedback'] = df['feedback'].astype(str).str.strip().str.upper().isin(['TRUE', 'YES', '1', 'T'])
    
    # 5. TEXT CLEANING
    text_columns = ['order_id', 'customer_name', 'country', 'state', 'city', 
                   'region', 'segment', 'ship_mode', 'category', 'sub_category', 'product_name']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    
    
    df.to_csv('/opt/airflow/dags/P2M3_ahmad_kurniawan_data_clean.csv', index=False, quoting=csv.QUOTE_ALL)
    print(f"Data bersih disimpan: /opt/airflow/dags/P2M3_ahmad_kurniawan_data_clean.csv")


def load():
    df = pd.read_csv('/opt/airflow/dags/P2M3_ahmad_kurniawan_data_clean.csv')

    es = Elasticsearch("http://elasticsearch:9200")

    # Bulk actions
    actions = [
        {
            "_index": "milestone_3", # simpan nama table sebagai milestone_3
            "_source": r.to_dict()
        }
        for _, r in df.iterrows()
    ]

    # Bulk load
    response = helpers.bulk(es, actions)
    print("Bulk upload selesai, hasil:", response)

with DAG(
    'P2M3_Ahmad_Kurniawan_DAG',
    default_args=default_args,
    description='ETL Pipeline',
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract',  
        python_callable=extract
    )
    
    transform_task = PythonOperator(
        task_id='transform',  
        python_callable=transform
    )
    
    load_es_task = PythonOperator(
        task_id='load',
        python_callable=load
    )
    
    extract_task >> transform_task >> load_es_task