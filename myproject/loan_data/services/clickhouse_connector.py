import clickhouse_connect
import os
import pandas as pd
from django.conf import settings
from loan_data.services.s3_fetcher import S3Fetcher

def get_clickhouse_client():
    """Returns a ClickHouse Cloud client connection using Django settings."""
    return clickhouse_connect.get_client(
        host=settings.CLICKHOUSE["HOST"],
        port=settings.CLICKHOUSE["PORT"],
        username=settings.CLICKHOUSE["USER"],
        password=settings.CLICKHOUSE["PASSWORD"],
        database="default",
        secure=True,
        verify=False
    )

def generate_table_schema():
    """Dynamically generate a ClickHouse table based on CSV files."""
    s3 = S3Fetcher()
    files = s3.list_files()  # List all loan-related CSV files
    
    all_columns = set()
    
    for file in files:
        df = s3.read_csv_from_s3(file)
        all_columns.update(df.columns)  # Add column names to set (ensures uniqueness)
    
    all_columns = sorted(list(all_columns))  # Sort for consistency

    # Generate SQL column definitions (defaulting to String type)
    column_definitions = ", ".join([f"`{col}` String" for col in all_columns])

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS unified_raw_loan_data (
        {column_definitions},
        ingestion_timestamp DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY ingestion_timestamp;
    """

    client = get_clickhouse_client()
    client.command(create_table_query)
    print("✅ Unified table created successfully with schema:", column_definitions)

