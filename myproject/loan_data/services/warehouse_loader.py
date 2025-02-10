import clickhouse_connect
import os
from loan_data.services.s3_fetcher import S3Fetcher
from loan_data.services.clickhouse_connector import get_clickhouse_client, generate_table_schema
import pandas as pd
from django.conf import settings

def create_table_if_not_exists(client, table_name, df):
    # Fix table name by replacing hyphens with underscores
    sanitized_table_name = table_name.replace("-", "_")

    # Create ClickHouse table dynamically based on DataFrame schema
    columns = ", ".join([f"`{col}` String" for col in df.columns])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {sanitized_table_name} ({columns}) 
    ENGINE = MergeTree() 
    ORDER BY tuple()
    """

    print(f"✅ Creating table: {sanitized_table_name}")
    client.command(create_table_query)



def insert_csv_from_s3(client, table_name, s3_path):
    # Fix table name by replacing hyphens with underscores
    sanitized_table_name = table_name.replace("-", "_")

    # Use ClickHouse's S3 import feature
    query = f"""
    INSERT INTO `{sanitized_table_name}`
    SELECT * FROM s3('{s3_path}', 'CSV')
    """

    print(f"✅ Inserting data from {s3_path} into `{sanitized_table_name}`")
    client.command(query)


def load_s3_csvs_to_clickhouse():
    """Load all CSVs from S3 into ClickHouse."""
    s3 = S3Fetcher()
    client = get_clickhouse_client()
    
    files = s3.list_files()  # List CSV files in S3 bucket
    
    for file in files:
        table_name = file.split(".")[0].lower().replace(" ", "_")  # Convert filename to table name
        s3_path = f"s3://lenderdata/{file}"  # Construct S3 URL
        
        print(f"Processing {file} from {s3_path}...")
        
        df = s3.read_csv_from_s3(file)  # Read headers
        create_table_if_not_exists(client, table_name, df)
        
        insert_csv_from_s3(client, table_name, s3_path)
        print(f"🚀 Finished processing {file}.")

    print("✅ All S3 CSV files loaded into ClickHouse.")


def store_schema_mapping(mapping_json):
    client = get_clickhouse_client()
    query = """
    INSERT INTO schema_mappings (raw_table, internal_table, mappings)
    VALUES (%s, %s, %s)
    """
    client.command(query, (mapping_json["raw_table"], mapping_json["internal_table"], json.dumps(mapping_json["mappings"])))
