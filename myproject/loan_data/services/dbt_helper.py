from openai import OpenAI
import json
import os
import requests
from loan_data.services.clickhouse_connector import get_clickhouse_client

client = OpenAI(api_key="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")
DBT_MODELS_PATH = "dbt_project/models/"
API_BASE_URL = "http://127.0.0.1:8000/api"

def get_table_definitions(database):
    """
    Fetch table definitions from ClickHouse API.
    
    :param database: The ClickHouse database to fetch tables from.
    :param table_type: 'raw' or 'cleansed' (defines which tables to fetch).
    :return: A dictionary containing table definitions.
    """
    api_url = f"http://127.0.0.1:8000/api/clickhouse_tables/"
    params = {"database": database}

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {database} table definitions: {e}")
        return None

def get_field_mappings():
    """
    Fetch field mappings stored in ClickHouse.
    """
    api_url = f"http://127.0.0.1:8000/api/field_mappings_clickhouse/"

    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json().get("mappings", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching field mappings: {e}")
        return None

def generate_dbt_models(raw_tables, cleansed_tables, column_mappings):
    """Generate a separate dbt model for each cleansed table."""
    models = {}

    for cleansed_table in cleansed_tables:
        cleansed_table_name = cleansed_table["name"]
        prompt = f"""
        You are an expert in dbt and SQL, and all generated SQL must be fully compatible with ClickHouse (version 24.10.1.11214). Generate a dbt SQL model for transforming raw tables into the cleansed table `{cleansed_table_name}`.

        ### IMPORTANT:
        - **ClickHouse Compatibility:**
        - Use only functions and type casts supported by ClickHouse. For example, use `toDate()` for date conversions (not `TO_DATE()`).
        - Use ClickHouse data types exactly as defined (e.g., `Float64`, `Decimal(precision, scale)`) and do not change the type of a column arbitrarily.
        - **Column Casting & Data Types:**
        - **Do not cast columns to numeric types if they contain non-numeric values.** For instance, if `loan_id` is a UUID (or a string) in any table, do not cast it to an `Int32`.
        - Ensure that the join keys (e.g., `loan_id`) have the same data type in every table or CTE. Apply casts only where necessary and ensure the casts occur when the column is first defined, not later referencing a parent scope.
        - **CTE Scope and Aliasing:**
        - All CTEs and subqueries must use consistent and correct aliases. For example, if you alias a table as `doc` in a CTE, then use `doc.loan_id` in joins—not another alias.
        - Avoid referencing parent scope columns with cast functions that aren’t supported in ClickHouse (e.g., do not wrap a parent column with `CAST()` in a subquery if that column was not defined there).
        - **Syntax:**
        - Ensure all quotes and backticks are properly opened and closed. No stray or unmatched quotes.
        - **Table Names & Incremental Processing:**
        - Fully qualify all table names. **DO NOT USE REF!!**
            - The raw tables are in the `default` database.
            - The cleansed tables are in the `internal_loan_data` database.
        - Use incremental processing with a `unique_key` for efficient updates.
        - **Transformation Requirements:**
        - Use the provided column mappings to rename fields.
        - Infer joins based on common columns (e.g., `borrower_id`).
        - Apply necessary transformations, including proper CAST operations and date conversions, ensuring that join keys and other columns retain compatible types.

        ### Raw Tables:
        {json.dumps(raw_tables, indent=2)}

        ### Cleansed Table Definition:
        {json.dumps(cleansed_table, indent=2)}

        ### Column Mappings:
        # Source represents the field name from the raw table  
        # Cleansed represents the field name for the cleansed table  
        {json.dumps(column_mappings, indent=2)}

        Return only the SQL code in a Markdown code block (no extra text).
        """


        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": "You are a dbt expert."},
                      {"role": "user", "content": prompt}]
        )

        sql_code = response.choices[0].message.content

        # Remove Markdown code block formatting
        if sql_code.startswith("```sql"):
            sql_code = sql_code[6:-3].strip()

        # Store generated model under the cleansed table name
        models[cleansed_table_name] = sql_code

    return models

def generate_and_save_dbt_model():
    """
    Fetch raw & cleansed table definitions, call OpenAI to generate dbt SQL, and save the models.
    
    :param database: The ClickHouse database to fetch tables from.
    """
    raw_tables = get_table_definitions("default")
    cleansed_tables = get_table_definitions("internal_loan_data")
    
    if not raw_tables or "tables" not in raw_tables:
        print("❌ Failed to fetch raw table definitions.")
        return
    
    if not cleansed_tables or "tables" not in cleansed_tables:
        print("❌ Failed to fetch cleansed table definitions.")
        return

    raw_tables = raw_tables["tables"]
    cleansed_tables = cleansed_tables["tables"]

    models_dir = "dbt_project/models/"
    
    # Generate column mappings automatically based on table definitions
    column_mappings = get_field_mappings()

    dbt_models = generate_dbt_models(raw_tables, cleansed_tables, column_mappings)

    for table_name, sql_code in dbt_models.items():
        model_path = os.path.join(models_dir, f"{table_name}.sql")
        with open(model_path, "w") as f:
            f.write(sql_code)
        print(f"✅ Generated model: {model_path}")


# # Run the function
# generate_and_save_dbt_model(database="default")
