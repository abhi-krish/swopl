from django.shortcuts import render
from django.http import JsonResponse
from rest_framework.views import APIView
from loan_data.services.clickhouse_connector import get_clickhouse_client
from loan_data.services.warehouse_loader import load_s3_csvs_to_clickhouse

class ClickHouseColumnNamesView(APIView):
    def get(self, request):
        """
        Fetch column names for all tables in ClickHouse and return as JSON.
        """
        try:
            client = get_clickhouse_client()

            # Fetch all table names
            tables_query = "SHOW TABLES"
            tables_result = client.query(tables_query).result_set
            table_names = [row[0] for row in tables_result]  # Extract table names
            
            all_columns = {}

            for table in table_names:
                # Get columns for each table
                describe_query = f"DESCRIBE TABLE `{table}`"
                result = client.query(describe_query).result_set
                columns = [row[0] for row in result]  # Extract column names

                all_columns[table] = columns

            return JsonResponse({"tables": all_columns}, safe=False)
        
        except Exception as e: 
            return JsonResponse({"error": str(e)}, status=500)

class S3DataLoadView(APIView):
    def post(self, request):
        """
        Trigger the ingestion of S3 loan data into ClickHouse.
        """
        load_s3_csvs_to_clickhouse()
        return JsonResponse({"message": "S3 loan data loaded into ClickHouse successfully."})

