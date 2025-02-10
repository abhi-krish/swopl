from django.shortcuts import render
from django.http import JsonResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from loan_data.services.clickhouse_connector import get_clickhouse_client
from loan_data.services.warehouse_loader import load_s3_csvs_to_clickhouse
import uuid

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
            
            all_columns = set()

            for table in table_names:
                if table == 'field_mappings':
                    continue
                # Get columns for each table
                describe_query = f"DESCRIBE TABLE `{table}`"
                result = client.query(describe_query).result_set
                for row in result:
                    all_columns.add(row[0])

            return JsonResponse({"tables": list(all_columns)}, safe=False)
        
        except Exception as e: 
            return JsonResponse({"error": str(e)}, status=500)
        
class FieldMappingsView(APIView):
    def post(self, request):
        """
        Receive and process field mappings from the frontend.
        Expected format: {
            "mappings": [
                {"source": "document_type", "target": "doc_type"},
                {"source": "interest_rate", "target": "rate_of_interest"},
                ...
            ]
        }
        """
        try:
            mappings = request.data.get('mappings', [])
            
            # Here you can add your logic to save or process the mappings
            # For example, save to database or use for data transformation
            print(mappings)
            client = get_clickhouse_client()

            # Insert mappings into ClickHouse
            # records = [(str(uuid.uuid4()), mapping["source"], mapping["target"]) for mapping in mappings]
            # print(records)
            rows = [(str(uuid.uuid4()), mapping["source"], mapping["target"]) for mapping in mappings]
            client.insert("field_mappings", rows, column_names=["id", "source_column", "target_column"])

            
            return Response({
                "message": "Mappings received successfully",
                "mappings": mappings
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class S3DataLoadView(APIView):
    def post(self, request):
        """
        Trigger the ingestion of S3 loan data into ClickHouse.
        """
        load_s3_csvs_to_clickhouse()
        return JsonResponse({"message": "S3 loan data loaded into ClickHouse successfully."})

class ClickHouseTableDefinitionsView(APIView):
    def get(self, request):
        """
        Fetch table definitions (table names and their columns) from ClickHouse for a specific database.
        """
        try:
            client = get_clickhouse_client()

            # Get database name from query parameter, default to 'default'
            database = request.GET.get("database", "default")

            # Fetch all table names from the specified database
            tables_query = f"SHOW TABLES FROM `{database}`"
            tables_result = client.query(tables_query).result_set
            table_names = [row[0] for row in tables_result]  # Extract table names

            tables = []

            for table in table_names:
                # Get column details for each table
                describe_query = f"DESCRIBE TABLE `{database}`.`{table}`"
                result = client.query(describe_query).result_set

                table_definition = {
                    "name": table,
                    "columns": [
                        {"name": row[0], "type": row[1]} for row in result
                    ]
                }

                tables.append(table_definition)

            return JsonResponse({"database": database, "tables": tables}, safe=False)

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

class GetFieldMappingsView(APIView):
    def get(self, request):
        """
        Fetch all field mappings from ClickHouse.
        """
        try:
            client = get_clickhouse_client()
            query = "SELECT source_column, target_column FROM field_mappings"
            result = client.query(query).result_set

            mappings = [{"source": row[0], "target": row[1]} for row in result]

            return Response({"mappings": mappings}, status=status.HTTP_200_OK)
        
        except Exception as e:
            return Response(
                {"error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )