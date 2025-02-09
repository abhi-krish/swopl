from django.shortcuts import render
from django.http import JsonResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
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
            
            all_columns = set()

            for table in table_names:
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

