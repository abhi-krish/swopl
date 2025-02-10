from django.urls import path
from .views import ClickHouseColumnNamesView, FieldMappingsView, ClickHouseTableDefinitionsView, GetFieldMappingsView
from .views import S3DataLoadView

urlpatterns = [
    path('columns/', ClickHouseColumnNamesView.as_view(), name='get_columns'),
    path('field-mappings/', FieldMappingsView.as_view(), name='field_mappings'),
    path('load_s3_data/', S3DataLoadView.as_view(), name='load_s3_data'),
    path('clickhouse_tables/', ClickHouseTableDefinitionsView.as_view(), name='clickhouse_tables'),
    path('field_mappings_clickhouse/', GetFieldMappingsView.as_view(), name='field_mappings_clickhouse'),
]
