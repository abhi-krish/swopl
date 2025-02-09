from django.urls import path
from .views import ClickHouseColumnNamesView
from .views import S3DataLoadView

urlpatterns = [
    path('columns/', ClickHouseColumnNamesView.as_view(), name='get_columns'),
    path('load_s3_data/', S3DataLoadView.as_view(), name='load_s3_data')
]
