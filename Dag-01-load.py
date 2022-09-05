from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator


dag = DAG(dag_id= 'DAG-1', start_date=datetime.today(), catchup=False, schedule_interval='@once')


csv_to_bigquery = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    gcp_conn_id='google_cloud_default',
    bucket="first-project-dataflow-bucket1",
    source_objects=['100 Records.csv'],
    skip_leading_rows=1,
    bigquery_conn_id='google_cloud_default',    
    destination_project_dataset_table="first-project-dataflow.myproject_001b.raw_data_employee",
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,
    dag=dag
)


start = DummyOperator(task_id='start'),
end = DummyOperator(task_id='end')


start >> csv_to_bigquery >> end