#Importing the libraries
from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.operators.dummy_operator import DummyOperator

#initiating DAG 
dag = DAG(dag_id= 'dataflow_pipeline', start_date=datetime.today(), catchup=False, schedule_interval='@once')

Cloud_Storage_to_Bigquery= BeamRunPythonPipelineOperator(
                                task_id="Cloud_Storage_to_Bigquery",
                                runner="DataflowRunner",
                                py_file="gs://us-central1-df-project-8875f7a8-bucket/dags/dataflow_pipeline1.py",
                                pipeline_options={'tempLocation': 'gs://us-central1-df-project-8875f7a8-bucket/dags/temp', 
                                                'stagingLocation': 'gs://us-central1-df-project-8875f7a8-bucket/dags/temp'},
                                py_options=[],
                                py_requirements=['apache-beam[gcp]==2.41.0'],
                                py_interpreter='python3',
                                py_system_site_packages=False,
                                dataflow_config=DataflowConfiguration(job_name='dataflow-pipeline1-gcs-to-bq', 
                                                                    project_id='first-project-dataflow', 
                                                                    location="us-central1"),
                                dag=dag)

transformation_pipeline= BeamRunPythonPipelineOperator(
                                task_id="transformation_pipeline",
                                runner="DataflowRunner",
                                py_file="gs://us-central1-df-project-8875f7a8-bucket/dags/dataflow_pipeline2.py",
                                pipeline_options={'tempLocation': 'gs://us-central1-df-project-8875f7a8-bucket/dags/temp', 
                                                'stagingLocation': 'gs://us-central1-df-project-8875f7a8-bucket/dags/temp'},
                                py_options=[],
                                py_requirements=['apache-beam[gcp]==2.41.0'],
                                py_interpreter='python3',
                                py_system_site_packages=False,
                                dataflow_config=DataflowConfiguration(job_name='datflow-pipeline-cleaning', 
                                                                    project_id='first-project-dataflow', 
                                                                    location="us-central1"),
                                dag=dag)


Start = DummyOperator(task_id='Start')
End = DummyOperator(task_id='End')

Start >> Cloud_Storage_to_Bigquery >> transformation_pipeline >> End