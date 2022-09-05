#1
from airflow import DAG
#from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
#from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator

#2
dag = DAG(dag_id= 'DAG-2-trans', start_date=datetime.today(), catchup=False, schedule_interval='@once')

#4
query1="""SELECT
                *,
                FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time
                FROM 
                 `first-project-dataflow.myproject_001b.raw_data_employee`"""


problem_statement_1 = BigQueryOperator(
    task_id='problem_statement_1',
    sql=query1,
    destination_dataset_table='first-project-dataflow.myproject_001b.employee_data',
    use_legacy_sql=False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

query2="""SELECT
                    Emp_ID,
                    CONCAT(First_Name, " ",Last_Name) AS Name,
                    Gender,
                    E_Mail AS Email,
                    Date_of_Birth,
                    Date_of_Joining,
                    Age_in_Company__Years_ AS Age_in_Company,
                    Salary,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time
                FROM
                    `first-project-dataflow.myproject_001b.raw_data_employee`"""


problem_statement_2a = BigQueryOperator(
    task_id='create_employee_table',
    sql=query2,
    destination_dataset_table='first-project-dataflow.myproject_001b.employee',
    use_legacy_sql=False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

query3="""SELECT
                    GENERATE_UUID() as ID , 
                    Emp_ID,
                    Father_s_Name AS Father_Name,
                    Mother_s_Name AS Mother_Name,
                    DATE_DIFF(CURRENT_DATE, CAST(Date_of_Birth AS DATE), year) AS Age_in_Yrs,
                    Weight_in_Kgs_ AS Weight_in_Kgs,
                    Phone_No__ AS Phone_No,
                    State,
                    Zip,
                    Region,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time
                FROM
                    `first-project-dataflow.myproject_001b.raw_data_employee`"""


problem_statement_2b = BigQueryOperator(
    task_id='create_employee_personal_info_table',
    sql=query3,
    destination_dataset_table='first-project-dataflow.myproject_001b.employee_personal_info',
    use_legacy_sql=False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

start = DummyOperator(task_id='start'),
end = DummyOperator(task_id='end')

start >> problem_statement_1 >> problem_statement_2a >> problem_statement_2b >> end