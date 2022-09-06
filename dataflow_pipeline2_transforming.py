import logging, json
import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT_ID = "first-project-dataflow"
table_schema1 = parse_table_schema_from_json(json.dumps(json.load(open("/home/g14gautam_nilesh/Dataflow/schema2.json"))))
table_schema2 = parse_table_schema_from_json(json.dumps(json.load(open("/home/g14gautam_nilesh/Dataflow/schema3.json"))))
table_schema3= parse_table_schema_from_json(json.dumps(json.load(open("/home/g14gautam_nilesh/Dataflow/schema4.json"))))

def run(argv=None):  

    q1 =  """SELECT
                    *,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time
                FROM 
                    `first-project-dataflow.employee.employee_raw`"""
  
    q2 = """SELECT
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
                    `first-project-dataflow.employee.employee_raw`"""
    
    q3 = """SELECT
                    GENERATE_UUID() as ID , 
                    Emp_ID,
                    Father_s_Name AS Father_Name,
                    Mother_s_Name AS Mother_Name,
                    DATE_DIFF(CURRENT_DATE, COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', Date_of_Birth), 
                                                        SAFE.PARSE_DATE('%m/%d/%Y', Date_of_Birth)), year) AS Age_in_Yrs,
                    Weight_in_Kgs_ AS Weight_in_Kgs,
                    Phone_No_ AS Phone_No,
                    State,
                    Zip,
                    Region,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time
                FROM
                    `first-project-dataflow.employee.employee_raw`"""
    p = beam.Pipeline(options=PipelineOptions())
    
    (p                                    
     | 'Query from BigQuery' >> beam.io.ReadFromBigQuery(query=q1, use_standard_sql=True)
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery('{0}:employee.employee_data'.format(PROJECT_ID),
                                                schema=table_schema1,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

    (p                                    
     | 'Query from BigQuery1' >> beam.io.ReadFromBigQuery(query=q2, use_standard_sql=True)
     | 'Write to BigQuery1' >> beam.io.WriteToBigQuery('{0}:employee.employee_info'.format(PROJECT_ID),
                                                schema=table_schema2,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE) 
    )
    (p                                                                             
     | 'Query from BigQuery2' >> beam.io.ReadFromBigQuery(query=q3, use_standard_sql=True)
     | 'Write to BigQuery2' >> beam.io.WriteToBigQuery('{0}:employee.employee_personal_info'.format(PROJECT_ID),
                                                schema=table_schema3,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    )
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    
      