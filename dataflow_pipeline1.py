import logging, json, re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json

SCHEMA = parse_table_schema_from_json(json.dumps(json.load(open("gs://us-central1-nilesh-project1-57d424ca-bucket/dags/schema1.json"))))

class DataIngestion:
    def parse_method(self, string_input):
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '', string_input)))
        row = dict(
            zip(("Emp_ID",	"Name_Prefix",	"First_Name", "Middle_Initial",	"Last_Name",	
                "Gender",	"E_Mail",	"Father_s_Name",  "Mother_s_Name",  "Mother_s_Maiden_Name",
                "Date_of_Birth","Time_of_Birth","Age_in_Yrs_","Weight_in_Kgs_","Date_of_Joining",
                "Quarter_of_Joining","Half_of_Joining","Year_of_Joining","Month_of_Joining","Month_Name_of_Joining",
                "Short_Month","Day_of_Joining","DOW_of_Joining","Short_DOW","Age_in_Company__Years_",
                "Salary","Last___Hike","SSN","Phone_No_","Place_Name", "County","City","State",
                "Zip","Region","User_Name","Password"), values))
        return row


def run(argv=None):   
    data_ingestion = DataIngestion()
    p= beam.Pipeline(options=PipelineOptions())
    (p
     | 'Read from a File' >>  beam.io.ReadFromText('gs://first-project-dataflow-bucket1/100 Records.csv', skip_header_lines=1)
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery('first-project-dataflow:employee.employee_raw',
                                            schema=SCHEMA,
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()