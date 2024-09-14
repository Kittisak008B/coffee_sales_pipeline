from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator
from google.cloud import storage

from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa

# Configuration
GCS_BUCKET = 'gs://us-central1-composer9999-6cd15f2e-bucket' 
EXPORT_PATH = '/home/airflow/gcs/data'
INSTANCE_NAME = 'testmysqldb' # Cloud SQL instance
DATABASE_NAME = 'demo_database01'
TABLES = ['transaction', 'customer', 'coffee']  # List of tables to export

# Default arguments
default_args = {
    'owner': 'user007', 
    'depends_on_past': False,
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(0), #datetime(2024, 9, 10),
}

@dag(default_args=default_args, schedule_interval=timedelta(hours=1 , minutes=0), tags=["coffee_sales_transactions"], max_active_runs=1, catchup=False, description="A DAG")
def coffee_sales_dag() :
    
    @task()
    def convert_csv_to_parquet(table_name) : #Converts CSV to Parquet and uploads it to Google Cloud Storage
        csv_file = f'{EXPORT_PATH}/{table_name}.csv'
        df = pd.read_csv(csv_file)
        
        parquet_file = f'{EXPORT_PATH}/{table_name}.parquet'
        # local path /home/airflow/gcs/data/ is linked to GCS, files written there will automatically appear in  GCS bucket
        df.to_parquet(parquet_file, engine='pyarrow', index=False) 
        
        # Upload Parquet to GCS
        # client = storage.Client() 
        # bucket = client.bucket(GCS_BUCKET.replace('gs://', ''))
        # blob = bucket.blob(f'data/{table_name}.parquet')
        # blob.upload_from_filename(parquet_file)
    
    @task()
    def merge_data() :
        transaction_df = pd.read_parquet("/home/airflow/gcs/data/transaction.parquet")
        coffee_df = pd.read_parquet("/home/airflow/gcs/data/coffee.parquet")
        customer_df = pd.read_parquet("/home/airflow/gcs/data/customer.parquet")
    
        #merge
        merge_df = pd.merge(transaction_df , coffee_df ,how = 'left' ,left_on ='coffee_id' ,right_on = 'coffee_id')
        final_df = pd.merge(merge_df , customer_df , how = 'left' , left_on ='card' ,right_on = 'card')
    
        final_df = final_df.drop(columns=['coffee_id','card'])
    
        column_order = ['date', 'datetime', 'customer_name', 'cash_type', 'coffee_name', 'money'] # Reorder the columns
        final_df = final_df[column_order]
    
        final_df = final_df.where(pd.notnull(final_df), None)  # Replace NaN with None
        final_df.to_parquet("/home/airflow/gcs/data/final_sales_df.parquet", index=False)
        print('Task has been completed successfully')
        
        
    t1 = CloudSqlInstanceExportOperator(    # Export table to CSV
            task_id = f'sql_export_{TABLES[0]}',
            instance = INSTANCE_NAME,
            body={
                'exportContext': {
                    'fileType': 'CSV',
                    'uri': f'{GCS_BUCKET}/data/{TABLES[0]}.csv',
                    'databases': [DATABASE_NAME], # Refers to the database from which to export
                    'csvExportOptions': {
                        'selectQuery': f"""SELECT 'date', 'datetime', 'cash_type', 'card', 'money', 'coffee_id'
                                           UNION ALL 
                                           SELECT date,datetime,cash_type,COALESCE(card,'N/A'),money,coffee_id 
                                           FROM {TABLES[0]} 
                                        """
                    }
                } # CloudSqlInstanceExportOperator exports data but doesn't automatically include column names as headers
            }     # use UNION ALL in SQL query to add the column names as the first row 
        )
    t2 = convert_csv_to_parquet(TABLES[0])

    t3 = CloudSqlInstanceExportOperator(   
            task_id = f'sql_export_{TABLES[1]}',
            instance = INSTANCE_NAME,
            body={
                'exportContext': {
                    'fileType': 'CSV',
                    'uri': f'{GCS_BUCKET}/data/{TABLES[1]}.csv',
                    'databases': [DATABASE_NAME], 
                    'csvExportOptions': {
                        'selectQuery': f"SELECT 'card' , 'customer_name' UNION ALL SELECT * FROM {TABLES[1]}"
                    }
                }
            }
        )
    t4 = convert_csv_to_parquet(TABLES[1])

    t5 = CloudSqlInstanceExportOperator(   
            task_id = f'sql_export_{TABLES[2]}',
            instance = INSTANCE_NAME,
            body={
                'exportContext': {
                    'fileType': 'CSV',
                    'uri': f'{GCS_BUCKET}/data/{TABLES[2]}.csv',
                    'databases': [DATABASE_NAME], 
                    'csvExportOptions': {
                        'selectQuery': f"SELECT 'coffee_name' , 'coffee_id' UNION ALL SELECT * FROM {TABLES[2]}"
                    }
                }
            }
        )
    t6 = convert_csv_to_parquet(TABLES[2])
    
    t7 = merge_data()
    t8 =  GCSToBigQueryOperator(
                                task_id='gcs_to_bq',
                                bucket='us-central1-composer9999-6cd15f2e-bucket',
                                source_objects=['data/final_sales_df.parquet'],
                                source_format='PARQUET',
                                destination_project_dataset_table='coffee_sales_dataset.sales_table01',
                                autodetect=True,
                                write_disposition='WRITE_TRUNCATE',
                                )

    # t3 = BashOperator(
    # task_id = "gcs_to_bq",
    # bash_command="bq load \
    #             --source_format=PARQUET \
    #             coffee_sales_dataset.sales_table01 \
    #             gs://us-central1-composer9999-6cd15f2e-bucket/data/final_sales_df.parquet",
    #                 )

    # bq load \
    # --source_format=CSV \
    # [DATASET].[TABLE_NAME] \
    # gs://[GCS_BUCKET]/data/[FILENAME].csv
    
    # Set task dependencies
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 
    
# Instantiate the DAG object
main_dag = coffee_sales_dag() 