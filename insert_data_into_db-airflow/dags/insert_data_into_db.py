from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import mysql.connector
import random
import os

class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")

def insert_transaction_into_db():
    connection = mysql.connector.connect(
        host= Config.MYSQL_HOST ,
        port =  Config.MYSQL_PORT ,
        user= Config.MYSQL_USER ,
        password= Config.MYSQL_PASSWORD,
        database = Config.MYSQL_DB
    )
    cursor = connection.cursor()

    insert_query = "INSERT INTO transaction (date, datetime, cash_type, card, money, coffee_id) VALUES (%s, %s, %s, %s, %s, %s)"
    
    now = datetime.now()
    cash_type = random.choice(['cash', 'card'])
    card_number = f"NO-{random.randint(1, 552):04d}" if cash_type == 'card' else None
    values = (
        now.strftime('%Y-%m-%d'),  # Convert date to string format
        now,                       # Current timestamp
        cash_type,                      
        card_number,                    
        round(random.uniform(20, 50), 2),  
        random.randint(0, 7)           
    )
    
    cursor.execute(insert_query, values)
    connection.commit()

    cursor.close()
    connection.close()
    
    print(f"Transaction inserted at {now}")
    
def print_word():
    print('Successfully inserted data into the database !')
    
# default arguments 
default_args = {
    'owner': 'user007',
    'depends_on_past': False,
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 9, 7),
}

# DAG
dag = DAG(
    'insert_transaction_dag',
    default_args=default_args,
    description='A DAG to insert a transaction row into Cloud SQL',
    schedule_interval=timedelta(hours=1 , minutes=0),
    max_active_runs=1, # Ensure only 1 run is active at a time
    catchup=False, # Disable backfilling
  
)

# Tasks
t1 = PythonOperator(
    task_id='insert_transaction',
    python_callable=insert_transaction_into_db,
    dag=dag,
)

t2 = PythonOperator(
    task_id='print_Successfully',
    python_callable=print_word,
    dag=dag,
)

# Dependencies
t1 >> t2