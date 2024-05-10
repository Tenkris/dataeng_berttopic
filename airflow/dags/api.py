from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json
import requests
from bs4 import BeautifulSoup

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Kafka broker address and topic
kafka_broker = 'localhost:9092'
topic = 'data'

# Function to send JSON data through Kafka
def send_data_to_kafka():
    # Instantiate KafkaProducer
    producer = KafkaProducer(bootstrap_servers=[kafka_broker])
    url  ='https://arxiv.org/abs/2405.05260'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    try:
        # Extract date
        dateline_div = soup.find('div', class_='dateline')
        date_string = dateline_div.text.replace("[Submitted on ", "").replace("]", "").replace("\n", "").strip()
        date = str(datetime.strptime(date_string, "%d %b %Y"))
    except ValueError:
        print(f"Could not parse date: {date_string}")
        date = None
    
    try:
        # Extract title
        title_tag = soup.find('h1', class_='title mathjax')
        title_text = title_tag.text.replace('Title:', '').strip()
    except AttributeError:
        print(f"Could not parse title: {title_tag}")
        title_text = None
    try:
        # Extract abstract
        abstract_tag = soup.find('blockquote', class_='abstract mathjax')
        abstract_text = abstract_tag.text.strip().replace('Abstract:', '').strip()
    except AttributeError:
        print(f"Could not parse abstract: {abstract_tag}")
        abstract_text = None

    try:
        # Extract authors
        authors_tag = soup.find('div', class_='authors')
        authors = authors_tag.text.strip().replace('Authors:', '').strip()
    except AttributeError:
        print(f"Could not parse authors: {authors_tag}")
        authors = None
    
    

    data = {
        'type': 'scraping',
        'date': date,
        'title': title_text,
        'abstract': abstract_text,
        'authors': authors,
    }
    
    # Send data as JSON to Kafka topic
    producer.send(topic, json.dumps(data).encode('utf-8'))
    producer.flush()  # Ensure all messages are sent
    producer.close()  # Close the producer

# Define the DAG object
dag = DAG(
    'example_dag_with_kafka',
    default_args=default_args,
    description='A DAG example with Kafka',
    schedule_interval=timedelta(days=1),
)

# Define the PythonOperator to execute the function
send_to_kafka_task = PythonOperator(
    task_id='send_to_kafka_task',
    python_callable=send_data_to_kafka,
    dag=dag,
)

# Set task dependencies
send_to_kafka_task  # No dependencies as it's the only task in this example
