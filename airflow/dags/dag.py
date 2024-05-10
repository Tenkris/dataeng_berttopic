# import required libraries
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from kafka import KafkaProducer
import json

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

category_dict = {
    "astro-ph": "Astrophysics",
    "cond-mat": "Condensed Matter",
    "cs": "Computer Science",
    "econ": "Economics",
    "eess": "Electrical Engineering and Systems Science",
    "gr-qc": "General Relativity and Quantum Cosmology",
    "hep-ex": "High Energy Physics - Experiment",
    "hep-lat": "High Energy Physics - Lattice",
    "hep-ph": "High Energy Physics - Phenomenology",
    "hep-th": "High Energy Physics - Theory",
    "math": "Mathematics",
    "math-ph": "Mathematical Physics",
    "nlin": "Nonlinear Sciences",
    "nucl-ex": "Nuclear Experiment",
    "nucl-th": "Nuclear Theory",
    "physics": "Physics",
    "q-bio": "Quantitative Biology",
    "q-fin": "Quantitative Finance",
    "quant-ph": "Quantum Physics",
    "stat": "Statistics"
}
category_arr = list(category_dict.keys()) # get all the keys from the dictionary

def create_link(category):
    ans =  [] 
    url = "https://arxiv.org/list/" + category + "/new"
    response = requests.get(url) 
    soup = BeautifulSoup(response.text, 'html.parser')
    link_arr = soup.find_all('a', title="Abstract")
    for link in link_arr:
        link = "https://arxiv.org" + link.get('href')
        ans.append(link)
    return ans

def get_data(link): # get the date, title and abstract from the link 
    response = requests.get(link)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract date
    dateline_div = soup.find('div', class_='dateline')
    date_string = dateline_div.text.replace("[Submitted on ", "").replace("]", "").replace("\n", "").strip()
    try:
        date = str(datetime.strptime(date_string, "%d %b %Y"))
    except ValueError:
        print(f"Could not parse date: {date_string}")
        date = None
    
    
    # Extract title
    title_tag = soup.find('h1', class_='title mathjax')
    title_text = title_tag.text.replace('Title:', '').strip()
    
    # Extract abstract
    abstract_tag = soup.find('blockquote', class_='abstract mathjax')
    abstract_text = abstract_tag.text.strip().replace('Abstract:', '').strip()

    # Extract authors 
    authors_div = soup.find('div', class_='authors')
    authors = [a.text for a in authors_div.find_all('a')]

    
    return date, title_text, abstract_text, authors
def main_webscraping(): 
    # Connect to kafka broker running in your local host (docker). Change this to your kafka broker if needed
    kafka_broker = 'localhost:9092'
    topic = 'data'
    producer = KafkaProducer(bootstrap_servers=[kafka_broker])
    link_arr = []
    ans = []
    for catergory in category_arr:
        link_arr += create_link(catergory) 
    for link in link_arr:
        date, title, abstract, authors = get_data(link)
        json_data = {
            "date": date,
            "title": title,
            "abstract": abstract,
            "authors": authors
        }
        ans.append(json_data)

        # Serialize the JSON data to bytes
        value_bytes = json.dumps(json_data).encode('utf-8')
        print(value_bytes)

        # Send the serialized data to Kafka
        producer.send(topic, value=value_bytes)
        
    return ans


# Define the default arguments for the DAG

default_args = {
    'type' : 'scraping',
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
}

# Define the DAG

dag = DAG(
    'webscraping',
    default_args=default_args,
    description='A DAG to scrape the arXiv website',
    schedule_interval='@daily',
)

# Define the tasks

start = DummyOperator(task_id='start', dag=dag)

webscraping = PythonOperator(
    task_id='webscraping',
    python_callable=main_webscraping,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Define the task dependencies

start >> webscraping >> end