import requests
import re
import google.cloud.logging
from google.cloud import secretmanager
import logging 
from cloudevents.http import CloudEvent
import functions_framework

client = google.cloud.logging.Client()
client.setup_logging()

dag_trigger_rules = [
    { 'dag': 'port_dag', 'regex': 'Daily_Port_Activity_Data_and_Trade_Estimates' }
]

@functions_framework.cloud_event
def trigger_dag(cloud_event:CloudEvent, context=None):
    
    
    object_name = cloud_event.data['name']
    logging.info('Triggering object Name: {}'.format(object_name))

    for rule in dag_trigger_rules:
        regex = rule['regex']
        if re.match(regex, object_name):
            dag_name = rule['dag']
            logging.info('Successfully triggered DAG: {}'.format(dag_name))
            response = make_airflow_request(dag_name, cloud_event.data)
            
            
            break

def make_airflow_request(dag_name: str, cloud_event: dict) -> requests.Response:
    secret_client = secretmanager.SecretManagerServiceClient()
    project_id = 'your-project-id'
    secret_id = ''

    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    response = secret_client.access_secret_version(name=name)

    secret_value = response.payload.data.decode('UTF-8')
    
    username = ""
    password = secret_value

    #Airflow REST API endpoint for triggering DAG runs
    endpoint = f"api/v1/dags/{dag_name}/dagRuns"
    url = f"http://ip-address:8080/{endpoint}"  #Replace with your Airflow server URL and port

    
    #Create a session with basic authentication
    session = requests.Session()
    session.auth = (username, password)


    return session.post(url,json={"conf": cloud_event})

#add requests, google-cloud-logging to requirements.txt
#set the entry point to 'trigger_dag'