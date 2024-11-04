import google.auth
from typing import Any
from google.auth.transport.requests import AuthorizedSession
import requests
import logging
import re
import google.cloud.logging
from cloudevents.http import CloudEvent
import functions_framework

client = google.cloud.logging.Client()
client.setup_logging()

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

dag_trigger_rules = [
    { 'dag': 'port_dag', 'regex': 'Daily_Port_Activity_Data_and_Trade_Estimates' }
]

@functions_framework.cloud_event
def trigger_dag(cloud_event:CloudEvent, context=None):

    #composer web url which can be found in the enviroment configuration section of composer enviroment
    web_server_url = ""

    
    object_name = cloud_event.data['name']
    logging.info('Triggering object Name: {}'.format(object_name))

    for rule in dag_trigger_rules:
        regex = rule['regex']
        if re.match(regex, object_name):
            dag_name = rule['dag']
            logging.info('Successfully triggered DAG: {}'.format(dag_name))
            endpoint = f"api/v1/dags/{dag_name}/dagRuns"
            url = f"{web_server_url}/{endpoint}"
            return make_composer2_web_server_request(url, method='POST', json={"conf": cloud_event.data})
            

def make_composer2_web_server_request(
    url: str, method: str = "GET", **kwargs: Any
) -> google.auth.transport.Response:

    authed_session = AuthorizedSession(CREDENTIALS)

    #Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    response = authed_session.request(method, url, **kwargs)
    logging.info(f"Response Status: {response.status_code}, Response Body: {response.text}")
    return response

