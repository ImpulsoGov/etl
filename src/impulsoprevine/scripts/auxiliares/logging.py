import json
import requests
import os
import datetime
from dotenv import load_dotenv
load_dotenv()

MENSAGEM = ' *'+datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')+'* '

def atualiza_mensagem(texto):
    global MENSAGEM
    MENSAGEM = MENSAGEM+'\n'+texto
    return True

def envia_mensagem():
    global MENSAGEM
    slack_data = {'text': MENSAGEM}
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if os.getenv("IS_PROD")=='TRUE':
        response = requests.post(
            webhook_url, data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            raise ValueError(
                'Request to slack returned an error %s, the response is:\n%s'
                % (response.status_code, response.text)
        )
    return True

