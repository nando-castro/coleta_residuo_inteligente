import json
import time
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
import requests

# Inicializa o DataFrame para armazenar os dados
data = {'Datetime': [], 'Topic': [], 'Payload': [], 'Capacidade': []}
df = pd.DataFrame(data)

# Fuso horário desejado ('America/Sao_Paulo')
desired_timezone = 'America/Sao_Paulo'
local_timezone = pytz.timezone(desired_timezone)

# Endereço para a API InterSCity
api = 'http://cidadesinteligentes.lsdi.ufma.br'

# Endereço do CSV que será salvo
csv_path = 'dados_mqtt.csv'

topicos_capacidades = {
    "esp8266/inputs/a": "vagaA",
    "esp8266/inputs/b": "vagaB",
    "esp8266/inputs/c": "vagaC",
    "esp8266/inputs/d": "vagaD",
    "esp8266/inputs/e": "vagaE"
}

def show_capacidades():
    r = requests.get(api + '/catalog/capabilities')
    if r.status_code == 200:
        content = json.loads(r.text)
        print(json.dumps(content, indent=2, sort_keys=True))
    else:
        print('Status code: ' + str(r.status_code))

def show_resources():
    r = requests.get(api + '/catalog/resources')
    if r.status_code == 200:
        content = json.loads(r.text)
        print(json.dumps(content, indent=2, sort_keys=True))
    else:
        print('Status code: ' + str(r.status_code))

def create_capability(nome, tipo, descricao):
    capability_json = {
        "name": nome,
        "description": descricao,
        "capability_type": tipo
    }
    r = requests.post(api + '/catalog/capabilities/', json=capability_json)
    if r.status_code == 201:
        content = json.loads(r.text)
        print(json.dumps(content, indent=2, sort_keys=True))
        return True
    else:
        print('Status code: ' + str(r.status_code))
        return False

def create_resource(descricao, latitude, longitude, capacidades):
    capacidades = capacidades.tolist()
    resource_json = {
        "data": {
            "description": descricao,
            "capabilities": capacidades,
            "status": "active",
            "city": "SLZ",
            "country": "BR",
            "state": "MA",
            "lat": latitude,
            "lon": longitude
        }
    }
    r = requests.post(api + '/catalog/resources', json=resource_json)
    uuid = ''
    if r.status_code == 201:
        resource = json.loads(r.text)
        uuid = resource['data']['uuid']
        print(json.dumps(resource, indent=2))
    else:
        print('Status code: ' + str(r.status_code))
    return uuid

def prepare_API():
    capacidades_list = np.array(df.Capacidade.tolist())
    capacidades_unique = np.unique(capacidades_list)
    print("Preparando a API...")
    time.sleep(1)
    print("Lista de tópicos das lixeiras:")
    print(capacidades_unique)
    time.sleep(1)
    capabilidade_criada = False
    for nomeCapacidade in capacidades_unique:
        print("=" * 20)
        print("Criando a capacidade para '" + nomeCapacidade + "' na API " + api + "...")
        time.sleep(1)
        capabilidade_criada = create_capability(nomeCapacidade, "sensor", "Estado da lixeira")
        time.sleep(1)
        if not capabilidade_criada:
            return ""
    print("Criando recurso 'Lixeiras_Centro' na API " + api + "...")
    time.sleep(1)
    uuid_resource = create_resource("Lixeiras_Centro", -2.55972052497871, -44.31196495361665, capacidades_unique)
    time.sleep(1)
    return uuid_resource

def addData_API(uuid_resource):
    df = pd.read_csv(csv_path)
    dates = df.Datetime.tolist()
    capacidades_ = df.Capacidade.tolist()
    payloads = df.Payload.tolist()
    capability_data_json = {
        "data": [{capacidade: value, 'timestamp': date} for capacidade, value, date in zip(capacidades_, payloads, dates)]
    }
    print("Exibindo dados das capacidades salvos no dataframe...")
    time.sleep(1)
    print(capability_data_json)
    time.sleep(1)
    print("Adicionando dados das capacidades ao recurso 'Lixeiras_Centro' da API " + api + "...")
    time.sleep(1)
    r = requests.post(api + '/adaptor/resources/' + uuid_resource + '/data/environment_monitoring', json=capability_data_json)
    if r.status_code == 201:
        print('OK!')
    else:
        print('Status code: ' + str(r.status_code))
        return False
    print("Exibindo dados do recurso 'Lixeiras_Centro'...")
    time.sleep(1)
    r = requests.post(api + '/collector/resources/' + uuid_resource + '/data')
    if r.status_code == 200:
        content = json.loads(r.text)
        print(json.dumps(content, indent=2, sort_keys=True))
    else:
        print('Status code: ' + str(r.status_code))
    return True

if __name__ == "__main__":
    uuid = prepare_API()
    if uuid:
        addData_API(uuid)
