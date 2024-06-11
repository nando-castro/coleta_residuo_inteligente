import json
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
import requests

# Endereço para a API InterSCity
api = 'http://cidadesinteligentes.lsdi.ufma.br'

# Endereço do CSV que será salvo no Google Drive
# csv_path = '/content/drive/My Drive/dados_mqtt.csv'

# Substitua 'YOUR_RESOURCE_UUID' pelo UUID do recurso criado na API InterSCity
resource_uuid = 'c9c9c48d-e482-49cc-8c4b-ef83d7916052'  # <- Substitua aqui

# Inicializa o DataFrame para armazenar os dados
data = {'Datetime': [], 'Topic': [], 'Payload': [], 'Capacidade': []}
df = pd.DataFrame(data)

# Fuso horário desejado ('America/Sao_Paulo')
desired_timezone = 'America/Sao_Paulo'

# Cria um objeto de fuso horário
local_timezone = pytz.timezone(desired_timezone)

# Caminho do arquivo CSV
csv_path = 'dados_mqtt.csv'

# Inicializa o DataFrame para armazenar os dados se o arquivo não existir
if not os.path.exists(csv_path):
    data = {'Datetime': [], 'Topic': [], 'Payload': [], 'Capacidade': []}
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)


# Função para criar uma capacidade na API
def create_capability(nome, tipo, descricao):
    capability_json = {
        "name": nome,
        "description": descricao,
        "capability_type": tipo
    }

    # Faz uma request para a API e salva a capacidade
    r = requests.post(api + '/catalog/capabilities/', json=capability_json)

    # Retorna se a capacidade foi "postada" com sucesso ou não
    if r.status_code == 201:
        content = json.loads(r.text)
        print(json.dumps(content, indent=2, sort_keys=True))
        return True
    else:
        print('Status code: ' + str(r.status_code))
        return False

def show_capacidades():
    r = requests.get(api + '/catalog/capabilities')

    # Retorno da API
    if r.status_code == 200:
        content = json.loads(r.text)
        print(json.dumps(content, indent=2, sort_keys=True))
    else:
        print('Status code: ' + str(r.status_code))

def show_resources():
    r = requests.get(api + '/catalog/resources')

    # Retorno da API
    if r.status_code == 200:
        content = json.loads(r.text)
        print(json.dumps(content, indent=2, sort_keys=True))
    else:
        print('Status code: ' + str(r.status_code))

def create_resource(descricao, latitude, longitude, capacidades):
    # Converte capacidades para listas se forem numpy arrays
    capacidades = capacidades.tolist() if isinstance(capacidades, np.ndarray) else capacidades
    
    # Cria o recurso da lixeira inteligente
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

    # "Post" do recurso na API
    r = requests.post(api + '/catalog/resources', json=resource_json)

    # Retorna sucesso ou fracasso da "postagem" do recurso de lixeira, também salva seu UUID.
    uuid = ''
    if r.status_code == 201:
        resource = json.loads(r.text)
        uuid = resource['data']['uuid']
        print(json.dumps(resource, indent=2))
    else:
        print('Status code: ' + str(r.status_code))
    return uuid

def prepare_API():
    # Lista de tópicos únicos do DATAFRAME
    capacidades_list = np.array(df.Capacidade.tolist())
    capacidades_unique = np.unique(capacidades_list)

    print("Preparando a API...")
    time.sleep(1)
    print("Lista de tópicos da lixeira inteligente:")
    print(capacidades_unique)
    time.sleep(1)

    # Armazena se a capacidade foi criada ou não
    capabilidade_criada = False

    # Criação das capacidades na API
    for nomeCapacidade in capacidades_unique:
        print("=" * 20)
        print("Criando a capacidade para '" + nomeCapacidade + "' na API " + api + "...")
        time.sleep(1)
        capabilidade_criada = create_capability(nomeCapacidade, "sensor", "Nível de preenchimento da lixeira")
        time.sleep(1)
        if capabilidade_criada == False:
            return ""

    # Criação do recurso na API
    print("Criando recurso 'Lixeira_Inteligente_A' na API " + api + "...")
    time.sleep(1)
    uuid_resource = create_resource("Lixeira_Inteligente_A", -2.55972052497871, -44.31196495361665, capacidades_unique)
    time.sleep(1)
    return uuid_resource

def addData_API(uuid_resource):
    # Lê o CSV do Google Drive
    df = pd.read_csv(csv_path)

    # Salva as colunas do CSV em listas
    dates = df.Datetime.tolist()
    capacidades_ = df.Capacidade.tolist()
    payloads = df.Payload.tolist()

    # Converte os dados das capacidades em JSON
    capability_data_json = {
        "data": [{capacidade: value, 'timestamp': date} for capacidade, value, date in zip(capacidades_, payloads, dates)]
    }

    print("Exibindo dados das capacidades salvos no dataframe...")
    time.sleep(1)
    print(capability_data_json)
    time.sleep(1)

    print("Adicionando dados das capacidades ao recurso 'Lixeira_Inteligente_A' da API " + api + "...")
    time.sleep(1)
    # Adiciona dados das 'capabilities' ao 'resource'
    r = requests.post(api + '/adaptor/resources/' + uuid_resource + '/data/environment_monitoring', json=capability_data_json)
    if r.status_code == 201:
        print('OK!')
    else:
        print('Status code: ' + str(r.status_code))
        return False

    print("Exibindo dados do recurso 'Lixeira_Inteligente_A'...")
    time.sleep(1)
    # Exibe dados do 'resource'
    r = requests.get(api + '/collector/resources/' + uuid_resource + '/data')
    if r.status_code == 200:
        content = json.loads(r.text)
        print(json.dumps(content, indent=2, sort_keys=True))
    else:
        print('Status code: ' + str(r.status_code))

    return True

# Exemplo de como você pode chamar essas funções
if __name__ == '__main__':
    uuid_resource = prepare_API()
    if uuid_resource:
        success = addData_API(uuid_resource)
        if success:
            print("Dados adicionados com sucesso à API InterSCity.")
        else:
            print("Falha ao adicionar dados à API InterSCity.")
    else:
        print("Falha ao preparar a API.")
