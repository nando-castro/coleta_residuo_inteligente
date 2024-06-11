import time

from paho.mqtt import client as mqtt_client

from connectInterscity import addData_API  # Importe as funções necessárias
from connectInterscity import prepare_API

# Configurações de conexão ao Broker
broker = 'mqtt.eclipseprojects.io'
port = 1883
topic_subscribe = "esp8266/inputs/#"  # Subscreva a todos os tópicos sob 'esp8266/inputs/'
client_id = 'BROKER_PC_SUB'

# Realiza conexão ao Broker
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Conectado ao Broker MQTT")
        else:
            print("Falha ao conectar, código de retorno %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

# Realiza subscribe ao Tópico
def subscribe(client):
    def on_message(client, userdata, msg):
        print(f"Recebido o dado `{msg.payload.decode()}` do tópico `{msg.topic}`")

        # Adiciona os dados recebidos ao DataFrame
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        new_data = {'Datetime': [timestamp], 'Topic': [msg.topic], 'Payload': [msg.payload.decode()]}
        global df
        df = df.append(new_data, ignore_index=True)

        # Salva os dados no CSV
        df.to_csv(csv_path, index=False)

        # Envia os dados para a API Interscity
        data = {
            'timestamp': timestamp,
            'topic': msg.topic,
            'payload': msg.payload.decode()
        }
        addData_API(data)

    client.subscribe(topic_subscribe)
    client.on_message = on_message

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

if __name__ == '__main__':
    # Prepara a API antes de iniciar o loop MQTT
    uuid = prepare_API()
    if uuid:
        print("UUID do recurso criado:", uuid)
    else:
        print("Falha ao preparar a API. Verifique os logs acima para mais detalhes.")
    
    # Inicia o loop MQTT
    run()
