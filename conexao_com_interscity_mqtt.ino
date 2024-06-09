#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <NewPing.h>

#define pin_a 16 // D0
#define pin_b 5  // D1
#define pin_c 4  // D2
#define pin_d 0  // D3
#define pin_e 2  // D4
#define ledPin 14 // D5 (GPIO 14)
#define TRIGGER_PIN 12 // D6
#define ECHO_PIN 13 // D7
#define MAX_DISTANCE 400 // Distância máxima para o sensor ultrassônico

// Define os pinos para os LEDs de distância
#define ledRedPin 15 // D8 (GPIO 15)
#define ledYellowPin 3 // RX (GPIO 3)
#define ledGreenPin 1 // TX (GPIO 1)

#define TOPIC_PUBLISH "RECEBE_DADOS_ESP" // Tópico

char buf[30];
int val_a, val_b, val_c, val_d, val_e; // Declarando as variáveis val no escopo global.
int lastAState, lastBState, lastCState, lastDState, lastEState; // Variável para armazenar o estado anterior de cada pino.

const char *ssid = "Fernando";  // ssid
const char *password = "1234567890";  // Substitua pela sua senha
const char *mqtt_broker = "mqtt.eclipseprojects.io"; 
const int mqtt_port = 1883;  // Substitua pela porta correta do broker MQTT
const char *topic = "esp8266/inputs";
const char *led_topic = "esp8266/led"; // Tópico para controlar o LED
const char *ultrasonic_topic = "esp8266/ultrasonic"; // Tópico para enviar dados do ultrassônico

WiFiClient espClient;
PubSubClient client(espClient);
NewPing sonar(TRIGGER_PIN, ECHO_PIN, MAX_DISTANCE); // Inicializa o sensor ultrassônico

// Declaração da função getLeitura
void getLeitura(char pino, int valor);

void setup() {
    Serial.begin(9600);
    pinMode(pin_a, INPUT);
    pinMode(pin_b, INPUT);
    pinMode(pin_c, INPUT);
    pinMode(pin_d, INPUT);
    pinMode(pin_e, INPUT);
    pinMode(ledPin, OUTPUT); // Configura o pino do LED como saída
    
    // Configura os pinos dos LEDs de distância como saída
    pinMode(ledRedPin, OUTPUT);
    pinMode(ledYellowPin, OUTPUT);
    pinMode(ledGreenPin, OUTPUT);

    // Conecta-se à rede Wi-Fi
    WiFi.begin(ssid, password);
    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.println("Conectando ao WiFi...");
    }
    Serial.println("Conectado à rede Wi-Fi");

    // Configura o servidor MQTT
    client.setServer(mqtt_broker, mqtt_port);
    client.setCallback(callback);

    // Conecta-se ao servidor MQTT
    reconnect();
}

void loop() {
    // Mantém a conexão MQTT ativa
    if (!client.connected()) {
        reconnect();
    }

    // Realiza a leitura de cada pino
    val_a = digitalRead(pin_a);
    val_b = digitalRead(pin_b);
    val_c = digitalRead(pin_c);
    val_d = !digitalRead(pin_d);
    val_e = !digitalRead(pin_e);

    // Verifica mudanças em cada pino e publica no broker se necessário
    if (val_a != lastAState) {
        getLeitura('a', val_a);
        lastAState = val_a;
    }

    if (val_b != lastBState) {
        getLeitura('b', val_b);
        lastBState = val_b;
    }

    if (val_c != lastCState) {
        getLeitura('c', val_c);
        lastCState = val_c;
    }

    if (val_d != lastDState) {
        getLeitura('d', val_d);
        lastDState = val_d;
    }

    if (val_e != lastEState) {
        getLeitura('e', val_e);
        lastEState = val_e;
    }

    // Realiza a leitura do sensor ultrassônico
    unsigned int distance = sonar.ping_cm();
    if (distance != 0) {
        dtostrf(distance, 6, 2, buf);
        client.publish(ultrasonic_topic, buf);
        Serial.print("Distância: ");
        Serial.print(buf);
        Serial.println(" cm");

        // Controle dos LEDs de distância
        if (distance < 10) { // Muito próximo
            digitalWrite(ledRedPin, HIGH);
            digitalWrite(ledYellowPin, LOW);
            digitalWrite(ledGreenPin, LOW);
        } else if (distance < 30) { // Distância mediana
            digitalWrite(ledRedPin, LOW);
            digitalWrite(ledYellowPin, HIGH);
            digitalWrite(ledGreenPin, LOW);
        } else { // Distante
            digitalWrite(ledRedPin, LOW);
            digitalWrite(ledYellowPin, LOW);
            digitalWrite(ledGreenPin, HIGH);
        }
    } else {
        // Apaga todos os LEDs se não houver leitura válida
        digitalWrite(ledRedPin, LOW);
        digitalWrite(ledYellowPin, LOW);
        digitalWrite(ledGreenPin, LOW);
    }

    // Aguarda antes de enviar novas leituras
    delay(1000);
  
    // Mantém a conexão MQTT ativa
    client.loop();
}

// Função que realiza a leitura do sensor e enviar para o tópico Publisher
void getLeitura(char pino, int valor) {
    dtostrf(valor, 6, 2, buf);
    String topic_pino = "esp8266/inputs/" + String(pino);
    client.publish(topic_pino.c_str(), buf);
    Serial.print("Pino ");
    Serial.print(pino);
    Serial.print(": ");
    Serial.println(buf);
    Serial.println("Payload enviado!");
}

// Função de callback para tratar mensagens recebidas
void callback(char *topic, byte *payload, unsigned int length) {
    Serial.print("Mensagem no tópico: ");
    Serial.println(topic);
    Serial.print("Conteúdo da mensagem: ");
    for (int i = 0; i < length; i++) {
        Serial.print((char) payload[i]);
    }
    Serial.println();
    Serial.println("-----------------------");

    // Trata mensagens do tópico do LED
    if (strcmp(topic, led_topic) == 0) {
        if ((char)payload[0] == '1') {
            digitalWrite(ledPin, HIGH); // Acende o LED
            Serial.println("LED aceso");
        } else if ((char)payload[0] == '0') {
            digitalWrite(ledPin, LOW); // Apaga o LED
            Serial.println("LED apagado");
        }
    }
}

void reconnect() {
    while (!client.connected()) {
        String client_id = "esp8266-client-";
        client_id += String(WiFi.macAddress());
        Serial.printf("Conectando o cliente %s ao broker MQTT\n", client_id.c_str());
        if (client.connect(client_id.c_str())) {
            Serial.println("Conectado ao broker MQTT!");
            client.subscribe(topic);
            client.subscribe(led_topic); // Inscreve-se no tópico do LED
        } else {
            Serial.print("Falha na conexão: ");
            Serial.print(client.state());
            delay(2000);
        }
    }
}
