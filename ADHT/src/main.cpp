
#include <DHT.h>
#include <DHT_U.h>
#include <ESP8266WiFi.h>
#include <ThingsBoard.h>

#define SSID "AAAA"
#define NW_PWD "********"
#define TOKEN "********"
#define DHTPIN 2
#define DHTTYPE DHT11

char thingsboardServer[] = "localhost";
int status = WL_IDLE_STATUS;
unsigned long lastSend;
WiFiClient wifiClient;

DHT dht(DHTPIN, DHTTYPE);
ThingsBoard tb(wifiClient);


void sendData() {
    Serial.println("Reading...");

    float humidity = dht.readHumidity();
    float temperature = dht.readTemperature();

    if (isnan(temperature) || isnan(humidity)) {
        Serial.println("Error!");
        return;
    }

    Serial.print(humidity);
    Serial.print("\t");
    Serial.print(temperature);

    tb.sendTelemetryFloat("temperature", temperature);
    tb.sendTelemetryFloat("humidity", humidity);
}


void InitWiFi() {
    Serial.println("Connecting...");
    WiFi.begin(SSID, NW_PWD);
    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }
    Serial.println("Connected to WiFi");
}


void reconnect() {
    while (!tb.connected()) {
        status = WiFi.status();
        if ( status != WL_CONNECTED) {
            WiFi.begin(SSID, NW_PWD);
            while (WiFi.status() != WL_CONNECTED) {
                delay(500);
                Serial.print('.');
            }
            Serial.println("Reconnected to WiFi");
        }

        Serial.print("Connecting to ThingsBoard");
        if ( tb.connect(thingsboardServer, TOKEN) ) {
            Serial.println("CONNECTED");
        } else {
            Serial.print("ERROR");
            Serial.println( " : retrying in 5 seconds]" );
            delay(5000);
        }
    }
}


void setup() {
    Serial.begin(115200);
    dht.begin();
    InitWiFi();
    lastSend = 0;
}


void loop() {
    if (!tb.connected()) {
        reconnect();
    }

    if (millis() - lastSend > 1000) { // Update and send only after 1 seconds
        sendData();
        lastSend = millis();
    }

    tb.loop();
}