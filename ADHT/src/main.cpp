#include <DHT.h>
#include <DHT_U.h>
#include <ESP8266WiFi.h>
#include <DNSServer.h>
#include <ESP8266WebServer.h>
#include <WiFiManager.h>
#include "ThingsBoard.h"

#define DHTPIN D3
#define DHTTYPE DHT11

char hostname[] = "hostname";
char token[] = "token";

WiFiClient wifiClient;
ThingsBoard tb(wifiClient);
DHT dht(DHTPIN, DHTTYPE);


void configModeCallback (WiFiManager *myWiFiManager) {
    Serial.println("Entered config mode");
    Serial.println(WiFi.softAPIP());
    Serial.println(myWiFiManager->getConfigPortalSSID());
}


void sendData() {
    float humidity = dht.readHumidity();
    float temperature = dht.readTemperature();

    if (isnan(temperature) || isnan(humidity)) {
        Serial.println("Error!");
        return;
    }

    Serial.print(humidity);
    Serial.print("\t");
    Serial.println(temperature);
    tb.sendTelemetryFloat("temperature", temperature);
    tb.sendTelemetryFloat("humidity", humidity);
}


void connect_tb() {
    Serial.println("Connecting to ThingsBoard");
    while (!tb.connected()) {
        if (tb.connect(hostname, token)) {
            Serial.println("[CONNECTED]");
        } else {
            Serial.print("[ERROR]");
            Serial.println( " : retrying in 3 second" );
            delay(3000);
        }
    }
}


void setup() {
    Serial.begin(115200);
    dht.begin();

    WiFiManager wifiManager;
    wifiManager.setAPCallback(configModeCallback);
    wifiManager.autoConnect();
    // wifiManager.resetSettings();

    if (WiFi.waitForConnectResult() == WL_CONNECTED) {
        Serial.println("Conectado!");
        connect_tb();
    }
}


void loop() {
    if (!tb.connected()) {
        connect_tb();
    }

    sendData();
    delay(1000);

    tb.loop();
}