import time
import paho.mqtt.client as mqtt
import json
import threading as th
import csv
import pandas as pd

username = "sadeghi"
password = "pbreak1375"
broker = "185.2.14.188"
port = 1883
cnt = 0

dht_topic = "application/0c4fa261-3881-4b18-a43e-b11a255fa566/device/70b3d57ed00551a4/command/down"
dht_topic_status = "application/0c4fa261-3881-4b18-a43e-b11a255fa566/device/70b3d57ed00551a4/event/up"
thermostat_topic = "b47122e3-023a-4324-97e1-0bcef239de68/4L0k3/oShJt/bTOzr/status"
global dhtRequest
global flag, temp, hum


def connect_mqtt(id):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt.Client(id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        data = json.loads(msg.payload.decode())
        if msg.topic == thermostat_topic:
            temp = data["temperature"]
            hum = data["humidity"]
            flag = True
            print("DHT Request Format:", dhtRequest)
            client.publish(dht_topic, dhtRequest)

        if msg.topic == dht_topic_status and flag:
            flag = False
            dht_temp = data["object"]["Temperature"]["value"]
            dht_humidity = data["object"]["Humidity"]["value"]
            writer.writerow([temp, dht_temp, hum, dht_humidity])
            print("cant decode payload")

    client.subscribe("#")
    client.on_message = on_message


def run():
    subscribe(client)
    client.publish(dht_topic)
    client.loop_forever()


def sctn():
    print("timer")
    _timer = th.Timer(10, sctn)
    _timer.start()


def tempreture_logger(recieved):
    if recieved == "thermostat":
        msg = {"confirmed": False, "fPort": 1, "data": "AgQ=", "devEui": "70b3d57ed00551a4"}
        msg = json.dumps(msg)
        client.publish(dht_topic, msg)


if __name__ == '__main__':
    dhtRequest = {"confirmed": False, "fPort": 1, "data": "AgQ=", "devEui": "70b3d57ed00551a4"}
    dhtRequest = json.dumps(dhtRequest)
    print(dhtRequest)
    with open('tempreture.csv', 'w+') as file:
        writer = csv.writer(file)
        writer.writerow(["Thermostat temp", "DHT Temp", "", "Thermostat Hum", "DHT hum"])
    _timer = th.Timer(10, sctn)
    client = connect_mqtt("DHT_Temp")
    _timer.start()
    run()
