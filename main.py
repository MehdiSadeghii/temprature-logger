import paho.mqtt.client as mqtt
import json
import csv


username = "sadeghi"
password = "pbreak1375"
broker = "185.2.14.188"
port = 1883
cnt = 0
_flag = False

dht_topic = "application/0c4fa261-3881-4b18-a43e-b11a255fa566/device/70b3d57ed00551a4/command/down"
dht_topic_status = "application/0c4fa261-3881-4b18-a43e-b11a255fa566/device/70b3d57ed00551a4/event/up"
thermostat_topic = "b47122e3-023a-4324-97e1-0bcef239de68/4L0k3/oShJt/bTOzr/status"


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


def on_message(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    handle_payload(msg)


def handle_payload(msg):
    global cnt, _flag, temp, hum
    data = json.loads(msg.payload.decode())
    if msg.topic == thermostat_topic:
        cnt += 1
        temp = data["temperature"]
        hum = data["humidity"]
        print("cnt: ", cnt)
        if cnt == 6:
            cnt = 0
            _flag = True
            client.publish(dht_topic, dhtRequest)
    if msg.topic == dht_topic_status and _flag == True:
        _flag = False
        dht_temp = data["object"]["Temperature"]["value"]
        dht_humidity = data["object"]["Humidity"]["value"]
        print(dht_temp,dht_humidity)
        with open('tempreture.csv', 'a') as file:
            writer = csv.writer(file)
            writer.writerow([temp, dht_temp, hum, dht_humidity])
        # print("cant decode payload")


def subscribe(client: mqtt):
    client.subscribe("#")
    client.on_message = on_message


def run():
    subscribe(client)
    # client.publish(dht_topic)
    client.loop_forever()


dhtRequest = {"confirmed": False, "fPort": 1, "data": "AgQ=", "devEui": "70b3d57ed00551a4"}
dhtRequest = json.dumps(dhtRequest)

with open('tempreture.csv', 'a') as file:
    writer = csv.writer(file)
    writer.writerow(["Thermostat temp", "DHT Temp", "Thermostat Hum", "DHT hum"])

client = connect_mqtt("DHT_Temp")
run()
