import paho.mqtt.client as mqtt
import json
import requests

# Define your MQTT settings
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "disaster/data"

# Define your service endpoints
GRAPH_SERVICE_URL = "http://localhost:5001/insert"  # Later we'll build this
NLP_CLASSIFIER_URL = "http://localhost:5002/classify"  # Later we'll build this

# Callback when connected to broker
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("disaster/data")
    print("Subscribed to disaster/data topic!")  # ADD THIS


# Callback when message is received
def on_message(client, userdata, msg):
    try:
        print(f"Message arrived on topic {msg.topic}")
        payload = json.loads(msg.payload.decode())
        print(f"Received message: {payload}")

        # Example payload: {"type": "sensor", "data": {...}} or {"type": "text", "content": "flood in city"}

        if payload["type"] == "sensor":
            # Forward sensor data to Graph Service (to store)
            response = requests.post(GRAPH_SERVICE_URL, json=payload)
            print("Forwarded sensor data to Graph Service:", response.status_code)

        elif payload["type"] == "text":
            # First classify the text
            classify_response = requests.post(NLP_CLASSIFIER_URL, json={"text": payload["content"]})
            classification = classify_response.json().get("classification")

            print(f"Text classified as: {classification}")

            if classification == "disaster":
                # Forward to Graph Service for graph insertion
                response = requests.post(GRAPH_SERVICE_URL, json=payload)
                print("Forwarded disaster news to Graph Service:", response.status_code)

            else:
                print("Non-disaster news. Ignored.")

    except Exception as e:
        print(f"Error handling message: {e}")

# Setup MQTT Client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Loop forever
client.loop_forever()
