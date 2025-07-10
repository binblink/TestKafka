from confluent_kafka import Consumer
import psycopg2
import json
import time


# Config PG
DB_CONFIG = {
    "host": "postgres",
    "dbname": "messages_db",
    "user": "user",
    "password": "pass"
}

# Creation de la table si necessaire
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            message TEXT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


# Config de Kafka
consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'message-group',
    # Lecture des messages par ordre d'arrivee
    'auto.offset.reset': 'earliest'
})
# Ecoutes de tout les messages du topic nommes 'messages'
consumer.subscribe(['messages'])

while True:
    # Si message dispo, on le retourne sinon on bloque jusqu'au prochain message ou timeout
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Erreur:", msg.error())
        continue

    try:
        data = json.loads(msg.value().decode('utf-8'))
        message = data.get("message")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("INSERT INTO messages (message) VALUES (%s);", (message,))
        conn.commit()
        cur.close()
        conn.close()
        print(f"Message enregistre en BDD: {message}")
    except Exception as e:
        print("Echec du traitement du message:", e)

if __name__ == "__main__":
    consume()