import os
import json
import pandas as pd
import requests
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

# 1. Configuration
load_dotenv()
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

# Configuration du Consumer Redpanda
conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'slack_notifier_demo_v2',  # <-- On change le nom du groupe
    'auto.offset.reset': 'latest'          # <-- CRUCIAL: 'latest' au lieu de 'earliest'
}
consumer = Consumer(conf)

# Le topic généré par Debezium suit la syntaxe : prefix.schema.table
TOPIC_NAME = 'poc_server.public.activites_sportives'
consumer.subscribe([TOPIC_NAME])

# 2. Chargement du référentiel RH en mémoire (pour croiser ID -> Nom)
# Dans un POC, on peut charger l'Excel en mémoire. En prod, on interrogerait une API ou une base.
print("Chargement du référentiel RH...")
df_rh = pd.read_excel("Données+RH.xlsx")
# Création d'un dictionnaire pour une recherche très rapide : {id: "Prénom Nom"}
salarie_dict = df_rh.set_index('ID salarié').apply(lambda row: f"{row['Prénom']} {row['Nom']}", axis=1).to_dict()

def send_slack_message(nom_complet, sport, distance_m, temps_s, commentaire):
    """Formate et envoie le message à Slack."""
    # Logique de formatage demandée par Juliette
    if distance_m and pd.notna(distance_m):
        distance_km = round(distance_m / 1000, 1)
        temps_min = round(temps_s / 60)
        texte = f"Bravo {nom_complet}! Tu viens de faire {distance_km} km en {temps_min} min de {sport} ! Quelle énergie !"
    else:
        texte = f"Bravo {nom_complet}! Tu as fait du {sport} ! Quelle énergie !"
        
    if commentaire and pd.notna(commentaire) and commentaire != "":
        texte += f"\n> \"{commentaire}\""

    # Envoi au Webhook (seulement si l'URL est configurée)
    if SLACK_WEBHOOK_URL and "hooks.slack.com" in SLACK_WEBHOOK_URL:
        payload = {"text": texte}
        try:
            response = requests.post(SLACK_WEBHOOK_URL, json=payload)
            if response.status_code == 200:
                print(f"Message Slack envoyé pour {nom_complet}")
            else:
                print(f"Erreur d'envoi Slack: {response.status_code}")
        except Exception as e:
            print(f"Erreur de connexion Slack: {e}")
    else:
        print(f"[SIMULATION SLACK] -> {texte}")

# 3. Boucle principale d'écoute
print(f"En attente de messages sur le topic '{TOPIC_NAME}'...")
try:
    while True:
        # Lit les messages (timeout de 1 seconde)
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Erreur du Consumer : {msg.error()}")
                break

        # 4. Traitement du message Debezium
        try:
            # Décoder la valeur JSON
            val = json.loads(msg.value().decode('utf-8'))
            
            # Debezium envoie "payload" contenant l'opération
            payload = val.get('payload', {})
            
            # On ne réagit qu'aux créations ('c')
            if payload.get('op') == 'c':
                after_data = payload.get('after', {})
                id_salarie = after_data.get('id_salarie')
                
                nom_complet = salarie_dict.get(id_salarie, f"Salarié Inconnu ({id_salarie})")
                
                send_slack_message(
                    nom_complet,
                    after_data.get('sport_type'),
                    after_data.get('distance_m'),
                    after_data.get('temps_ecoule_s'),
                    after_data.get('commentaire')
                )
        except Exception as e:
            print(f"Erreur lors du traitement d'un message : {e}")

except KeyboardInterrupt:
    print("Arrêt demandé par l'utilisateur.")
finally:
    # Ferme proprement la connexion
    consumer.close()
    print("Consumer arrêté.")