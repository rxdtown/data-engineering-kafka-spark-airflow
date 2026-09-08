import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

produits = ["ordinateur", "telephone", "casque", "clavier", "souris"]
villes = ["Fes", "Rabat", "Casablanca", "Marrakech"]

def generer_commande():
    return {
        "id_commande": random.randint(100000, 999999),
        "produit": random.choice(produits),
        "montant": round(random.uniform(50, 2000), 2),
        "ville": random.choice(villes),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    while True:
        commande = generer_commande()
        producer.send("commandes", value=commande)
        print(f"Envoyé : {commande}")
        time.sleep(1)