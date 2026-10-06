import json
import logging
import os
import random
import time
from azure.eventhub import EventData, EventHubProducerClient
from dotenv import load_dotenv
from pymongo import MongoClient

# --- CHARGEMENT DES SECRETS (.env) ---
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

# --- CONFIGURATION DU LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

# --- INITIALISATION DES SERVICES ---
logging.info("Initialisation de la connexion MongoDB...")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["streamvault_db"]

logging.info("Initialisation du client Azure Event Hub...")
producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
)

# Cache local pour garantir un prix stable par titre
price_cache = {}


def get_stable_price(title: str) -> tuple[float, bool]:
  """Vérifie si le prix existe déjà en cache.

  Retourne le prix et un booléen (True si trouvé en cache, False sinon).
  """
  if title in price_cache:
    return price_cache[title], True  # Le prix vient du cache (stable)
  else:
    # Premier calcul et stockage dans le cache
    price_cache[title] = round(float(5 + (abs(hash(title)) % 16)), 2)
    return price_cache[title], False  # Nouveau prix généré


def main():
  logging.info(" Démarrage du producteur de commandes en temps réel...")
  logging.info("Appuie sur Ctrl+C pour interrompre le script proprement.")

  try:
    while True:
      # 1. Tirer un VRAI client et un VRAI média depuis MongoDB
      random_client = list(db.clients.aggregate([{"$sample": {"size": 1}}]))[
          0
      ]
      random_item = list(
          db.catalogue_mixte.aggregate([{"$sample": {"size": 1}}])
      )[0]

      prenom = random_client.get("prenom", "Inconnu")
      nom = random_client.get("nom", "")
      client_name = f"{prenom} {nom}".strip()
      client_email = f"{prenom.lower()}.{nom.lower()}@streamvault.com"

      item_title = random_item.get(
          "title", random_item.get("_id", "Titre inconnu")
      )

      # Récupération du prix et du statut du cache
      item_price, is_cached = get_stable_price(item_title)

      # Indicateur visuel pour les logs du terminal
      cache_indicator = "🔄 [CACHE]" if is_cached else "✨ [NOUVEAU]"

      # 2. Construction de la commande
      order = {
          "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
          "client": {"email": client_email, "nom": client_name},
          "item": {"titre": item_title, "prix": item_price},
          "quantite": random.randint(1, 3),
      }

      # 3. Envoi dans l'Event Hub
      event_data_batch = producer.create_batch()
      event_data_batch.add(EventData(json.dumps(order)))
      producer.send_batch(event_data_batch)

      # 4. Affichage avec l'indicateur de cache dans les logs
      logging.info(
          f"📤 Commande envoyée {cache_indicator} : {client_name}"
          f" ({client_email}) a acheté '{item_title}' pour {item_price} €"
      )

      time.sleep(3)

  except KeyboardInterrupt:
    logging.warning(" Arrêt du producteur demandé par l'utilisateur.")
  except Exception as e:
    logging.error(f" Une erreur critique est survenue : {e}", exc_info=True)
  finally:
    producer.close()
    logging.info("🔌 Connexion Event Hub fermée proprement. Fin du script.")


if __name__ == "__main__":
  main()