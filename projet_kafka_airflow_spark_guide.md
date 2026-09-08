# Projet Data Engineering complet — Kafka + Spark + Airflow + SQL (100% local, Docker)

**Objectif du projet :** simuler le système de traitement de commandes d'un site e-commerce fictif, en combinant une chaîne **temps réel** (Kafka → Spark Structured Streaming) et une chaîne **batch orchestrée** (Airflow → PySpark → PostgreSQL), le tout exécuté localement via Docker.

C'est un projet volontairement complet car il couvre les deux grandes familles de Data Engineering (streaming et batch) et les technologies les plus demandées en entretien : **Kafka, Spark (Structured Streaming + batch), Airflow, SQL, Docker**.

---

## Sommaire

1. [Architecture générale](#1-architecture-générale)
2. [Prérequis](#2-prérequis)
3. [Arborescence du projet](#3-arborescence-du-projet)
4. [Le fichier docker-compose.yml expliqué service par service](#4-le-fichier-docker-composeyml-expliqué-service-par-service)
5. [Étape 1 — Démarrer l'infrastructure](#5-étape-1--démarrer-linfrastructure)
6. [Étape 2 — Kafka : créer le topic et comprendre les commandes](#6-étape-2--kafka--créer-le-topic-et-comprendre-les-commandes)
7. [Étape 3 — Le Producer Kafka (générateur d'événements)](#7-étape-3--le-producer-kafka-générateur-dévénements)
8. [Étape 4 — Spark Structured Streaming : Kafka → Bronze](#8-étape-4--spark-structured-streaming--kafka--bronze)
9. [Étape 5 — Base PostgreSQL : création du schéma](#9-étape-5--base-postgresql--création-du-schéma)
10. [Étape 6 — Scripts PySpark batch : Silver et Gold](#10-étape-6--scripts-pyspark-batch--silver-et-gold)
11. [Étape 7 — Airflow : le DAG d'orchestration expliqué tâche par tâche](#11-étape-7--airflow--le-dag-dorchestration-expliqué-tâche-par-tâche)
12. [Étape 8 — Lancer et suivre le pipeline complet](#12-étape-8--lancer-et-suivre-le-pipeline-complet)
13. [Étape 9 — Requêtes SQL finales et vérification](#13-étape-9--requêtes-sql-finales-et-vérification)
14. [Commandes Docker utiles (récapitulatif)](#14-commandes-docker-utiles-récapitulatif)
15. [Résumé du projet pour l'entretien](#15-résumé-du-projet-pour-lentretien)

---

## 1. Architecture générale

```
┌─────────────┐     ┌───────────────┐     ┌──────────────────────┐
│  Producer   │────▶│  Kafka Topic  │────▶│  Spark Structured     │
│  (Python)   │     │  "commandes"  │     │  Streaming            │
│  génère des │     │  (+Zookeeper) │     │  (consumer temps réel)│
│  commandes  │     └───────────────┘     └───────────┬───────────┘
└─────────────┘                                       │ écrit en continu
                                                        ▼
                                          ┌─────────────────────────┐
                                          │  BRONZE (Parquet local)  │
                                          │  données brutes horodat.│
                                          └───────────┬─────────────┘
                                                       │
                                    Chaque nuit, Airflow déclenche :
                                                       ▼
                                          ┌─────────────────────────┐
                                          │  PySpark batch : SILVER │
                                          │  nettoyage, dédup, typage│
                                          └───────────┬─────────────┘
                                                       ▼
                                          ┌─────────────────────────┐
                                          │  PySpark batch : GOLD   │
                                          │  agrégats métier        │
                                          └───────────┬─────────────┘
                                                       ▼
                                          ┌─────────────────────────┐
                                          │  PostgreSQL              │
                                          │  table analytique finale │
                                          └─────────────────────────┘
```

**Pourquoi cette architecture ?** Elle reproduit un cas réel : les événements (commandes) arrivent en continu et doivent être capturés sans perte (Kafka + Streaming), mais les transformations lourdes et les agrégats métier n'ont pas besoin d'être recalculés en permanence — un traitement batch orchestré chaque nuit (Airflow + PySpark) suffit et coûte moins cher en ressources. C'est le pattern **Lambda/Kappa simplifié** qu'on retrouve dans beaucoup d'entreprises.

---

## 2. Prérequis

- **Docker Desktop** installé et démarré (inclut Docker Compose).
- Au moins 8 Go de RAM disponibles pour Docker (Kafka + Spark + Airflow + Postgres en simultané).
- Un éditeur de code (VS Code recommandé).

**À quoi ça sert Docker ici :** plutôt que d'installer Kafka, Spark, Airflow et PostgreSQL séparément sur ta machine (source classique de conflits de versions), chaque outil tourne dans son propre conteneur isolé, démarré et arrêté d'un seul coup avec Docker Compose — exactement comme dans une vraie stack de production.

---

## 3. Arborescence du projet

```
projet-de-a-z/
├── docker-compose.yml
├── producer/
│   └── kafka_producer.py
├── spark_jobs/
│   ├── streaming_kafka_to_bronze.py
│   ├── batch_bronze_to_silver.py
│   └── batch_silver_to_gold.py
├── sql/
│   └── init_postgres.sql
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── pipeline_commandes_dag.py
└── data/
    ├── bronze/
    ├── silver/
    └── gold/
```

**À quoi ça sert :** séparer clairement chaque responsabilité (producteur, jobs Spark, SQL, orchestration) rend le projet lisible et facile à présenter en entretien — chaque dossier correspond à une brique technique.

---

## 4. Le fichier docker-compose.yml expliqué service par service

```yaml
version: "3.8"

services:

  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "29092:29092"

  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - ./sql/init_postgres.sql:/docker-entrypoint-initdb.d/init.sql

  spark:
    image: bitnamilegacy/spark:3.5
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"
      - "7077:7077"
    volumes:
      - ./spark_jobs:/opt/spark_jobs
      - ./data:/opt/data

  spark-worker:
    image: bitnamilegacy/spark:3.5
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=2
    depends_on:
      - spark
    volumes:
      - ./spark_jobs:/opt/spark_jobs
      - ./data:/opt/data

  airflow:
    build: ./airflow
    user: "0:0"
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./spark_jobs:/opt/spark_jobs
      - ./data:/opt/data
    ports:
      - "8081:8080"
    command: >
      bash -c "airflow db migrate &&
               airflow users create --username admin --password admin
               --firstname A --lastname D --role Admin --email admin@example.com || true &&
               (airflow scheduler &) &&
               airflow webserver"
```

**Explication service par service :**

> **Note (mi-2025) :** Bitnami a retiré ses images versionnées gratuites (`bitnami/spark:3.5`) du Docker Hub public suite à un changement de politique de Broadcom. On utilise donc `bitnamilegacy/spark:3.5` — la version figée (non maintenue, mais toujours gratuite et fonctionnelle) que Bitnami a laissée disponible, avec les mêmes variables d'environnement. Pour un vrai projet long terme, l'alternative recommandée serait l'image officielle `apache/spark`.

| Service | Rôle |
|---|---|
| `zookeeper` | Coordonne les métadonnées du cluster Kafka (broker, partitions) — obligatoire pour les versions de Kafka utilisées ici |
| `kafka` | Le broker de messages : reçoit les événements du producer et les met à disposition des consommateurs (ici Spark) |
| `postgres` | Sert deux rôles : base de métadonnées d'Airflow **et** base analytique finale (table Gold) — en production on séparerait ces deux usages |
| `spark` (master) | Coordonne l'exécution distribuée des jobs Spark (streaming et batch) |
| `spark-worker` | Exécute réellement les tâches Spark envoyées par le master |
| `airflow` | Orchestrateur : héberge le webserver (interface) et le scheduler (déclenchement des DAGs) |

**Détail des éléments techniques clés :**
| Élément | Rôle |
|---|---|
| `depends_on` | Force Docker à démarrer les services dans le bon ordre (ex. Kafka après Zookeeper) |
| `ports: "29092:29092"` | Expose le port du conteneur (29092) sur la machine hôte, pour que des outils externes puissent s'y connecter |
| `volumes` | Monte un dossier local dans le conteneur, pour que les fichiers (scripts Spark, DAGs, données) soient partagés entre ta machine et les conteneurs sans les copier |
| `environment` | Définit des variables de configuration lues par l'image au démarrage (identifiants, mode du service, adresses de connexion) |
| `build: ./airflow` | Construit une image personnalisée à partir du `Dockerfile` de ce dossier au lieu d'utiliser l'image officielle telle quelle — nécessaire ici pour ajouter Java et PySpark, absents de l'image de base |
| `user: "0:0"` | Force le conteneur à s'exécuter en tant que root (UID/GID 0) au lieu de l'utilisateur `airflow` par défaut. Nécessaire ici car le dossier `./data` est partagé entre plusieurs conteneurs (Spark, Airflow) qui écrivent avec des utilisateurs différents ; sans cela, Airflow n'a pas toujours le droit de supprimer ou modifier des fichiers créés par un autre conteneur sur le volume partagé. Acceptable pour un projet local de démonstration ; en production, on harmoniserait plutôt les UID entre conteneurs ou on utiliserait des volumes dédiés par service |

> Le service `airflow` utilise maintenant `build: ./airflow` au lieu de `image: apache/airflow:2.9.0`, car l'image officielle ne contient ni Java ni PySpark. Voir le `Dockerfile` dédié plus bas dans ce guide (section 4 bis).

---

## 4 bis. Le Dockerfile personnalisé pour Airflow

`airflow/Dockerfile` :
```dockerfile
FROM apache/airflow:2.9.0
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        procps \
        openjdk-17-jre-headless \
    && ln -sf $(readlink -f /usr/bin/java | sed "s:bin/java::") /usr/lib/jvm/default-java \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.0
```

**Explication ligne par ligne :**
| Ligne | Rôle |
|---|---|
| `FROM apache/airflow:2.9.0` | Part de l'image officielle Airflow comme base |
| `USER root` | Bascule temporairement en administrateur, nécessaire pour installer des paquets système |
| `apt-get install procps openjdk-17-jre-headless` | Installe `ps` (`procps`, utilisé en interne par les scripts de démarrage de Spark) et une JVM headless (sans interface graphique, plus légère) — indispensable car PySpark s'exécute sur une JVM |
| `ln -sf ... /usr/lib/jvm/default-java` | Crée un lien symbolique stable vers l'installation Java réelle, quel que soit le chemin exact généré par le paquet système |
| `ENV JAVA_HOME=...` | Définit la variable que PySpark/Spark cherche pour localiser la JVM au démarrage |
| `ENV PATH=$JAVA_HOME/bin:$PATH` | Ajoute les exécutables Java au chemin de recherche du shell |
| `USER airflow` | Revient à l'utilisateur non-root standard, par bonne pratique de sécurité |
| `pip install pyspark==3.5.0` | Installe PySpark, qui fournit la commande `spark-submit` capable de soumettre des jobs à un cluster distant (`--master spark://spark:7077`) via le réseau Docker |

### Construire l'image et démarrer
```bash
docker compose build airflow
docker compose up -d --force-recreate airflow
```
**Explication :**
| Commande | Rôle |
|---|---|
| `docker compose build airflow` | Lit le `Dockerfile` du dossier `./airflow` et construit l'image personnalisée (peut prendre 1-2 minutes la première fois, le temps de télécharger Java et PySpark) |
| `up -d --force-recreate airflow` | Redémarre le conteneur avec la nouvelle image construite |

### Vérifier
```bash
docker exec -it <nom_conteneur_airflow> spark-submit --version
```
Doit maintenant afficher la version de Spark sans erreur `JAVA_HOME` ni `ps: command not found`.

---

## 5. Étape 1 — Démarrer l'infrastructure

```bash
cd projet-de-a-z
docker compose up -d
```

**Explication :**
| Commande | Rôle |
|---|---|
| `docker compose up` | Lit le fichier `docker-compose.yml` et démarre tous les services définis, dans l'ordre de leurs dépendances |
| `-d` | Mode détaché : les conteneurs tournent en arrière-plan, la main est rendue au terminal |

Vérifier que tout tourne :
```bash
docker compose ps
```
**Rôle :** liste tous les conteneurs du projet avec leur statut (`Up`, `Exited`), pour confirmer qu'aucun service n'a planté au démarrage.

---

## 6. Étape 2 — Kafka : créer le topic et comprendre les commandes

### 6.1 Se connecter au conteneur Kafka
```bash
docker exec -it <nom_du_conteneur_kafka> bash
```
**Rôle :** `docker exec -it` ouvre un terminal interactif **à l'intérieur** du conteneur Kafka déjà démarré, pour y exécuter les outils en ligne de commande de Kafka (installés dans l'image).

### 6.2 Créer le topic
```bash
kafka-topics --create \
  --topic commandes \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

**Explication commande par commande :**
| Option | Rôle |
|---|---|
| `--create` | Indique qu'on veut créer un nouveau topic |
| `--topic commandes` | Nom du topic — c'est le "canal" dans lequel le producer publiera les événements de commande |
| `--bootstrap-server` | Adresse du broker Kafka auquel se connecter |
| `--partitions 3` | Découpe le topic en 3 partitions, ce qui permet à plusieurs consommateurs de lire en parallèle (parallélisme) |
| `--replication-factor 1` | Nombre de copies de chaque partition (1 = pas de réplication, suffisant en local ; en production on mettrait 3 pour la tolérance aux pannes) |

### 6.3 Vérifier la création
```bash
kafka-topics --list --bootstrap-server localhost:9092
```
**Rôle :** liste tous les topics existants sur le broker, pour confirmer que `commandes` a bien été créé.

### 6.4 Consommer manuellement pour tester (optionnel)
```bash
kafka-console-consumer --topic commandes --bootstrap-server localhost:9092 --from-beginning
```
**Rôle :** affiche en direct dans le terminal chaque message publié sur le topic — outil de debug pour vérifier que des données arrivent bien, avant même de brancher Spark.

---

## 7. Étape 3 — Le Producer Kafka (générateur d'événements)

`producer/kafka_producer.py` :
```python
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
```

**Explication commande par commande :**
| Ligne / commande | Rôle |
|---|---|
| `KafkaProducer(bootstrap_servers=..., value_serializer=...)` | Crée le client producer, connecté au broker, avec une fonction qui convertit chaque dictionnaire Python en JSON binaire avant envoi |
| `producer.send("commandes", value=commande)` | Publie un message sur le topic `commandes` — c'est l'action centrale : chaque appel correspond à un nouvel événement dans le flux |
| `time.sleep(1)` | Simule un flux continu réaliste (une commande par seconde) plutôt qu'un envoi instantané massif |

### Lancer le producer
```bash
pip install kafka-python
python producer/kafka_producer.py
```
**Rôle :** exécute le script en continu (boucle infinie), simulant un flux réel de commandes e-commerce arrivant sur le topic Kafka.

---

## 8. Étape 4 — Spark Structured Streaming : Kafka → Bronze

`spark_jobs/streaming_kafka_to_bronze.py` :
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("KafkaToBronze").getOrCreate()

schema = (StructType()
    .add("id_commande", IntegerType())
    .add("produit", StringType())
    .add("montant", DoubleType())
    .add("ville", StringType())
    .add("timestamp", StringType())
)

df_raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "commandes")
    .option("startingOffsets", "earliest")
    .load()
)

df_parsed = (df_raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

query = (df_parsed.writeStream
    .format("parquet")
    .option("path", "/opt/data/bronze/commandes")
    .option("checkpointLocation", "/opt/data/checkpoints/bronze_commandes")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
```

**Explication commande par commande :**
| Commande | Rôle |
|---|---|
| `spark.readStream.format("kafka")` | Ouvre un flux de lecture continue depuis Kafka (au lieu de `spark.read` pour une lecture ponctuelle) |
| `.option("kafka.bootstrap.servers", "kafka:9092")` | Adresse du broker **vue depuis l'intérieur du réseau Docker** (nom du service, pas `localhost`) |
| `.option("subscribe", "commandes")` | Indique le topic à consommer |
| `.option("startingOffsets", "earliest")` | Demande à Spark de relire depuis le tout début du topic (utile en démo, en production on utiliserait souvent `latest`) |
| `selectExpr("CAST(value AS STRING)...")` | Kafka transmet chaque message sous forme binaire brute (`value`) — cette ligne le convertit en texte |
| `from_json(col("json_str"), schema)` | Parse le texte JSON en colonnes structurées, selon le schéma défini plus haut |
| `.writeStream.format("parquet")` | Définit la sortie du flux : écriture continue au format Parquet (colonne, compressé) |
| `.option("checkpointLocation", ...)` | Dossier où Spark enregistre sa progression (quels messages ont déjà été traités) — indispensable pour reprendre proprement après un redémarrage sans dupliquer ni perdre de données |
| `.trigger(processingTime="10 seconds")` | Traite les nouveaux messages par micro-lots toutes les 10 secondes, plutôt qu'en continu message par message (meilleur compromis latence/performance) |
| `query.awaitTermination()` | Maintient le job actif indéfiniment, à l'écoute de nouveaux messages |

### Lancer le job de streaming
```bash
docker exec -it <nom_conteneur_spark> bash
```
**Rôle :** ouvre un terminal interactif à l'intérieur du conteneur Spark, déjà démarré par Docker Compose.

> ⚠️ **Point technique à connaître :** sur l'image `bitnamilegacy/spark`, une session ouverte via `docker exec` ne repasse pas par le script d'entrypoint qui donne normalement une identité système à l'utilisateur du conteneur (d'où le prompt `I have no name!@...`). Sans cette identité, Hadoop (utilisé en interne par Spark) échoue à l'authentification locale dès qu'on utilise `--packages`. Il faut donc recréer cette identité manuellement à chaque nouvelle session `docker exec`, via `nss_wrapper` — le même mécanisme que l'entrypoint officiel utilise :

```bash
export NSS_WRAPPER_PASSWD=/tmp/nss_passwd
export NSS_WRAPPER_GROUP=/tmp/nss_group
echo "spark:x:$(id -u):$(id -g):Spark User:/tmp:/bin/bash" > $NSS_WRAPPER_PASSWD
echo "spark:x:$(id -g):" > $NSS_WRAPPER_GROUP
export LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
export HOME=/tmp
export HADOOP_USER_NAME=spark
```

**Explication :**
| Commande | Rôle |
|---|---|
| `NSS_WRAPPER_PASSWD` / `NSS_WRAPPER_GROUP` | Fichiers temporaires simulant les entrées `/etc/passwd` et `/etc/group`, sans toucher aux vrais fichiers système |
| `echo "spark:x:$(id -u):$(id -g):..." > $NSS_WRAPPER_PASSWD` | Crée une entrée utilisateur valide (avec ton UID/GID réel du conteneur) dans ce fichier temporaire |
| `LD_PRELOAD=.../libnss_wrapper.so` | Charge une librairie qui intercepte les appels système de résolution d'utilisateur (glibc) et les redirige vers ces fichiers temporaires |
| `HOME=/tmp` | Donne un répertoire personnel valide, utilisé par Spark/Ivy pour son cache de dépendances (`~/.ivy2`) |
| `HADOOP_USER_NAME=spark` | Indique explicitement à Hadoop quel nom d'utilisateur utiliser pour ses propres besoins internes |

Une fois ces variables définies, lancer le job :
```bash
spark-submit \
  --master local[2] \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark_jobs/streaming_kafka_to_bronze.py
```
**Explication :**
| Élément | Rôle |
|---|---|
| `spark-submit` | Commande standard pour soumettre un script Spark à exécuter sur le cluster |
| `--conf spark.jars.ivy=/tmp/.ivy2` | Force Ivy (le gestionnaire de dépendances utilisé par `--packages`) à utiliser `/tmp/.ivy2` comme répertoire de cache, un chemin garanti accessible en écriture dans le conteneur |
| `--packages spark-sql-kafka-0-10` | Télécharge et ajoute la librairie de connecteur Kafka nécessaire pour que Spark sache lire ce type de source |

Ce job **tourne en continu** (c'est un job de streaming) : c'est lui qui alimente la couche Bronze au fil de l'eau, indépendamment du reste du pipeline batch.

---

## 9. Étape 5 — Base PostgreSQL : création du schéma

`sql/init_postgres.sql` (exécuté automatiquement au premier démarrage du conteneur postgres, via le volume monté) :
```sql
CREATE DATABASE analytics;

\connect analytics

CREATE TABLE IF NOT EXISTS reporting_commandes (
    ville VARCHAR(50),
    jour DATE,
    volume_total NUMERIC(12,2),
    nb_commandes INTEGER,
    PRIMARY KEY (ville, jour)
);
```

**Explication commande par commande :**
| Commande | Rôle |
|---|---|
| `CREATE DATABASE analytics` | Crée une base séparée pour les données métier, distincte de la base `airflow` utilisée par l'orchestrateur pour ses propres métadonnées |
| `\connect analytics` | Bascule la session SQL sur cette nouvelle base avant de créer les tables |
| `CREATE TABLE IF NOT EXISTS` | Crée la table cible seulement si elle n'existe pas déjà (évite une erreur si le script est rejoué) |
| `PRIMARY KEY (ville, jour)` | Garantit qu'il ne peut exister qu'une seule ligne par combinaison ville/jour — empêche les doublons lors des réexécutions du pipeline |

> ⚠️ **Point de vigilance :** ce script ne s'exécute automatiquement que lors de la **toute première initialisation** du volume de données PostgreSQL (dossier vide). Si le conteneur `postgres` a déjà été démarré une fois avant l'ajout de ce script (ou si son volume existe déjà d'un essai précédent), le script ne sera **jamais rejoué**, et l'écriture Gold échouera avec `FATAL: database "analytics" does not exist`.
>
> **Deux façons de corriger ça :**
> - **Repartir de zéro** (perd aussi les métadonnées Airflow, il faudra recréer l'utilisateur admin) : `docker compose down -v` puis `docker compose up -d`.
> - **Créer la base manuellement sans tout réinitialiser** (plus rapide) :
> ```bash
> docker exec -it <nom_conteneur_postgres> psql -U airflow -d airflow -c "CREATE DATABASE analytics;"
> docker exec -it <nom_conteneur_postgres> psql -U airflow -d analytics -c "CREATE TABLE IF NOT EXISTS reporting_commandes (ville VARCHAR(50), jour DATE, volume_total NUMERIC(12,2), nb_commandes INTEGER, PRIMARY KEY (ville, jour));"
> ```

---

## 10. Étape 6 — Scripts PySpark batch : Silver et Gold

### 10.1 `spark_jobs/batch_bronze_to_silver.py`
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

df = spark.read.parquet("/opt/data/bronze/commandes")

df_silver = (df
    .dropDuplicates(["id_commande"])
    .filter(F.col("montant") > 0)
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("jour", F.to_date("timestamp"))
)

df_silver.write.mode("overwrite").parquet("/opt/data/silver/commandes")
print(f"Silver : {df_silver.count()} lignes écrites")
```

**Explication (points nouveaux par rapport aux modules précédents) :**
| Commande | Rôle |
|---|---|
| `spark.read.parquet(...)` | Relit l'intégralité des fichiers Bronze accumulés par le job de streaming depuis le dernier passage batch |
| `F.to_timestamp("timestamp")` | Convertit la chaîne de caractères ISO envoyée par le producer en véritable type `timestamp` Spark |
| `F.to_date("timestamp")` | Extrait la date seule (sans l'heure), utilisée ensuite pour les agrégations journalières |
| `.write.mode("overwrite")` | Recalcule entièrement la couche Silver à chaque exécution batch (approche simple ; en production on ferait un traitement incrémental) |

### 10.2 `spark_jobs/batch_silver_to_gold.py`
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

df_silver = spark.read.parquet("/opt/data/silver/commandes")

df_gold = (df_silver
    .groupBy("ville", "jour")
    .agg(
        F.sum("montant").alias("volume_total"),
        F.count("*").alias("nb_commandes")
    )
)

(df_gold.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/analytics")
    .option("dbtable", "reporting_commandes")
    .option("user", "airflow")
    .option("password", "airflow")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)
print(f"Gold : {df_gold.count()} lignes écrites dans PostgreSQL")
```

**Explication (points nouveaux) :**
| Commande | Rôle |
|---|---|
| `.config("spark.jars.packages", "org.postgresql:postgresql:...")` | Ajoute le driver JDBC PostgreSQL, nécessaire pour que Spark sache dialoguer avec une base relationnelle |
| `groupBy("ville", "jour").agg(...)` | Calcule le volume total et le nombre de commandes par ville et par jour — c'est l'agrégat métier final |
| `.write.format("jdbc")` | Bascule le mode d'écriture : au lieu d'écrire des fichiers Parquet, Spark envoie les données via une connexion SQL directe vers PostgreSQL |
| `.option("url", "jdbc:postgresql://postgres:5432/analytics")` | Chaîne de connexion JDBC : protocole, nom du service Docker (`postgres`), port, nom de la base |
| `.option("driver", "org.postgresql.Driver")` | Précise explicitement la classe Java du driver JDBC à utiliser. Sans cette option, `DriverManager` peut échouer à trouver automatiquement le bon driver parmi les JARs chargés dynamiquement via `--packages`, provoquant l'erreur `No suitable driver` |
| `.mode("overwrite")` | Remplace le contenu de la table `reporting_commandes` à chaque exécution (cohérent avec le recalcul complet en Silver) |

---

## 11. Étape 7 — Airflow : le DAG d'orchestration expliqué tâche par tâche

> **Choix technique : `--master local[2]` plutôt que `spark://spark:7077`.** En théorie, les tâches batch devraient s'exécuter sur le vrai cluster Spark distribué (`spark` + `spark-worker`). En pratique, sur une image `bitnamilegacy/spark` figée, le build binaire du Spark du worker peut différer légèrement du PySpark installé via `pip` dans le conteneur Airflow, même à version affichée identique (3.5.0) — ce qui casse la sérialisation Java entre driver et executor (`InvalidClassException: serialVersionUID` incompatible). Faire tourner ces jobs batch en local (`local[2]`, directement dans le conteneur Airflow qui contient déjà PySpark) contourne ce problème : c'est un choix pragmatique et courant pour des jobs de taille modeste en environnement de démonstration. Le cluster `spark`/`spark-worker` reste disponible dans l'architecture pour le job de streaming ou pour une évolution future vers un vrai déploiement distribué.

`airflow/dags/pipeline_commandes_dag.py` :
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "data-engineer",
    "retries": 1,
}

with DAG(
    dag_id="pipeline_commandes_batch",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kafka", "spark", "postgres"],
) as dag:

    clean_silver_task = BashOperator(
        task_id="clean_silver_dir",
        bash_command="rm -rf /opt/data/silver/commandes",
    )

    silver_task = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "spark-submit --master local[2] "
            "/opt/spark_jobs/batch_bronze_to_silver.py"
        ),
    )

    gold_task = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "spark-submit --master local[2] "
            "--packages org.postgresql:postgresql:42.7.3 "
            "/opt/spark_jobs/batch_silver_to_gold.py"
        ),
    )

    clean_silver_task >> silver_task >> gold_task
```

**Explication commande par commande :**
| Élément | Rôle |
|---|---|
| `DAG(dag_id=..., schedule_interval="0 2 * * *", ...)` | Déclare le pipeline et sa planification en syntaxe **cron** : `0 2 * * *` signifie "tous les jours à 2h du matin" |
| `start_date` | Date à partir de laquelle le DAG est éligible à l'exécution |
| `catchup=False` | Empêche Airflow de rattraper automatiquement toutes les exécutions passées manquées depuis `start_date` — sans ça, au premier démarrage, Airflow tenterait de rejouer tous les jours écoulés |
| `BashOperator(task_id=..., bash_command=...)` | Définit une tâche qui exécute une commande shell — ici, un nettoyage de dossier ou un `spark-submit` |
| `clean_silver_task` | Supprime explicitement le dossier `silver/commandes` en bash (`rm -rf`) avant l'écriture Spark, pour contourner un problème connu de verrouillage de fichiers sur les volumes Docker montés depuis Windows/WSL2, où le mécanisme interne de nettoyage de Spark (`mode("overwrite")`) échoue silencieusement |
| `clean_silver_task >> silver_task >> gold_task` | Définit la **chaîne de dépendances** : chaque tâche ne démarre que si la précédente a réussi — c'est la syntaxe Airflow pour construire le DAG (Directed Acyclic Graph) |
| `retries: 1` | Si une tâche échoue, Airflow la relance automatiquement une fois avant de la marquer en échec définitif |

**À quoi sert Airflow ici, concrètement :** sans orchestrateur, il faudrait lancer manuellement les deux scripts PySpark chaque jour, dans le bon ordre, et surveiller à la main si l'un des deux a échoué. Airflow automatise cette planification, garantit l'ordre d'exécution, et donne une interface de suivi (retries, logs, alertes).

---

## 12. Étape 8 — Lancer et suivre le pipeline complet

### 12.1 Copier le DAG et redémarrer Airflow
```bash
docker compose restart airflow
```
**Rôle :** relance le conteneur Airflow pour qu'il détecte le nouveau fichier DAG déposé dans le dossier monté `airflow/dags`.

### 12.2 Ouvrir l'interface Airflow
- Ouvrir un navigateur sur `http://localhost:8081`.
- Se connecter avec `admin` / `admin` (créés dans la commande de démarrage du conteneur, section 4).

### 12.3 Activer et déclencher le DAG
- Dans la liste des DAGs, trouver `pipeline_commandes_batch`.
- Cliquer sur le **toggle** à gauche du nom pour l'activer (passe de gris à bleu).
- Cliquer sur le bouton **▶ (Trigger DAG)** à droite pour lancer une exécution immédiate, sans attendre 2h du matin.

**À quoi ça sert :** le toggle active la planification automatique future ; le déclenchement manuel permet de tester le pipeline tout de suite pendant le développement.

### 12.4 Suivre l'exécution
- Cliquer sur le nom du DAG → vue **Graph** : affiche les deux tâches (`bronze_to_silver`, `silver_to_gold`) avec un code couleur (vert = succès, rouge = échec, jaune = en cours).
- Cliquer sur une tâche → **Logs** : affiche la sortie complète de la commande `spark-submit`, utile pour diagnostiquer une erreur.

---

## 13. Étape 9 — Requêtes SQL finales et vérification

### 13.1 Se connecter à PostgreSQL
```bash
docker exec -it <nom_conteneur_postgres> psql -U airflow -d analytics
```
**Rôle :** `psql` est le client en ligne de commande de PostgreSQL ; cette commande ouvre une session SQL interactive directement dans la base `analytics`.

### 13.2 Vérifier les données
```sql
SELECT * FROM reporting_commandes
ORDER BY jour DESC, volume_total DESC
LIMIT 10;
```
**Rôle :** affiche les 10 lignes les plus récentes et les plus importantes en volume — vérifie que le pipeline Kafka → Spark Streaming → Silver → Gold → PostgreSQL a bien fonctionné de bout en bout.

### 13.3 Requête analytique un peu plus poussée
```sql
SELECT
    ville,
    SUM(volume_total) AS total_periode,
    ROUND(AVG(nb_commandes), 1) AS moyenne_commandes_jour
FROM reporting_commandes
GROUP BY ville
ORDER BY total_periode DESC;
```
**Explication :**
| Élément | Rôle |
|---|---|
| `SUM(volume_total)` | Cumule le volume total sur toute la période disponible, par ville |
| `AVG(nb_commandes)` | Calcule la moyenne quotidienne du nombre de commandes par ville |
| `ROUND(..., 1)` | Arrondit le résultat à une décimale pour la lisibilité |
| `GROUP BY ville` | Applique les agrégations `SUM`/`AVG` par ville plutôt que sur l'ensemble de la table |

---

## 14. Commandes Docker utiles (récapitulatif)

| Commande | Rôle |
|---|---|
| `docker compose up -d` | Démarre tous les services en arrière-plan |
| `docker compose ps` | Liste les conteneurs et leur statut |
| `docker compose logs -f <service>` | Affiche les logs d'un service en continu (ex. `docker compose logs -f kafka`), utile pour debugger |
| `docker compose stop` | Arrête les conteneurs sans les supprimer (les données des volumes sont conservées) |
| `docker compose down` | Arrête et supprime les conteneurs (les volumes nommés persistent, sauf avec `-v`) |
| `docker exec -it <conteneur> bash` | Ouvre un terminal interactif à l'intérieur d'un conteneur en cours d'exécution |

---

## 15. Résumé du projet pour l'entretien

À retenir pour présenter ce projet :
- **Kafka** : ingestion d'événements temps réel (producer Python), topic partitionné pour le parallélisme.
- **Spark Structured Streaming** : consommation continue du topic Kafka, écriture en micro-batchs vers une couche Bronze Parquet, avec checkpointing pour la tolérance aux pannes.
- **Batch PySpark orchestré par Airflow** : transformation Bronze → Silver (nettoyage, dédoublonnage, typage) puis Silver → Gold (agrégats métier), planifiée chaque nuit via un DAG avec dépendances explicites (`>>`).
- **SQL / PostgreSQL** : couche de restitution finale, interrogée par des requêtes d'agrégation classiques (`GROUP BY`, `SUM`, `AVG`).
- **Docker Compose** : orchestration de toute l'infrastructure (Kafka, Zookeeper, Spark, Airflow, PostgreSQL) en local, reproductible en une seule commande.
- Ce projet illustre la différence entre **traitement temps réel** (Kafka + Streaming, latence de secondes) et **traitement batch** (Airflow + Spark, latence de type "une fois par jour"), et pourquoi les deux coexistent souvent dans une architecture réelle.
- Point de troubleshooting réel rencontré et à savoir expliquer en entretien : une incompatibilité de sérialisation Java entre le PySpark du driver (installé via pip) et le Spark du cluster distant (image Bitnami figée), résolue en exécutant les jobs batch en mode local plutôt que sur le cluster — illustre l'importance de la cohérence de version driver/executor dans un vrai déploiement Spark.

---

*Guide généré pour un projet personnel de démonstration — noms de conteneurs, ports et mots de passe (`airflow`/`airflow`) sont à adapter à ton environnement réel, notamment ne jamais utiliser des mots de passe aussi simples en production.*
