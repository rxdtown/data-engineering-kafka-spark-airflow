# Pipeline Data Engineering — Kafka + Spark + Airflow + PostgreSQL

![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache-Kafka-231F20?logo=apachekafka&logoColor=white)
![Spark](https://img.shields.io/badge/Apache-Spark%203.5-E25A1C?logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache-Airflow%202.9-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)

Pipeline data engineering complet et 100% local, combinant une chaîne **temps réel** (Kafka → Spark Structured Streaming) et une chaîne **batch orchestrée** (Airflow → PySpark → PostgreSQL). Le tout est conteneurisé avec Docker Compose, reproductible en une seule commande.

Projet volontairement complet : il couvre les deux grandes familles du Data Engineering (streaming et batch) et les technologies les plus demandées en entretien — **Kafka, Spark, Airflow, SQL, Docker**.

---

## Sommaire

- [Architecture](#architecture)
- [Démarrage rapide](#démarrage-rapide)
- [Prérequis](#prérequis)
- [Arborescence du projet](#arborescence-du-projet)
- [Le fichier docker-compose.yml expliqué service par service](#le-fichier-docker-composeyml-expliqué-service-par-service)
- [Le Dockerfile personnalisé pour Airflow](#le-dockerfile-personnalisé-pour-airflow)
- [Étape 1 — Démarrer l'infrastructure](#étape-1--démarrer-linfrastructure)
- [Étape 2 — Kafka : créer le topic](#étape-2--kafka--créer-le-topic)
- [Étape 3 — Le Producer Kafka](#étape-3--le-producer-kafka)
- [Étape 4 — Spark Structured Streaming : Kafka → Bronze](#étape-4--spark-structured-streaming--kafka--bronze)
- [Étape 5 — Base PostgreSQL](#étape-5--base-postgresql)
- [Étape 6 — Scripts PySpark batch : Silver et Gold](#étape-6--scripts-pyspark-batch--silver-et-gold)
- [Étape 7 — Le DAG Airflow](#étape-7--le-dag-airflow)
- [Étape 8 — Lancer et suivre le pipeline complet](#étape-8--lancer-et-suivre-le-pipeline-complet)
- [Étape 9 — Requêtes SQL finales](#étape-9--requêtes-sql-finales)
- [Commandes Docker utiles](#commandes-docker-utiles)
- [Points de troubleshooting rencontrés](#points-de-troubleshooting-rencontrés)
- [Résumé pour l'entretien](#résumé-pour-lentretien)

---

## Architecture

```
┌─────────────┐     ┌───────────────┐     ┌──────────────────────┐
│  Producer   │───▶│  Kafka Topic   │ ───▶|  Spark Structured   │
│  (Python)   │     │  "commandes"  │     │  Streaming           │
│  génère des │     │  (+Zookeeper) │     │ (consumer temps réel)│
│  commandes  │     └───────────────┘     └───────────┬──────────┘
└─────────────┘                                       │ écrit en continu
                                                      ▼
                                          ┌─────────────────────────┐
                                          │  BRONZE (Parquet local) │
                                          │  données brutes horodat.│
                                          └───────────┬─────────────┘
                                                      │
                                        Chaque nuit, Airflow déclenche :
                                                      ▼
                                          ┌─────────────────────────┐
                                          │  PySpark batch : SILVER │
                                          │ nettoyage, dédup, typage│
                                          └───────────┬─────────────┘
                                                       ▼
                                          ┌─────────────────────────┐
                                          │  PySpark batch : GOLD   │
                                          │  agrégats métier        │
                                          └───────────┬─────────────┘
                                                       ▼
                                          ┌─────────────────────────┐
                                          │  PostgreSQL             │
                                          │  table analytique finale│
                                          └─────────────────────────┘
```

**Pourquoi cette architecture ?** Elle reproduit un cas réel : les événements (commandes) arrivent en continu et doivent être capturés sans perte (Kafka + Streaming), mais les transformations lourdes et les agrégats métier n'ont pas besoin d'être recalculés en permanence — un traitement batch orchestré chaque nuit (Airflow + PySpark) suffit et coûte moins cher en ressources. C'est le pattern **Lambda/Kappa simplifié** qu'on retrouve dans beaucoup d'entreprises.

---

## Démarrage rapide

```bash
git clone <url-du-depot>
cd projet-de-a-z
docker compose build airflow
docker compose up -d
```

Puis :
- **Airflow** : http://localhost:8081 (`admin` / `admin`)
- **PostgreSQL** : `localhost:5432` (`airflow` / `airflow`, base `analytics`)
- **Spark master UI** : http://localhost:8080

Pour le détail complet des étapes (création du topic Kafka, lancement du producer, du job de streaming, activation du DAG…), voir les sections ci-dessous.

---

## Prérequis

- **Docker Desktop** installé et démarré (inclut Docker Compose).
- Au moins 8 Go de RAM disponibles pour Docker (Kafka + Spark + Airflow + Postgres en simultané).
- Un éditeur de code (VS Code recommandé).

Plutôt que d'installer Kafka, Spark, Airflow et PostgreSQL séparément sur la machine (source classique de conflits de versions), chaque outil tourne dans son propre conteneur isolé, démarré et arrêté d'un seul coup avec Docker Compose — exactement comme dans une vraie stack de production.

---

## Arborescence du projet

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

Séparer clairement chaque responsabilité (producteur, jobs Spark, SQL, orchestration) rend le projet lisible — chaque dossier correspond à une brique technique.

---

## Le fichier docker-compose.yml expliqué service par service

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
| `volumes` | Monte un dossier local dans le conteneur, pour que les fichiers (scripts Spark, DAGs, données) soient partagés entre la machine hôte et les conteneurs sans les copier |
| `environment` | Définit des variables de configuration lues par l'image au démarrage (identifiants, mode du service, adresses de connexion) |
| `build: ./airflow` | Construit une image personnalisée à partir du `Dockerfile` de ce dossier au lieu d'utiliser l'image officielle telle quelle — nécessaire ici pour ajouter Java et PySpark, absents de l'image de base |
| `user: "0:0"` | Force le conteneur à s'exécuter en tant que root (UID/GID 0) au lieu de l'utilisateur `airflow` par défaut. Nécessaire ici car le dossier `./data` est partagé entre plusieurs conteneurs (Spark, Airflow) qui écrivent avec des utilisateurs différents ; sans cela, Airflow n'a pas toujours le droit de supprimer ou modifier des fichiers créés par un autre conteneur sur le volume partagé |

---

## Le Dockerfile personnalisé pour Airflow

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

| Ligne | Rôle |
|---|---|
| `FROM apache/airflow:2.9.0` | Part de l'image officielle Airflow comme base |
| `USER root` | Bascule temporairement en administrateur, nécessaire pour installer des paquets système |
| `apt-get install procps openjdk-17-jre-headless` | Installe `ps` (`procps`, utilisé en interne par les scripts de démarrage de Spark) et une JVM headless — indispensable car PySpark s'exécute sur une JVM |
| `ln -sf ... /usr/lib/jvm/default-java` | Crée un lien symbolique stable vers l'installation Java réelle, quel que soit le chemin exact généré par le paquet système |
| `ENV JAVA_HOME=...` | Définit la variable que PySpark/Spark cherche pour localiser la JVM au démarrage |
| `USER airflow` | Revient à l'utilisateur non-root standard, par bonne pratique de sécurité |
| `pip install pyspark==3.5.0` | Installe PySpark, qui fournit la commande `spark-submit` capable de soumettre des jobs Spark |

```bash
docker compose build airflow
docker compose up -d --force-recreate airflow
docker exec -it <nom_conteneur_airflow> spark-submit --version
```

---

## Étape 1 — Démarrer l'infrastructure

```bash
cd projet-de-a-z
docker compose up -d
docker compose ps
```

`docker compose up -d` lit le fichier `docker-compose.yml` et démarre tous les services définis, dans l'ordre de leurs dépendances, en arrière-plan. `docker compose ps` liste ensuite tous les conteneurs avec leur statut, pour confirmer qu'aucun service n'a planté au démarrage.

---

## Étape 2 — Kafka : créer le topic

```bash
docker exec -it <nom_du_conteneur_kafka> bash
```

```bash
kafka-topics --create \
  --topic commandes \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

| Option | Rôle |
|---|---|
| `--create` | Indique qu'on veut créer un nouveau topic |
| `--topic commandes` | Nom du topic — le "canal" dans lequel le producer publiera les événements |
| `--partitions 3` | Découpe le topic en 3 partitions, ce qui permet à plusieurs consommateurs de lire en parallèle |
| `--replication-factor 1` | Nombre de copies de chaque partition (1 = pas de réplication, suffisant en local) |

Vérifier / tester :
```bash
kafka-topics --list --bootstrap-server localhost:9092
kafka-console-consumer --topic commandes --bootstrap-server localhost:9092 --from-beginning
```

---

## Étape 3 — Le Producer Kafka

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

`KafkaProducer` crée le client connecté au broker, avec une fonction qui convertit chaque dictionnaire Python en JSON binaire. `producer.send()` publie un message sur le topic à chaque itération — `time.sleep(1)` simule un flux continu réaliste (une commande par seconde).

```bash
pip install kafka-python
python producer/kafka_producer.py
```

---

## Étape 4 — Spark Structured Streaming : Kafka → Bronze

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

| Commande | Rôle |
|---|---|
| `spark.readStream.format("kafka")` | Ouvre un flux de lecture continue depuis Kafka |
| `.option("startingOffsets", "earliest")` | Relit depuis le début du topic (utile en démo) |
| `from_json(col("json_str"), schema)` | Parse le texte JSON en colonnes structurées |
| `.option("checkpointLocation", ...)` | Dossier où Spark enregistre sa progression — indispensable pour reprendre proprement sans dupliquer ni perdre de données |
| `.trigger(processingTime="10 seconds")` | Traite les nouveaux messages par micro-lots toutes les 10 secondes |

### Lancer le job de streaming

```bash
docker exec -it <nom_conteneur_spark> bash
```

> ⚠️ Sur l'image `bitnamilegacy/spark`, une session ouverte via `docker exec` ne repasse pas par le script d'entrypoint qui donne normalement une identité système à l'utilisateur du conteneur. Sans cette identité, Hadoop échoue à l'authentification locale dès qu'on utilise `--packages`. Il faut donc recréer cette identité manuellement via `nss_wrapper` :

```bash
export NSS_WRAPPER_PASSWD=/tmp/nss_passwd
export NSS_WRAPPER_GROUP=/tmp/nss_group
echo "spark:x:$(id -u):$(id -g):Spark User:/tmp:/bin/bash" > $NSS_WRAPPER_PASSWD
echo "spark:x:$(id -g):" > $NSS_WRAPPER_GROUP
export LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
export HOME=/tmp
export HADOOP_USER_NAME=spark
```

Puis lancer le job :
```bash
spark-submit \
  --master local[2] \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark_jobs/streaming_kafka_to_bronze.py
```

Ce job **tourne en continu** : il alimente la couche Bronze au fil de l'eau, indépendamment du reste du pipeline batch.

---

## Étape 5 — Base PostgreSQL

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

> ⚠️ **Point de vigilance :** ce script ne s'exécute automatiquement que lors de la **toute première initialisation** du volume de données PostgreSQL. Si le conteneur `postgres` a déjà été démarré une fois avant l'ajout de ce script, il ne sera jamais rejoué, et l'écriture Gold échouera avec `FATAL: database "analytics" does not exist`.
>
> **Correction rapide sans tout réinitialiser :**
> ```bash
> docker exec -it <nom_conteneur_postgres> psql -U airflow -d airflow -c "CREATE DATABASE analytics;"
> docker exec -it <nom_conteneur_postgres> psql -U airflow -d analytics -c "CREATE TABLE IF NOT EXISTS reporting_commandes (ville VARCHAR(50), jour DATE, volume_total NUMERIC(12,2), nb_commandes INTEGER, PRIMARY KEY (ville, jour));"
> ```

---

## Étape 6 — Scripts PySpark batch : Silver et Gold

`spark_jobs/batch_bronze_to_silver.py` :
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

`spark.read.parquet(...)` relit l'intégralité des fichiers Bronze accumulés depuis le dernier passage batch. Le pipeline déduplique (`dropDuplicates`), filtre les montants invalides, et type correctement les colonnes temporelles.

`spark_jobs/batch_silver_to_gold.py` :
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

| Commande | Rôle |
|---|---|
| `groupBy("ville", "jour").agg(...)` | Calcule le volume total et le nombre de commandes par ville et par jour — l'agrégat métier final |
| `.write.format("jdbc")` | Envoie les données via une connexion SQL directe vers PostgreSQL, au lieu d'écrire des fichiers Parquet |
| `.option("driver", "org.postgresql.Driver")` | Précise explicitement la classe Java du driver JDBC à utiliser — sans cette option, `DriverManager` peut échouer à trouver automatiquement le bon driver parmi les JARs chargés via `--packages`, provoquant l'erreur `No suitable driver` |

---

## Étape 7 — Le DAG Airflow

> **Choix technique : `--master local[2]` plutôt que `spark://spark:7077`.** En théorie, les tâches batch devraient s'exécuter sur le vrai cluster Spark distribué. En pratique, sur une image `bitnamilegacy/spark` figée, le build binaire du Spark du worker peut différer légèrement du PySpark installé via `pip` dans le conteneur Airflow, même à version affichée identique (3.5.0) — ce qui casse la sérialisation Java entre driver et executor (`InvalidClassException: serialVersionUID` incompatible). Faire tourner ces jobs batch en local (`local[2]`, directement dans le conteneur Airflow) contourne ce problème : un choix pragmatique et courant pour des jobs de taille modeste en environnement de démonstration.

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

| Élément | Rôle |
|---|---|
| `schedule_interval="0 2 * * *"` | Syntaxe cron : "tous les jours à 2h du matin" |
| `catchup=False` | Empêche Airflow de rattraper automatiquement toutes les exécutions passées manquées depuis `start_date` |
| `clean_silver_task` | Supprime explicitement le dossier `silver/commandes` en bash (`rm -rf`) avant l'écriture Spark, pour contourner un problème de verrouillage de fichiers sur les volumes Docker montés depuis Windows/WSL2 |
| `clean_silver_task >> silver_task >> gold_task` | Chaîne de dépendances : chaque tâche ne démarre que si la précédente a réussi (syntaxe du DAG Airflow) |
| `retries: 1` | Si une tâche échoue, Airflow la relance automatiquement une fois avant de la marquer en échec définitif |

Sans orchestrateur, il faudrait lancer manuellement les scripts PySpark chaque jour, dans le bon ordre, et surveiller à la main les échecs. Airflow automatise cette planification et donne une interface de suivi (retries, logs, alertes).

---

## Étape 8 — Lancer et suivre le pipeline complet

```bash
docker compose restart airflow
```

- Ouvrir `http://localhost:8081`, se connecter (`admin` / `admin`).
- Dans la liste des DAGs, activer `pipeline_commandes_batch` (toggle).
- Cliquer sur **▶ Trigger DAG** pour lancer une exécution immédiate.
- Vue **Graph** : suit l'état de chaque tâche (vert = succès, rouge = échec, jaune = en cours).
- Cliquer sur une tâche → **Logs** pour diagnostiquer une erreur.

---

## Étape 9 — Requêtes SQL finales

```bash
docker exec -it <nom_conteneur_postgres> psql -U airflow -d analytics
```

```sql
SELECT * FROM reporting_commandes
ORDER BY jour DESC, volume_total DESC
LIMIT 10;
```

```sql
SELECT
    ville,
    SUM(volume_total) AS total_periode,
    ROUND(AVG(nb_commandes), 1) AS moyenne_commandes_jour
FROM reporting_commandes
GROUP BY ville
ORDER BY total_periode DESC;
```

---

## Commandes Docker utiles

| Commande | Rôle |
|---|---|
| `docker compose up -d` | Démarre tous les services en arrière-plan |
| `docker compose ps` | Liste les conteneurs et leur statut |
| `docker compose logs -f <service>` | Affiche les logs d'un service en continu, utile pour debugger |
| `docker compose stop` | Arrête les conteneurs sans les supprimer (données des volumes conservées) |
| `docker compose down` | Arrête et supprime les conteneurs |
| `docker compose down -v` | Arrête, supprime les conteneurs **et** les volumes (reset complet) |
| `docker exec -it <conteneur> bash` | Ouvre un terminal interactif dans un conteneur en cours d'exécution |

