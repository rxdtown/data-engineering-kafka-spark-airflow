# Pipeline Data Engineering — Kafka + Spark + Airflow + PostgreSQL

Pipeline complet combinant traitement temps réel et batch, 100% conteneurisé avec Docker.

## Architecture

Producer Kafka → Spark Structured Streaming → Bronze (Parquet)
→ Airflow (orchestration quotidienne) → Spark batch (Silver → Gold) → PostgreSQL

## Technologies

- **Kafka** : ingestion d'événements temps réel
- **Spark Structured Streaming** : consommation continue, écriture Bronze
- **Spark (batch)** : transformation Silver et agrégation Gold
- **Airflow** : orchestration du pipeline batch
- **PostgreSQL** : couche de restitution analytique
- **Docker Compose** : orchestration de toute l'infrastructure

## Lancer le projet

\`\`\`bash
docker compose up -d
\`\`\`

Interface Airflow : http://localhost:8081 (admin/admin)

## Structure du projet

\`\`\`
projet-de-a-z/
├── docker-compose.yml
├── producer/              # Producer Kafka Python
├── spark_jobs/             # Scripts PySpark (streaming + batch)
├── sql/                    # Initialisation PostgreSQL
├── airflow/                # DAGs et Dockerfile personnalisé
└── data/                   # Données générées (non versionné)
\`\`\`