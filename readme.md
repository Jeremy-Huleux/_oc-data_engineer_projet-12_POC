# POC Data Engineering : architecture hybride (batch & streaming) - "Prime sportive"

Ce dépôt contient la preuve de concept (POC) d'une architecture de données hybride, combinant un traitement par lots (Batch) et un traitement en temps réel (Streaming).

Ce projet a été réalisé dans le cadre de la simulation de la mise en place d'une "Prime Sportive" (récompense financière pour les employés utilisant des modes de transport doux), afin d'en évaluer l'impact financier et d'automatiser les alertes métier.

## Objectifs du projet

1. **Ingestion & Validation :** Collecter les déclarations de trajets des employés et valider la cohérence des distances réelles via l'API Google Maps Distance Matrix.
2. **Streaming & CDC (Temps Réel) :** Mettre en place un pipeline Change Data Capture (CDC) pour intercepter toute nouvelle déclaration dans la base de données et envoyer une notification instantanée sur Slack, sans impacter les performances de la base de données transactionnelle.
3. **Traitement Batch :** Développer un job de transformation permettant de calculer l'éligibilité à la prime et de générer un dataset consolidé.
4. **Business Intelligence (BI) :** Fournir un tableau de bord dynamique à la direction avec un paramètre de scénario (What-If) pour simuler l'impact financier d'une variation du taux de la prime en temps réel.

## Architecture technique

L'architecture repose sur des conteneurs isolés et reproductibles :

* **Base de Données Transactionnelle :** PostgreSQL
* **Change Data Capture (CDC) :** Debezium
* **Message Broker (Streaming) :** Redpanda (Compatible Kafka)
* **Traitement Batch :** PySpark
* **Data Visualisation :** Power BI (avec mesures DAX pour paramètres dynamiques et formatage conditionnel)
* **Déploiement :** Docker & Docker Compose

## Structure du dépôt

* `docker-compose.yml` : Configuration de l'infrastructure (PostgreSQL, Redpanda, Debezium).
* `init.sql` : Script d'initialisation de la base de données PostgreSQL.
* `etl_poc.py` : Script d'ingestion et d'appel à l'API Google Maps.
* `slack_consumer.py` : Script d'écoute du topic Redpanda et d'envoi des notifications Slack.
* `spark_job.py` : Job PySpark pour la transformation des données et l'export CSV.
* `.env.example` : Modèle de variables d'environnement (API keys, identifiants BDD) garantissant la sécurité des secrets.
* `dashboard_prime_sportive.pbix` : Tableau de bord Power BI interactif.

## Prérequis et installation

### 1. Cloner le dépôt et configurer l'environnement

Clonez le dépôt sur votre machine locale et créez un fichier `.env` basé sur `.env.example` en y ajoutant vos propres clés API (Slack, Google Maps).

```bash
git clone https://github.com/Jeremy-Huleux/nom-de-ton-repo.git
cd nom-de-ton-repo
cp .env.example .env

```

### 2. Lancer l'infrastructure Docker

Démarrez PostgreSQL, Redpanda et les services associés en tâche de fond.

```bash
docker-compose up -d

```

### 3. Configurer Debezium (CDC)

Lancez une requête POST via cURL (ou Postman) vers l'API de Debezium Connect pour lui indiquer de surveiller la table `activites_sportives` de PostgreSQL et d'envoyer les événements vers Redpanda.

### 4. Exécuter les pipelines Python

Installez les dépendances (`pip install -r requirements.txt`) puis exécutez les scripts de traitement :

* `python etl_poc.py` pour générer des données.
* `python slack_consumer.py` pour lancer l'écoute en temps réel.
* `python spark_job.py` pour consolider les données destinées à la Business Intelligence.

## Sécurité et conformité RGPD

Ce projet intègre les principes de Privacy by Design :

* **Minimisation des données :** Aucun point de géolocalisation GPS précis n'est stocké en base, seules les distances calculées sont conservées.
* **Gestion des secrets :** Utilisation stricte de fichiers `.env` et d'un `.gitignore` configuré pour empêcher la fuite d'informations sensibles (clés API, mots de passe, données RH brutes).

---

## Auteur

**Jérémy Huleux**
Actuellement en formation Data Engineer (OpenClassrooms).
*Je suis à la recherche d'un job dans la Data Engineering ou dans le développement dans la région Hauts-de-France (Lille, Calais, Boulogne-sur-Mer, Ardres).*

* [LinkedIn](https://www.linkedin.com/in/huleux-jeremy)
* [GitHub](https://github.com/Jeremy-Huleux)