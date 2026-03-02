# BLS CEW Data Pipeline (Bronze → Silver → Gold)

Pipeline Data Engineering de bout en bout construit sur les données **BLS QCEW / CEW Annual Singlefile** (2015–2024/2025 selon disponibilité).

Ce projet automatise :
- L’ingestion des données BLS
- L’analyse exploratoire (EDA) des données brutes
- La transformation vers une couche Silver propre (Delta Lake)
- Le calcul de KPIs analytiques (Public vs Privé) avec Pandas

---

## Objectif du projet

L’objectif de ce projet est de concevoir une pipeline Data Engineering complète basée sur les données publiques du BLS (QCEW/CEW), allant de l’ingestion brute jusqu’à l’analyse avancée et la modélisation prédictive.

Le projet vise à :

- Automatiser le téléchargement et l’extraction des données annuelles BLS
- Mettre en place une architecture Data Lake structurée (Bronze / Silver / Gold)
- Appliquer des règles de nettoyage et de standardisation robustes
- Construire des indicateurs analytiques pertinents (KPIs)
- Comparer l’évolution de l’emploi entre secteur privé et secteur public
- Analyser l’impact des chocs économiques (ex: COVID-19)
- Implémenter un modèle de Machine Learning pour prévoir les tendances futures de l’emploi

Ce projet démontre la capacité à :

- Travailler avec des datasets volumineux (36M+ lignes brutes)
- Utiliser Spark et Delta Lake dans un environnement Databricks
- Concevoir une pipeline reproductible et scalable
- Transformer des données brutes en insights exploitables
- Intégrer l’analyse prédictive dans une architecture data moderne

En résumé, ce projet illustre une approche complète de l’ingénierie des données combinée à l’analyse avancée et au Machine Learning.

---

## Architecture
```
BLS Website
↓
Script Python d’automatisation
↓
Bronze (CSV bruts)
↓
EDA Bronze (contrôle qualité)
↓
Silver (Delta Lake propre)
↓
Gold KPIs (Pandas)
↓
Dashboard / Analyse
```

---

## Source des données

- Source officielle : Bureau of Labor Statistics (BLS)
- Dataset utilisé : CEW Annual Singlefile
- Format : ZIP contenant des CSV annuels

Niveau d’agrégation choisi :
- `agglvl_code = 73`
→ Données État + Industrie (équilibre optimal entre granularité et performance)

---
## Structure du projet
```

bls-cew-data-pipeline/
│
├── ingestion/
│  └── download_cew_data.py
│ 
├── exploration/
│  └── eda.py
│ 
├── transformation/
│ ├── cleaning.py
│ └── kpis.py
│
├── notebooks/
├── requirements.txt
├── README.md
└── .gitignore
```
---

## Étapes du pipeline

### 1. Ingestion (Automatisation)

- Téléchargement automatique des fichiers ZIP annuels
- Extraction des fichiers `.annual*.csv`
- Stockage dans la couche Bronze

Fichier : ingestion/download_cew_data.py


---

### 2. EDA Bronze

Analyse des données brutes :

- Schéma des colonnes
- Nombre total de lignes
- Détection des valeurs nulles
- Vérification des doublons
- Détection des valeurs négatives

Fichier : exploration/eda.py


---

### 3. Nettoyage Silver (Delta Lake)

Transformations appliquées :

- Normalisation des noms de colonnes
- Suppression des colonnes `disclosure_code`
- Filtrage `agglvl_code = 73`
- Création de `sector_type` :
  - Private (own_code = 1)
  - Public (own_code = 2,3,5)
- Typage des colonnes
- Suppression des doublons
- Écriture en format Delta partitionné par année


Fichier : Stockage : dbfs:/Volumes/bls_cew/silver/silver_cew/cleaned_data


---

### 4. Calcul des KPIs (Gold – Pandas)

Lecture des données Silver via Spark puis conversion en Pandas.

KPIs calculés :

1. Emploi total par année et secteur
2. Salaires totaux par année et secteur
3. Salaire moyen annuel
4. Croissance annuelle (OTY %)
5. Top industries par emploi

Fichier : transformation/kpis.py

---

## Exécution sur Databricks

### Étape 1 : Cloner le repo via Databricks Repos

Repos → Add Repo → GitHub

### Étape 2 : Lancer les scripts

```python
%run ./src/ingestion/download_cew_data
%run ./src/exploration/eda
%run ./src/transformation/silver_cleaning
%run ./src/transformation/kpis
```
---
### 5. Technologies utilisées
- Python
- PySpark
- Databricks
- Delta Lake
- Pandas

