# BLS CEW Data Pipeline (Bronze → Silver → Gold → ML)

Pipeline Data Engineering de bout en bout construit sur les données **BLS QCEW / CEW Annual Singlefile** (2015–2024/2025 selon disponibilité).

Ce projet met en place une architecture Data Lake moderne et reproductible permettant de transformer des données brutes volumineuses (36M+ lignes) en indicateurs analytiques et en modèles prédictifs.

Le pipeline couvre :

- L’ingestion automatisée et l’extraction des données BLS
- L’analyse exploratoire (EDA) et le contrôle qualité des données
- La transformation vers une couche Silver structurée (Delta Lake)
- La construction d’indicateurs analytiques (comparaison Public vs Privé)
- L’analyse des tendances du marché du travail (croissance, salaires, chocs économiques)
- L’étude des transformations sectorielles liées à l’automatisation et à l’intelligence artificielle
-  L’implémentation d’un modèle de Machine Learning pour la prévision de l’emploi


Ce projet combine Data Engineering, Data Analysis et Machine Learning dans un environnement Databricks basé sur Spark et Delta Lake.

---

## Objectif du projet

L’objectif est de concevoir une pipeline Data Engineering complète exploitant les données publiques du BLS, depuis l’ingestion brute jusqu’à l’analyse avancée et la modélisation prédictive.

Plus précisément, le projet vise à :

- Structurer un Data Lake en couches Bronze / Silver / Gold
- Mettre en place des règles de nettoyage et de standardisation robustes
- Produire des KPIs pertinents sur l’évolution de l’emploi et des salaires
- Comparer les dynamiques entre secteur privé et secteur public
- Analyser les chocs macroéconomiques (ex : COVID-19)
- Explorer les tendances sectorielles pouvant refléter l’impact croissant de l’IA
- Développer un modèle prédictif pour anticiper l’évolution future de l’emploi

Ce projet démontre la capacité à :

- Manipuler des volumes importants de données avec Spark
- Concevoir une architecture data scalable et reproductible
- Transformer des données brutes en insights décisionnels
- Intégrer l’analyse prédictive dans une démarche d’ingénierie des données

En résumé, il illustre une approche intégrée combinant ingénierie des données, analyse économique et modélisation prédictive.

---

## Architecture
<img width="1024" height="541" alt="image" src="https://github.com/user-attachments/assets/0915df9a-96ce-46d7-b343-d15868718783" />


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
%run ./ingestion/download_cew_data
%run ./exploration/eda
%run ./transformation/silver_cleaning
%run ./transformation/kpis
```
---
### 5. Technologies utilisées
- Python
- PySpark
- Databricks
- Delta Lake
- Pandas
- Jira & Confluence

