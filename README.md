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
<img width="1536" height="1024" alt="ChatGPT Image 2 mars 2026, 21_16_41" src="https://github.com/user-attachments/assets/f73fdd17-bf8d-4620-8826-508b4df285ec" />


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
│ └── kpi_sql.sql
│
├── analysis
│  ├── statistical_validation.ipynb
│  └── BLS Labor Market Analytics Dashboard.Lvdash.json
│ 
├── ml/
│  └── prediction.py
├── requirements.txt
├── README.md
└── .gitignore
```
---

## Étapes du pipeline

### 1. Ingestion (Bronze)

- Téléchargement automatique des fichiers ZIP
- Extraction des fichiers CSV
- Stockage dans Databricks

📁 `ingestion/download_cew_data.py`

---

### 2. EDA (Exploration)

Analyse des données brutes :

- Vérification du schéma
- Analyse des nulls
- Détection des doublons
- Détection des valeurs négatives

📁 `exploration/eda.py`

---

### 3. Nettoyage (Silver - Delta Lake)

Transformations :

- Normalisation des colonnes
- Filtrage `agglvl_code = 73`
- Création `sector_type` (Private / Public)
- Typage des données
- Suppression des doublons
- Traitement des valeurs aberrantes
- Ajout de colonnes dérivées (naics2, covid_period)
- Jointure avec les noms des industries

📁 `transformation/silver_cleaning.py`  
📦 Stockage : `bls_cew.silver.cleaned_data`

---

### 4. Analyse & KPIs (SQL Layer)

KPIs calculés via SQL :

- Emploi total
- Salaires totaux
- Salaire moyen
- Croissance annuelle (%)
- Top industries

📁 `transformation/kpi_sql.sql`

---

### 5. Dashboard

Dashboard interactif Databricks :

- Vue d’ensemble
- Analyse salaires
- Top industries
- Impact COVID
- Impact AI

📁 `analysis/dashboard.json`

---

### 6. Machine Learning

Modèle de régression pour prédire l’emploi :

- Variable : année
- Cible : emploi total

📁 `ml/prediction.py`

---

## Exécution sur Databricks

### Étape 1 : Cloner le repo via Databricks Repos

Repos → Add Repo → GitHub

### Étape 2 : Lancer les scripts

```python
%run ./ingestion/download_cew_data
%run ./exploration/eda
%run ./transformation/cleaning
```
---
### 5. Technologies utilisées
- Python
- PySpark
- Databricks
- Delta Lake
- Pandas
- SQL
- Jira & Confluence

