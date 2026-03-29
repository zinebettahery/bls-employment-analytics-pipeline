
----------------------------------------PAGE 1: EMPLOYMENT TRENDS----------------------------------------
-- Emploi total 
SELECT SUM(annual_avg_emplvl)
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private','Public')

-- Évolution de l’emploi par année et par secteur (Public vs Privé)
SELECT 
  year,
  sector_type,
  SUM(annual_avg_emplvl) AS total_employment
FROM bls_cew.silver.cleaned_data
GROUP BY year, sector_type
ORDER BY year, sector_type;

-- Taux de croissance annuel moyen de l’emploi (%)
SELECT 
  year,
  sector_type,
  AVG(oty_annual_avg_emplvl_pct_chg) AS employment_growth_pct
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private', 'Public')
GROUP BY year, sector_type
ORDER BY year;

-- Répartition de l’emploi par secteur (année la plus récente)
SELECT 
  sector_type,
  SUM(annual_avg_emplvl) AS total_emp
FROM bls_cew.silver.cleaned_data
WHERE year = (SELECT MAX(year) FROM bls_cew.silver.cleaned_data)
  AND sector_type IN ('Private','Public')
GROUP BY sector_type

-- Nombre total d’établissements par année et par secteur
SELECT
  year, sector_type,
  SUM(annual_avg_estabs) AS total_estabs
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private','Public')
GROUP BY year, sector_type
ORDER BY year, sector_type

-- Emploi total dans le secteur privé
SELECT 
  SUM(annual_avg_emplvl) AS private_employment
FROM bls_cew.silver.cleaned_data
WHERE sector_type = 'Private'

-- Emploi total dans le secteur public
SELECT 
  SUM(annual_avg_emplvl) AS public_employment
FROM bls_cew.silver.cleaned_data
WHERE sector_type = 'Public'

-- Salaire annuel moyen par année et par secteur
SELECT 
  year,
  sector_type,
  ROUND(AVG(avg_annual_pay), 2) AS avg_salary
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private', 'Public')
GROUP BY year, sector_type
ORDER BY year, sector_type


----------------------------------------PAGE 2: SALARY TRENDS----------------------------------------

-- Salaire moyen annuel – secteur privé
SELECT 
  ROUND(AVG(avg_annual_pay), 2) AS avg_salary_private
FROM bls_cew.silver.cleaned_data
WHERE sector_type = 'Private'

-- Salaire moyen annuel – secteur public
SELECT 
  ROUND(AVG(avg_annual_pay), 2) AS avg_salary_public
FROM bls_cew.silver.cleaned_data
WHERE sector_type = 'Public'

-- Écart salarial entre secteur privé et public (Wage Gap)
SELECT 
  ROUND(
    AVG(CASE WHEN sector_type='Private' THEN avg_annual_pay END) -
    AVG(CASE WHEN sector_type='Public' THEN avg_annual_pay END),
    2
  ) AS wage_gap
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private','Public')

-- Évolution du salaire moyen par année et par secteur
SELECT 
  year,
  sector_type,
  ROUND(AVG(avg_annual_pay), 2) AS avg_salary
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private', 'Public')
GROUP BY year, sector_type
ORDER BY year, sector_typeA

-- Taux de croissance annuel des salaires (%)
SELECT 
  year,
  sector_type,
  ROUND(AVG(oty_avg_annual_pay_pct_chg), 2) AS wage_growth_pct
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private', 'Public')
GROUP BY year, sector_type
ORDER BY year, sector_type

-- Top industries par salaire moyen (année récente)
SELECT industry_name,
AVG(avg_annual_pay) AS avg_salary
FROM bls_cew.silver.cleaned_data
WHERE year = (SELECT MAX(year) FROM bls_cew.silver.cleaned_data)
GROUP BY industry_name
ORDER BY avg_salary DESC
LIMIT 10

--------------------------------PAGE 3: INDUSTRY ANALYSIS----------------------------------------

-- Nombre total d’industries distinctes
SELECT 
  COUNT(DISTINCT industry_name) AS nb_industries
FROM bls_cew.silver.cleaned_data
WHERE industry_name IS NOT NULL


-- Industrie avec le plus faible niveau d’emploi
SELECT industry_name
FROM bls_cew.silver.cleaned_data
WHERE industry_name IS NOT NULL
GROUP BY industry_name
ORDER BY SUM(annual_avg_emplvl) ASC
LIMIT 1

-- Industrie avec le plus fort niveau d’emploi
SELECT industry_name
FROM bls_cew.silver.cleaned_data
WHERE industry_name IS NOT NULL
GROUP BY industry_name
ORDER BY SUM(annual_avg_emplvl) DESC
LIMIT 1

-- Top 15 industries par emploi total (année récente)
SELECT
  industry_code,
  industry_name,
  sector_type,
  SUM(annual_avg_emplvl)           AS total_employment,
  ROUND(AVG(avg_annual_pay), 0)      AS avg_pay,
  ROUND(
    SUM(annual_avg_emplvl) * 100.0 /
    SUM(SUM(annual_avg_emplvl)) OVER()
  , 1)                                 AS pct_total
FROM bls_cew.silver.cleaned_data
WHERE year = (SELECT MAX(year) FROM bls_cew.silver.cleaned_data)
  AND sector_type IN ('Private', 'Public')
  AND is_disclosed = true
GROUP BY industry_code, industry_name, sector_type
ORDER BY total_employment DESC
LIMIT 15


-------------------------------------- PAGE 4: COVID-19 IMPACT ANALYSIS--------------------------------------
-- Évolution de l’emploi selon les périodes COVID
SELECT
  covid_period,
  SUM(annual_avg_emplvl) AS total_emp
FROM bls_cew.silver.cleaned_data
GROUP BY covid_period
ORDER BY covid_period

-- Évolution de l’emploi pendant la période COVID (2018–2022)
SELECT
  year,
  sector_type,
  SUM(annual_avg_emplvl) AS emp
FROM bls_cew.silver.cleaned_data
WHERE year BETWEEN 2018 AND 2022
  AND sector_type IN ('Private', 'Public')
GROUP BY year, sector_type
ORDER BY year, sector_type

WITH yearly AS (
  SELECT
    industry_code,
    industry_name,
    year,
    SUM(annual_avg_emplvl) AS emp
  FROM bls_cew.silver.cleaned_data
  WHERE year IN (2019, 2020)
    AND industry_name IS NOT NULL
  GROUP BY industry_code, industry_name, year
),
pivoted AS (
  SELECT
    industry_code,
    industry_name,
    MAX(CASE WHEN year = 2019 THEN emp END) AS emp_2019,
    MAX(CASE WHEN year = 2020 THEN emp END) AS emp_2020
  FROM yearly
  GROUP BY industry_code, industry_name
)

-- Top industries les plus impactées par le COVID (baisse d’emploi)
SELECT
  industry_code,
  industry_name,
  emp_2019,
  emp_2020,
  ROUND(((emp_2020 - emp_2019) / emp_2019) * 100, 2) AS drop_pct
FROM pivoted
WHERE emp_2019 IS NOT NULL
  AND emp_2020 IS NOT NULL
ORDER BY drop_pct ASC
LIMIT 10

WITH data AS (
  SELECT year, SUM(annual_avg_emplvl) AS emp
  FROM bls_cew.silver.cleaned_data
  GROUP BY year
)

-- Variation globale de l’emploi entre 2019 et 2020 (%)
SELECT
  ROUND(
    ((MAX(CASE WHEN year=2020 THEN emp END) -
      MAX(CASE WHEN year=2019 THEN emp END))
    / MAX(CASE WHEN year=2019 THEN emp END)) * 100,
    2
  ) AS drop_pct
FROM data



-------------------------------------- PAGE 5: INDUSTRY GROWTH ANALYSIS--------------------------------------
-- Croissance de l’emploi et salaire moyen par industrie
SELECT industry_name,
AVG(avg_annual_pay) AS salary,
AVG(oty_annual_avg_emplvl_pct_chg) AS growth
FROM bls_cew.silver.cleaned_data
GROUP BY industry_name

-- Top industries avec la plus forte croissance d’emploi
SELECT industry_name,
AVG(oty_annual_avg_emplvl_pct_chg) AS growth
FROM bls_cew.silver.cleaned_data
GROUP BY industry_name
ORDER BY growth DESC
LIMIT 10

-- Industries avec la plus faible croissance d’emploi
SELECT industry_name,
AVG(oty_annual_avg_emplvl_pct_chg) AS growth
FROM bls_cew.silver.cleaned_data
GROUP BY industry_name
ORDER BY growth ASC
LIMIT 10

-- Industrie avec la croissance maximale
SELECT industry_name
FROM bls_cew.silver.cleaned_data
WHERE industry_name IS NOT NULL
GROUP BY industry_name
ORDER BY AVG(oty_annual_avg_emplvl_pct_chg) DESC
LIMIT 1

-- Industrie avec la croissance minimale
SELECT industry_name
FROM bls_cew.silver.cleaned_data
WHERE industry_name IS NOT NULL
GROUP BY industry_name
ORDER BY AVG(oty_annual_avg_emplvl_pct_chg) ASC
LIMIT 1


-- Croissance moyenne globale de l’emploi
SELECT
  ROUND(AVG(oty_annual_avg_emplvl_pct_chg),2) AS avg_growth
FROM bls_cew.silver.cleaned_data

