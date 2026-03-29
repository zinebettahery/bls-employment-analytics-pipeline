
----------------------------------------PAGE 1: EMPLOYMENT TRENDS----------------------------------------
-- Total employment 
SELECT SUM(annual_avg_emplvl)
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private','Public')

-- Total employment by year and sector type
SELECT 
  year,
  sector_type,
  SUM(annual_avg_emplvl) AS total_employment
FROM bls_cew.silver.cleaned_data
GROUP BY year, sector_type
ORDER BY year, sector_type;

-- Average annual percentage change in employment by year and sector type
SELECT 
  year,
  sector_type,
  AVG(oty_annual_avg_emplvl_pct_chg) AS employment_growth_pct
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private', 'Public')
GROUP BY year, sector_type
ORDER BY year;

-- Total employment by sector type for the most recent year
SELECT 
  sector_type,
  SUM(annual_avg_emplvl) AS total_emp
FROM bls_cew.silver.cleaned_data
WHERE year = (SELECT MAX(year) FROM bls_cew.silver.cleaned_data)
  AND sector_type IN ('Private','Public')
GROUP BY sector_type

-- Total establishments by year and sector type
SELECT
  year, sector_type,
  SUM(annual_avg_estabs) AS total_estabs
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private','Public')
GROUP BY year, sector_type
ORDER BY year, sector_type

-- Average annual percentage change in establishments by year and sector type
SELECT 
  SUM(annual_avg_emplvl) AS private_employment
FROM bls_cew.silver.cleaned_data
WHERE sector_type = 'Private'

-- Average annual percentage change in establishments by year and sector type
SELECT 
  SUM(annual_avg_emplvl) AS public_employment
FROM bls_cew.silver.cleaned_data
WHERE sector_type = 'Public'

-- Average annual pay by year and sector type
SELECT 
  year,
  sector_type,
  ROUND(AVG(avg_annual_pay), 2) AS avg_salary
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private', 'Public')
GROUP BY year, sector_type
ORDER BY year, sector_type


----------------------------------------PAGE 2: SALARY TRENDS----------------------------------------

-- Average annual pay for private sector
SELECT 
  ROUND(AVG(avg_annual_pay), 2) AS avg_salary_private
FROM bls_cew.silver.cleaned_data
WHERE sector_type = 'Private'

-- Average annual pay for public sector
SELECT 
  ROUND(AVG(avg_annual_pay), 2) AS avg_salary_public
FROM bls_cew.silver.cleaned_data
WHERE sector_type = 'Public'

-- Wage gap between private and public sectors
SELECT 
  ROUND(
    AVG(CASE WHEN sector_type='Private' THEN avg_annual_pay END) -
    AVG(CASE WHEN sector_type='Public' THEN avg_annual_pay END),
    2
  ) AS wage_gap
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private','Public')

-- Average annual pay by year and sector type
SELECT 
  year,
  sector_type,
  ROUND(AVG(avg_annual_pay), 2) AS avg_salary
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private', 'Public')
GROUP BY year, sector_type
ORDER BY year, sector_typeA

-- Average annual percentage change in pay by year and sector type
SELECT 
  year,
  sector_type,
  ROUND(AVG(oty_avg_annual_pay_pct_chg), 2) AS wage_growth_pct
FROM bls_cew.silver.cleaned_data
WHERE sector_type IN ('Private', 'Public')
GROUP BY year, sector_type
ORDER BY year, sector_type

-- Average annual pay by industry for the most recent year
SELECT industry_name,
AVG(avg_annual_pay) AS avg_salary
FROM bls_cew.silver.cleaned_data
WHERE year = (SELECT MAX(year) FROM bls_cew.silver.cleaned_data)
GROUP BY industry_name
ORDER BY avg_salary DESC
LIMIT 10

--------------------------------PAGE 3: INDUSTRY ANALYSIS----------------------------------------

-- Number of unique industries in the dataset
SELECT 
  COUNT(DISTINCT industry_name) AS nb_industries
FROM bls_cew.silver.cleaned_data
WHERE industry_name IS NOT NULL


-- Industry with the highest total employment
SELECT industry_name
FROM bls_cew.silver.cleaned_data
WHERE industry_name IS NOT NULL
GROUP BY industry_name
ORDER BY SUM(annual_avg_emplvl) ASC
LIMIT 1

-- Industry with the lowest total employment
SELECT industry_name
FROM bls_cew.silver.cleaned_data
WHERE industry_name IS NOT NULL
GROUP BY industry_name
ORDER BY SUM(annual_avg_emplvl) DESC
LIMIT 1

-- Top 15 industries by total employment for the most recent year
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
-- Employment trends by COVID-19 period
SELECT
  covid_period,
  SUM(annual_avg_emplvl) AS total_emp
FROM bls_cew.silver.cleaned_data
GROUP BY covid_period
ORDER BY covid_period

-- Employment trends by year and sector type
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

-- Top 10 industries with the largest percentage drop in employment from 2019 to 2020
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

-- Percentage drop in total employment from 2019 to 2020
SELECT
  ROUND(
    ((MAX(CASE WHEN year=2020 THEN emp END) -
      MAX(CASE WHEN year=2019 THEN emp END))
    / MAX(CASE WHEN year=2019 THEN emp END)) * 100,
    2
  ) AS drop_pct
FROM data



-------------------------------------- PAGE 5: INDUSTRY GROWTH ANALYSIS--------------------------------------
-- Average annual pay and employment growth by industry
SELECT industry_name,
AVG(avg_annual_pay) AS salary,
AVG(oty_annual_avg_emplvl_pct_chg) AS growth
FROM bls_cew.silver.cleaned_data
GROUP BY industry_name

-- Industries with the highest average annual percentage change in employment
SELECT industry_name,
AVG(oty_annual_avg_emplvl_pct_chg) AS growth
FROM bls_cew.silver.cleaned_data
GROUP BY industry_name
ORDER BY growth DESC
LIMIT 10

-- Industries with the lowest average annual percentage change in employment
SELECT industry_name,
AVG(oty_annual_avg_emplvl_pct_chg) AS growth
FROM bls_cew.silver.cleaned_data
GROUP BY industry_name
ORDER BY growth ASC
LIMIT 10

-- Industry with the highest average annual percentage change in employment
SELECT industry_name
FROM bls_cew.silver.cleaned_data
WHERE industry_name IS NOT NULL
GROUP BY industry_name
ORDER BY AVG(oty_annual_avg_emplvl_pct_chg) DESC
LIMIT 1

-- Industry with the lowest average annual percentage change in employment
SELECT industry_name
FROM bls_cew.silver.cleaned_data
WHERE industry_name IS NOT NULL
GROUP BY industry_name
ORDER BY AVG(oty_annual_avg_emplvl_pct_chg) ASC
LIMIT 1


-- Average annual percentage change in employment across all industries
SELECT
  ROUND(AVG(oty_annual_avg_emplvl_pct_chg),2) AS avg_growth
FROM bls_cew.silver.cleaned_data

