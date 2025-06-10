-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT *
FROM layoffs;

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE layoffs_staging AS
SELECT *
FROM layoffs;
 
SELECT *
FROM layoffs_staging;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and blank values
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates

# First let's check for duplicates
WITH duplicates_cte AS
(SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging)
SELECT *
FROM duplicates_cte
WHERE row_num > 1;

#now creating new table layoffs_staging2 and adding values to actually delete duplicates values
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;



-- 2)Standardizing Data

SELECT DISTINCT(company)
FROM layoffs_staging2
ORDER BY 1;

SELECT company, Trim(company)
FROM layoffs_staging2;

#trimming the spaces from the records in company column
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT industry
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_staging2
SET industry ="Crypto"
WHERE industry like "Crypto%";

SELECT DISTINCT(location)
FROM layoffs_staging2
ORDER BY 1;				 #NO ISSUE

SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT(Country)
FROM layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET country ="United States"
WHERE country like "United States%";

#chaging date format from string to Date
UPDATE layoffs_staging2
SET `date`= STR_TO_DATE(`date`,"%m/%d/%Y");

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` Date;

SELECT `date`
FROM layoffs_staging2;



-- 3) Blank values

SELECT * 
FROM layoffs_staging2;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry= ""
ORDER BY industry;

#LETS SEE IN DETAIL
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%'; #NOTHING WRONG HERE
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';
-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

#CHECKING
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL OR industry = ''
ORDER BY industry;

# now we need to populate those nulls if possible
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company AND t1.location=t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;



-- 4) remove any columns and rows we need to
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2;















