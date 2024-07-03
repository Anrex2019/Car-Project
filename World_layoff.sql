SELECT * FROM World_layoff.layoffs;

-- Deleting duplicates
-- Creating a staging table to clean the data 
Create table layoffs_staging like layoffs;
select * from layoff_staging;

insert layoffs_staging select * from layoffs;

Select *,
Row_number() OVER( 
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging;

WITH duplicate_cte AS (
    SELECT *,
        ROW_NUMBER() OVER ( 
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
        ) AS row_num
    FROM layoffs_staging
) 
SELECT *
FROM duplicate_cte
WHERE row_num > '1';

select * from layoffs_staging where company = 'Oyster';

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
  `row_num`INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * From layoffs_staging2;

insert into layoffs_staging2 select * ,
ROW_NUMBER() OVER ( 
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
        ) AS row_num from layoffs_staging;
        
select * from layoffs_staging2 where row_num > 1;
SET SQL_SAFE_UPDATES = 0;

delete from layoffs_staging2 where row_num > 1;

-- Standardizing Data
select * from layoffs_staging2;

select distinct (Trim(company)) from layoffs_staging2;
select company, Trim(company) from layoffs_staging2;

update layoffs_staging2 set company = TRIM(company);

select distinct industry from layoffs_staging2 order by 1;

select * from layoffs_staging2 where industry like 'Crypto%';
Update layoffs_staging2 set industry = 'Crypto' where industry like 'Crypto%';
-- There were multiple variations of the industry with the name crypto so we updated, so that  all three are named after crypto
select distinct location from layoffs_staging2 order by 1;
select distinct country from layoffs_staging2 order by 1;

select distinct country, Trim(trailing '.' from country) from layoffs_staging2 order by 1;
-- We found United states in 2 different variations where one of them states 'United states.' by using trim and trailing I removed the '.' and updated the table.
select * from layoffs_staging2 where country like 'united states%';
Update layoffs_staging2 set country =Trim(trailing '.' from country);


select distinct country from layoffs_staging2 order by 1;

select date from layoffs_staging2;
-- We can use Str to update and fix the date columns as well as converting the data type propery by using modify and alter table.
select date,
STR_TO_DATE(date, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set date = STR_TO_DATE(date, '%m/%d/%Y');

Alter table layoffs_staging2 modify column date Date;

select * from layoffs_staging2;

-- Looking at NULL values and removing those rows that we have no use for. 
-- Also deleting the row_num coloumn as we don't need it anymore as we deleted the duplicates

select * from layoffs_staging2 where total_laid_off IS NULL and percentage_laid_off IS NULL;


select *  from layoffs_staging2 where industry is NULL or industry = ' '; 
Delete  from layoffs_staging2 where total_laid_off IS NULL and percentage_laid_off IS NULL;

select * from layoffs_staging2;

Alter table layoffs_staging2 drop column row_num;
select * from layoffs_staging2;

-- Expolratory Data Analysis (EDA)
select * from layoffs_staging2;
-- Most total laid off and the highest percentage laid offf. 
select MAX(total_laid_off), MAX(percentage_laid_off) from layoffs_staging2;
-- Order by most total laid off whilst having 100% laid off
select * from layoffs_staging2 where percentage_laid_off = 1 order by total_laid_off desc;
-- Order by funds raised millions whilst having 100% laid off
select * from layoffs_staging2 where percentage_laid_off = 1 order by funds_raised_millions desc;
-- Create 2 coloumns with company and the sum of total laid per company. Number 2 orders it by the second column. 
Select company, Sum(total_laid_off)
from layoffs_staging2 group by company order by 2 desc;
-- Show case the earliest and latest date
select min(date), max(date) from layoffs_staging2;
-- What industry got hit the most with the most layoffs in this data set.
Select industry, Sum(total_laid_off)
from layoffs_staging2 group by industry order by 2 desc;
-- What country got hit the most with the most layoffs in this dataset.
Select country, Sum(total_laid_off)
from layoffs_staging2 group by country order by 2 desc;
-- Show what year got hit with the most layoffs in this data set
Select YEAR(date), Sum(total_laid_off)
from layoffs_staging2 group by YEAR(date) order by 2 desc;
-- Show what the sum of total_laid_off per month in year
select substring(date,1,7) as 'Month', SUM(total_laid_off) FROM layoffs_staging2 where Substring(date,1,7)  is not NULL
GROUP BY substring(date,1,7) ORDER BY 1 ASC;
-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, total_laid_off, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC; 












