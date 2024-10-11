-- DATA CLEANING OF LAPTOP DATASET USING MYSQL

-- What I did:

-- Checked if the table had duplicate values.
-- Removed missing values in the "Unnamed: 0" column by sequencing the IDs.
-- Rounded off the values in the "Price" column to two decimal places.

SELECT *
FROM laptopdata;

-- Duplicating Raw Data 
CREATE TABLE laptopdata_staging
LIKE laptopdata;

SELECT * 
FROM laptopdata_staging;

INSERT INTO laptopdata_staging
SELECT * 
FROM laptopdata;

-- Remove Duplicates 
SELECT *,
ROW_NUMBER() OVER(PARTITION BY `Unnamed: 0`, 
	Company, 
	TypeName, 
	Inches, 
	ScreenResolution,
	`Cpu`, 
	Ram, 
	`Memory`, 
	Gpu, 
	OpSys, 
	Weight, 
	Price) AS Row_Num
FROM laptopdata_staging;

WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY 
	`Unnamed: 0`, 
	Company, 
	TypeName, 
	Inches, 
	ScreenResolution,
	`Cpu`, 
	Ram, 
	`Memory`, 
	Gpu, 
	OpSys, 
	Weight, 
	Price) AS row_num
FROM laptopdata_staging
) 
SELECT * 
FROM duplicate_CTE
WHERE row_num > 1;

-- Generating new number series in unnamed: 0 because it has missing values

CREATE TEMPORARY TABLE numbers (num INT);

INSERT INTO numbers (num)
WITH RECURSIVE sequence AS (
    SELECT 0 AS num
    UNION ALL
    SELECT num + 1
    FROM sequence
    WHERE num < (SELECT MAX(`Unnamed: 0`) FROM laptopdata_staging)
)
SELECT num FROM sequence;

SELECT num AS missing_id
FROM numbers
LEFT JOIN laptopdata_staging ON numbers.num = laptopdata_staging.`Unnamed: 0`
WHERE laptopdata_staging.`Unnamed: 0` IS NULL;

CREATE TABLE cleaned_laptopdata AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY `Unnamed: 0`) - 1 AS new_id, 
    Company, 
    TypeName, 
    Inches, 
    ScreenResolution,
    `Cpu`, 
    Ram, 
    `Memory`, 
    Gpu, 
    OpSys, 
    Weight, 
    Price
FROM 
    laptopdata_staging;

-- Add a new column for the new ID
ALTER TABLE laptopdata_staging ADD COLUMN new_id INT;

-- Update the new column with the new IDs
SET @row_number = 0;
UPDATE laptopdata_staging
SET new_id = (@row_number:=@row_number + 1) - 1
ORDER BY `Unnamed: 0`;

-- Dropping the old ID column
ALTER TABLE laptopdata_staging DROP COLUMN `Unnamed: 0`;

-- Standardizing the data 
-- Rounding off the Price values

SELECT ROUND(Price,2) AS Round_off_Price
FROM cleaned_laptopdata;

UPDATE cleaned_laptopdata
SET Price = ROUND(Price, 2);

SELECT * 
FROM cleaned_laptopdata;

-- Ensuring that the data sets are completely cleaned

-- Checking if there is no null values in the table
SELECT *
FROM cleaned_laptopdata
WHERE new_id IS NULL
   OR Company IS NULL
   OR TypeName IS NULL
   OR Inches IS NULL
   OR ScreenResolution IS NULL
   OR `Cpu` IS NULL
   OR Ram IS NULL
   OR `Memory` IS NULL
   OR Gpu IS NULL
   OR OpSys IS NULL
   OR Weight IS NULL
   OR Price IS NULL;

-- Checking if there is no duplicate data inserted
SELECT new_id, Company, TypeName, Inches, ScreenResolution, `Cpu`, Ram, `Memory`, Gpu, OpSys, Weight, Price, COUNT(*)
FROM cleaned_laptopdata
GROUP BY new_id, Company, TypeName, Inches, ScreenResolution, `Cpu`, Ram, `Memory`, Gpu, OpSys, Weight, Price
HAVING COUNT(*) > 1;

-- Checking if data has a correct data types
SELECT *
FROM cleaned_laptopdata
WHERE NOT (Inches REGEXP '^[0-9]+([.][0-9]+)?$')
   OR NOT (Ram REGEXP '^[0-9]+$')
   OR NOT (Weight REGEXP '^[0-9]+([.][0-9]+)?$')
   OR NOT (Price REGEXP '^[0-9]+([.][0-9]+)?$');

-- Checking if there is an outliers in Price column
SELECT *
FROM cleaned_laptopdata
WHERE Price > (SELECT AVG(Price) + 3 * STDDEV(Price) FROM cleaned_laptopdata)
   OR Price < (SELECT AVG(Price) - 3 * STDDEV(Price) FROM cleaned_laptopdata);

-- Checking if the data has no negitive values
SELECT *
FROM cleaned_laptopdata
WHERE Inches < 0
   OR Ram < 0
   OR Weight < 0
   OR Price < 0;

SELECT Company, COUNT(*)
FROM cleaned_laptopdata
GROUP BY Company
;

SELECT MAX(Cpu)
FROM cleaned_laptopdata;
