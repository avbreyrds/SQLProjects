-- DATA CLEANING OF CAR SALES DATASET USING MYSQL

-- What I did:

-- Removed duplicates.
-- Trimmed the column to remove whitespaces and extra spaces.
-- Removed null values and blank cells.
-- Removed outliers in the "Year" column.
-- Removed the "RowNumber" column used for removing duplicates.
-- Rounded off the values in the "Sales" column to two decimal places.
-- Renamed the "Make" column to "Brand."
-- Capitalized the first letter of each word in the "Brand" column.

SELECT *
FROM car_sales_data;

SELECT *
FROM car_sales_data_original;

-- REMOVING DUPLICATES

WITH duplicate_row AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY 
Make, 
Model,
`Year`, 
Sales) rownumber
FROM car_sales_data_original)
SELECT *
FROM duplicate_row
WHERE rownumber > 1;

-- CREATING NEW TABLE

CREATE TABLE `car_sales_data_original_staging` (
  `Make` text,
  `Model` text,
  `Year` text,
  `Sales` double DEFAULT NULL,
  `RowNumber` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM car_sales_data_original_staging;

INSERT INTO car_sales_data_original_staging
SELECT *,
ROW_NUMBER() OVER(PARTITION BY Make,
Model,
`Year`,
Sales) AS RowNumber
FROM car_sales_data_original;

SELECT *
FROM car_sales_data_original_staging
WHERE RowNumber > 1;

DELETE 
FROM car_sales_data_original_staging
WHERE RowNumber > 1;

-- STANDARDIZING THE DATA

-- TRIMING COLUMN
-- (MAKE)
SELECT Make, TRIM(Make)
FROM car_sales_data_original_staging;

UPDATE car_sales_data_original_staging
SET Make = TRIM(Make);

-- (MODEL)
SELECT Model, TRIM(Model)
FROM car_sales_data_original_staging;

UPDATE car_sales_data_original_staging
SET Model = TRIM(Model);

SELECT DISTINCT Model, TRIM(TRAILING '.' FROM Model)
FROM car_sales_data_original_staging
ORDER BY 1;

UPDATE car_sales_data_original_staging
SET Model = TRIM(TRAILING '.' FROM Model);

-- (Year)
SELECT `Year`, TRIM(`Year`)
FROM car_sales_data_original_staging;

UPDATE car_sales_data_original_staging
SET `Year` = TRIM(`Year`);

-- (Sales)
SELECT Sales, TRIM(Sales)
FROM car_sales_data_original_staging;

UPDATE car_sales_data_original_staging
SET Sales = TRIM(Sales);

SELECT *
FROM car_sales_data_original_staging;

-- REMOVING NULL VALUES AND BLANK CELLS
-- (Make)
SELECT * 
FROM car_sales_data_original_staging
WHERE Make is null 
OR Make = "";
-- (Model)
SELECT * 
FROM car_sales_data_original_staging
WHERE Model is null 
OR Model = "";
-- (Year)
SELECT * 
FROM car_sales_data_original_staging
WHERE `Year` is null 
OR `Year` = "";

DELETE
FROM car_sales_data_original_staging
WHERE `Year` is null 
OR `Year` = "";
-- (SALES)
SELECT * 
FROM car_sales_data_original_staging
WHERE Sales is null 
OR Sales = "";

-- REMOVING OUTLIERS
SELECT *
FROM car_sales_data_original_staging;

SELECT *
FROM car_sales_data_original_staging
WHERE NOT (`Year` BETWEEN 1900 AND 2099);

DELETE 
FROM car_sales_data_original_staging
WHERE NOT (`Year` BETWEEN 1900 AND 2099);

-- ALTERING THE COLUMN NAME OF (MAKE) TO BRAND
ALTER TABLE car_sales_data_original_staging
CHANGE COLUMN Make Brand varchar(255);

-- DELETING ROWNUMBER COLUMN 
ALTER TABLE car_sales_data_original_staging 
DROP COLUMN RowNumber;

-- CAPITALIZING THE FIRST LETTER OF THE BRAND COLUMN
SELECT CONCAT(UPPER(SUBSTRING(Brand, 1, 1)), LOWER(SUBSTRING(Brand, 2))) AS CapitalizedColumn
FROM car_sales_data_original_staging;

UPDATE car_sales_data_original_staging
SET Brand = CONCAT(UPPER(SUBSTRING(Brand, 1, 1)), LOWER(SUBSTRING(Brand, 2)));

-- ROUNDING OF THE VALUE OF SALES 
SELECT ROUND(Sales, 2) as RoundOff
FROM car_sales_data_original_staging;

UPDATE car_sales_data_original_staging
SET Sales = ROUND(Sales, 2);

-- CHECKING IF THE CODE WORKS 
SELECT Sales
FROM car_sales_data_original_staging
WHERE Sales <> ROUND(Sales, 2);