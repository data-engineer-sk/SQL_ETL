CREATE DATABASE IF NOT EXISTS SQL_ETL;
USE SQL_ETL;
/*
SELECT TxDate, Trim(SUBSTRING_INDEX(Item,',',1)) AS Prod1, 
trim(SUBSTRING_INDEX(Item,',',-1)) AS Rest1 from InitialTable;
*/
-- Create the 2nd Table
CREATE TABLE TempTable (
	TxID INT AUTO_INCREMENT PRIMARY KEY,
	TxDate VARCHAR(40),
    Store_Name VARCHAR(100),
    Cust_Name VARCHAR(100),
    Item MEDIUMTEXT,
    TotPrice FLOAT,
    Payment VARCHAR(20),
    CardNo VARCHAR(20)
);

CREATE TABLE SecondTable (
	TxID INT,
	TxDate VARCHAR(40),
    Store_Name VARCHAR(100),
    Cust_Name VARCHAR(100),
    Item MEDIUMTEXT,
    TotPrice FLOAT,
    Payment VARCHAR(20),
    CardNo VARCHAR(20)
);

-- Make sure the TempTable have all data from InitialTable
INSERT INTO TempTable (TxDate, Store_Name, Cust_Name, Item, TotPrice, Payment, CardNo)
SELECT TxDate, Store_Name, Cust_Name, Item, TotPrice, Payment, CardNo FROM InitialTable;

-- Convert TempTable into SecondTable have all data we want for cleaning
INSERT INTO SecondTable (TxID, TxDate, Store_Name, Cust_Name, Item, TotPrice, Payment, CardNo)
SELECT TxID, TxDate, Store_Name, Cust_Name, Item, TotPrice, Payment, CardNo FROM TempTable;

DROP TABLE TempTable;
DROP TABLE TempCustTable;
/*
DROP TABLE SecondTable;
*/
Select * from TempTable;
select * from SecondTable;
Select * from InitialTable;

/* Create a CustomerTable with the SecondTable to droup the duplicate!!!!!
CREATE TABLE CustomerTable SELECT Distinct Cust_Name, CardNo from SecondTable; 
*/
CREATE TABLE TempCustTable SELECT TxID, Cust_Name, CardNo from SecondTable;

-- Remove the duplicate Rows from Customer
-- CREATE TABLE CustomerTable SELECT Distinct Cust_Name, CardNo from TempCustTable; 

Select * from SecondTable;

Select * from TempTable;
Select * from CustomerTable;

Select Item from InitialTable;

-- Slicing the products.  Split single column into multiple columns
CREATE TABLE TempProdTable_v0 SELECT TxID, Store_Name, Cust_Name, 
		SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',1),',',-1) AS Prod1, 
		SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',2),',',-1) AS Prod2,
        SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',3),',',-1) AS Prod3,
        SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',4),',',-1) AS Prod4,
        SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',5),',',-1) AS Prod5,
        SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',6),',',-1) AS Prod6,
		SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',7),',',-1) AS Prod7,
		SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',8),',',-1) AS Prod8,
		SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',9),',',-1) AS Prod9,
		SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',10),',',-1) AS Prod10,
		SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',11),',',-1) AS Prod11,
		SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',12),',',-1) AS Prod12, 
		SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',13),',',-1) AS Prod13,
		SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',14),',',-1) AS Prod14,
		SUBSTRING_INDEX(SUBSTRING_INDEX(Item,',',15),',',-1) AS Prod15        
FROM SecondTable;

CREATE TABLE TempProdTable_v1 SELECT * from TempProdTable_v0;

-- Count the total columns in the table "TempProdTable"
SELECT count(*) AS NO_OF_COLUMNS FROM information_schema.columns
WHERE table_name ="TempProdTable_v1";

Select * from TempProdTable_v1;

-- Transpose x/y columns to form the productable and remove the duplication Columns
-- And Transpose to corrected number of rows!!!!
CREATE TABLE OrderDetailTable
(select TxID, Store_Name, Cust_Name, Trim(Prod1) as Prod_Name from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod2) as Prod_Name from TempProdTable_v1
union  
select TxID, Store_Name, Cust_Name, Trim(Prod3) as Prod_Name from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod4) as Prod_Name from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod5) as Prod_Name from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod6) as Prod_Name from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod7) as Prod_Name from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod8) as Prod_Name from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod9) as Prod_Name  from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod10) as Prod_Name from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod11) as Prod_Name  from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod12) as Prod_Name  from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod13) as Prod_Name  from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod14) as Prod_Name  from TempProdTable_v1
union
select TxID, Store_Name, Cust_Name, Trim(Prod15) as Prod_Name  from TempProdTable_v1)
Order by TxID;

-- Clean the Product. 90% WORK!!!!
CREATE TABLE ProductTable_v0
SELECT TxID, Trim(Substring_index(Prod_Name,' ',1)) as Size, 
		Left(Substring_index(Substring_index(Prod_Name,'Large',-1),'Regular',-1),Length(Prod_Name) - 12) as Prod_Name,
        SUBSTRING_INDEX(Prod_Name,' ', -1) as Price
from OrderDetailTable;

SELECT * from ProductTable_v0;
Select * from OrderDetailTable;

-- Create the ProductTable_V1, 99% clean and in order
CREATE TABLE ProductTable_v1
Select TxID, Size, SUBSTR(Prod_Name, 1, length(Prod_Name)-1) as Prod_Name, Price as UnitPrice
from ProductTable_v0
Where SUBSTR(Prod_Name, -1, 1) = '-' 
union
Select TxID, Size, Prod_Name, Price as UnitPrice
from ProductTable_v0
Where SUBSTR(Prod_Name, -1, 1) != '-';

-- Create the ProductTable_v2, 100% clean and ordered
CREATE TABLE ProductTable_v2
Select * from ProductTable_v1
Order by TxID;

-- Filter out the duplicate rows and stored into ProductTable_v3, that's it!
CREATE TABLE ProductTable_v3
SELECT Size, Prod_Name, UnitPrice FROM ProductTable_v2
GROUP BY Size, Prod_Name, UnitPrice;

Select * from ProductTable_v3;

--  klf;kjfdas;lfdjkf;dkajf;dskfjad;lfkjd;fljdaf;kdjf;lkdsjaf;ldjkfd;safj
USE SQL_ETL;
CREATE TABLE ProductTable_v4 (
    TempNo INT,
	Size VARCHAR(10),
	ProdName VARCHAR(100),
    UnitPrice DECIMAL(6,2)
);

INSERT INTO ProductTable_v4 (TempNo, Size, ProdName, UnitPrice)
SELECT row_number() over (ORDER BY Prod_Name ASC) TempNo,
	Size,
    Prod_Name,
    UnitPrice
FROM ProductTable_v3;

Select * from ProductTable_v4;

-- Finalize the ProdTable
CREATE TABLE ProductTable (
    ProdNo CHAR(10),
	Size VARCHAR(10),
	ProdName VARCHAR(100),
    UnitPrice DECIMAL(6,2)
);

INSERT INTO ProductTable
SELECT CONCAT("PRD",CONVERT(TempNo, CHAR)), Size, ProdName, UnitPrice
FROM ProductTable_v4;

Select * from ProductTable;

select * from ProductTable_v2;

ALTER TABLE ProductTable_v2
ADD COLUMN ProdNo CHAR(10) AFTER Size;

# Make a copy of ProductTable_v2 for futther processing
CREATE TABLE ProductTable_v22
SELECT * FROM ProductTable_v2;

Select * from ProductTable;

# Update the ProdNo to the ProductTable_v22.
# This table will be convertted to OrderDetailTable in future
USE SQL_ETL;
UPDATE ProductTable_v22
SET ProductTable_v22.ProdNo = (SELECT ProductTable.ProdNo FROM ProductTable
WHERE ProductTable.Size = ProductTable_v22.Size
And ProductTable.ProdName = ProductTable_v22.Prod_Name
And ProductTable.UnitPrice = ProductTable_v22.UnitPrice);

-- Examine the ProdNo in the ProductTable_v22
Select * from ProductTable_v22;

Select * from OrderDetailTable;
Select * from TempTable;
select * from InitialTable;
# Format the OrderDetailTable_v1 based on ProductTable_v22
USE SQL_ETL;
CREATE TABLE OrderDetailTable_v1
SELECT * FROM ProductTable_v22;

ALTER TABLE OrderDetailTable_v1
ADD COLUMN OrderNo CHAR(10) AFTER TxID;

SELECT * FROM OrderDetailTable_v1;

-- Update the OrderNo to format : 'ORD...'
UPDATE OrderDetailTable_v1
SET OrderNo = CONCAT("ORD",CONVERT(TxID, CHAR));

-- Create the RecNo in OrderDetailTable_v1
ALTER TABLE OrderDetailTable_v1
ADD COLUMN RecNo CHAR(10) AFTER TxID;

SELECT * FROM OrderDetailTable_v1;

-- Convert the OrderDetailTable_v1 to OrderDetailTable_v2
CREATE TABLE OrderDetailTable_v2
SELECT * FROM OrderDetailTable_v1;

Truncate Table OrderDetailTable_v2;

-- Update the RecNo into a sequence number
INSERT INTO OrderDetailTable_v2 (TxID, RecNo, OrderNo, Size, ProdNo, Prod_Name, UnitPrice)
SELECT TxID,
	row_number() over (ORDER BY OrderNo ASC) RecNo,
    OrderNo,
    Size,
    ProdNo,
    Prod_Name,
    UnitPrice
FROM OrderDetailTable_v1;

-- Drop the Unnessisary columns
-- 1) TxID, 2) Prod_Name 
ALTER TABLE OrderDetailTable_v2
DROP COLUMN TxID;
ALTER TABLE OrderDetailTable_v2
DROP COLUMN Prod_Name;

SELECT * FROM OrderDetailTable_v2;

-- Create the OrderTable for Further Processing
-- Update the OrderNo to format : 'ORD...'
CREATE TABLE OrderTable
SELECT * FROM TempTable;

-- Check the table structure of OrderTable
SELECT * FROM OrderTable;

-- Add the OrderNo Column into the OrderTable
ALTER TABLE OrderTable
ADD COLUMN OrderNo CHAR(10) AFTER TxID;

UPDATE OrderTable
SET OrderNo = CONCAT("ORD",CONVERT(TxID, CHAR));

-- Drop the TxID and Item Columns
ALTER TABLE OrderTable
DROP COLUMN TxID;
ALTER TABLE OrderTable
DROP COLUMN Item;

SELECT * FROM OrderTable;

-- Create the CustomerTable here
CREATE TABLE CustomerTable 
SELECT * FROM OrderTable;

-- Create the StoreTable here
CREATE TABLE StoreTable 
SELECT * FROM OrderTable;

-- For CustomerTable, remove the unnecessary columns
-- e.g.  Store_Name, TotPrice
ALTER TABLE CustomerTable
DROP COLUMN Store_Name;
ALTER TABLE CustomerTable
DROP COLUMN TotPrice;
ALTER TABLE CustomerTable
DROP COLUMN Payment;
ALTER TABLE CustomerTable
DROP COLUMN TxDate;

USE SQL_ETL;
SELECT * FROM CustomerTable;

SELECT Cust_Name, CardNo
FROM CustomerTable
GROUP BY Cust_Name, CardNo;

ALTER TABLE CustomerTable
ADD COLUMN CustNo CHAR(10) AFTER OrderNo;

ALTER TABLE CustomerTable
DROP COLUMN OrderNo;

CREATE TABLE CustomerTable_v1
SELECT CustNo, Cust_Name, CardNo
FROM CustomerTable
GROUP BY CustNo, Cust_Name, CardNo;

SELECT * FROM CustomerTable_v1;

CREATE TABLE CustomerTable_v2
SELECT * from CustomerTable_v1;

-- Truncate all the old record
-- Make sure the record number will start from 1 til the end of record
TRUNCATE TABLE CustomerTable_v2;

INSERT INTO CustomerTable_v2 (CustNo, Cust_Name, CardNo)
SELECT row_number() over (ORDER BY Cust_Name ASC) CustNo,
    Cust_Name,
    CardNo
FROM CustomerTable_v1;

SELECT * FROM CustomerTable_v2;

UPDATE CustomerTable_v2
SET CustNo = CONCAT("CUS",CustNo);

SELECT * FROM CustomerTable_v2;

/*
UPDATE CustomerTable
SET CardNo = 'n'
Where CardNo = 'No Card';
*/

-- Normalize the StoreTable
SELECT * FROM StoreTable;

ALTER TABLE StoreTable
DROP COLUMN TxDate;
ALTER TABLE StoreTable
DROP COLUMN Cust_Name;
ALTER TABLE StoreTable
DROP COLUMN TotPrice;
ALTER TABLE StoreTable
DROP COLUMN Payment;
ALTER TABLE StoreTable
DROP COLUMN CardNo;

SELECT * FROM StoreTable;

ALTER TABLE StoreTable
ADD COLUMN StoreNo VARCHAR(40) AFTER OrderNo;

Select * from StoreTable;

ALTER TABLE StoreTable
DROP COLUMN OrderNo;

CREATE TABLE StoreTable_v1
SELECT StoreNo, Store_Name
FROM StoreTable
GROUP BY StoreNo, Store_Name;

TRUNCATE TABLE StoreTable;

INSERT INTO StoreTable (StoreNo, Store_Name)
SELECT row_number() over (ORDER BY Store_Name ASC) StoreNo,
    Store_Name
FROM StoreTable_v1;

SELECT * FROM StoreTable;

USE SQL_ETL;
UPDATE StoreTable
SET StoreNo = CONCAT("STR",StoreNo);

SELECT * FROM StoreTable;

-- Perform the normalization of OrderTable again
SELECT * from OrderTable;

-- Add Columns : StoreNo and CustNo to the OrderTable
USE SQL_ETL;
ALTER TABLE OrderTable
ADD COLUMN StoreNo VARCHAR(100) AFTER Store_Name;
ALTER TABLE OrderTable
ADD COLUMN CustNo VARCHAR(100) AFTER Cust_Name;

-- Perform the normalization of OrderTable again
SELECT * from OrderTable;

-- Update the StoreNo by comparing the Store_Name in StoreTable
USE SQL_ETL;
UPDATE OrderTable
SET OrderTable.StoreNo = (SELECT StoreTable.StoreNo FROM StoreTable
WHERE OrderTable.Store_Name = StoreTable.Store_Name);

-- Check the status
SELECT * from OrderTable;

SELECT * from CustomerTable_v2;

-- Update the CustNo
USE SQL_ETL;
UPDATE OrderTable
SET OrderTable.CustNo = (SELECT CustomerTable_v2.CustNo FROM CustomerTable_v2
WHERE OrderTable.Cust_Name = CustomerTable_v2.Cust_Name
AND OrderTable.CardNo = CustomerTable_v2.CardNo);

-- Verify the results, if OK, proceed...
SELECT * FROM OrderTable;

-- Drop the unneccessary columns
-- i.e. Store_Name, Cust_Name, CardNo
ALTER TABLE OrderTable
DROP COLUMN Store_Name;
ALTER TABLE OrderTable
DROP COLUMN Cust_Name;
ALTER TABLE OrderTable
DROP COLUMN CardNo;

-- Verify the results, if OK, proceed...
SELECT * FROM OrderTable;

-- Final, Update the CustomerTable
USE SQL_ETL;
SELECT * FROM CustomerTable;

Update CustomerTable_v2
SET CardNo = 'No Card'
Where CardNo = 'n';

DROP TABLE CustomerTable_v1;

SELECT * FROM CustomerTable_v2;

DROP TABLE CustomerTable;

CREATE TABLE CustomerTable
SELECT * FROM CustomerTable_v2;

DROP TABLE CustomerTable_v2;


-- Clear ProductTable
DROP TABLE ProductTable_v0;
DROP TABLE ProductTable_v1;
DROP TABLE ProductTable_v2;
DROP TABLE ProductTable_v3;
DROP TABLE ProductTable_v4;
DROP TABLE ProductTable_v22;

SELECT * FROM ProductTable;

-- Clear OrderDetailTable
DROP TABLE OrderDetailTable;
DROP TABLE OrderDetailTable_v1;

CREATE TABLE OrderDetailTable
SELECT * FROM OrderDetailTable_v2;

DROP TABLE OrderDetailTable_v2;

Select * from OrderDetailTable;

-- --------------- Delete the SecondTable
DROP TABLE SecondTable;
-- --------------- Delete the TempTable
DROP TABLE TempTable;
-- --------------- Delete the TempCustTable
DROP TABLE TempCustTable;
-- --------------- Delete the TempProdTable_v0
DROP TABLE TempProdTable_v0;
-- --------------- Delete the TempProdTable_v1
DROP TABLE TempProdTable_v1;
-- --------------- Delete the StoreTable_v1
DROP TABLE StoreTable_v1;
