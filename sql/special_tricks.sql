USE SQL_ETL;
SELECT Store_Name, format(Sum(TotPrice), 2)
FROM SecondTable
GROUP BY Store_Name;

Select * from TempProdTable;
SELECT count(*) AS NO_OF_COLUMNS FROM information_schema.columns
WHERE table_name ="TempProdTable";
select * from TempProdTable;

delimiter //
CREATE PROCEDURE testPros()
BEGIN
DECLARE lastRow INT DEFAULT 0;
DECLARE startRow INT DEFAULT 0;
SELECT COUNT(*) FROM TempProdTable INTO lastRow;
SET startRow=0;
WHILE startRow < lastRow DO
INSERT INTO ProductTable(Prod5, Prod6, Prod7) SELECT (Prod5, Prod6, Prod7) FROM TempProdTable LIMIT startRow,1;
SET startRow = startRow+1;
END WHILE;
End;
//

USE SQL_ETL;
call testPros();

select TxID, Prod10 from TempProdTable;

-- Slicing the products.  Split single column into multiple columns
CREATE TABLE TempProdTable SELECT TxID, Store_Name, Cust_Name, 
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


-- Transpose x/y columns to form the productable
CREATE TABLE OrderDetailTable
(select TxID, Store_Name, Cust_Name, Trim(Prod1) as Prod_Name from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod2) as Prod_Name from TempProdTable
union  
select TxID, Store_Name, Cust_Name, Trim(Prod3) as Prod_Name from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod4) as Prod_Name from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod5) as Prod_Name from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod6) as Prod_Name from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod7) as Prod_Name from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod8) as Prod_Name from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod9) as Prod_Name  from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod10) as Prod_Name from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod11) as Prod_Name  from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod12) as Prod_Name  from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod13) as Prod_Name  from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod14) as Prod_Name  from TempProdTable
union
select TxID, Store_Name, Cust_Name, Trim(Prod15) as Prod_Name  from TempProdTable)
Order by TxID;

-- Select the Product. 80% OK!!!!
Select TxID, Trim(Substring_index(Prod_Name,' ',1)) as Size, 
		LEFT(Trim(Substring_index((Substring_index(Substring_index(Prod_Name,'Large',-1),'Regular',-1)),'-',-3)), (Length(Prod_Name) - 13)) as Prod_Name, 
        SUBSTRING_INDEX(Prod_Name,' ', -1) as Price
from OrderDetailTable;