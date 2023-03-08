-- This project mainly focus on the T-SQL to perform the ETL functions
-- By Samuel Ko, 06 Mar 2023
CREATE DATABASE IF NOT EXISTS SQL_ETL;
USE SQL_ETL;

CREATE TABLE InitialTable (
	TxDate VARCHAR(40),
    Store_Name VARCHAR(100),
    Cust_Name VARCHAR(100),
    Item MEDIUMTEXT,
    TotPrice FLOAT,
    Payment VARCHAR(20),
    CardNo VARCHAR(20)
);

USE SQL_ETL;
select * from InitialTable
where CardNo != 'n';