/*
==============================================================

Create Database and Schemas

==============================================================

Script Purpose:
  This script creates a new database name 'Datawarehouse'.
  The script sets up three schemas within the database: bronze, silver, gold.

*/

USE master;
GO
  
-- CREATE DATABASE 'Datawarehouse'
CREATE DATABASE Datawarehouse;
GO
  
-- Move to the database 'Datawarehouse'
USE Datawarehouse;
GO
  
-- Create Schemas within the database: Bronze, Silver, Gold
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;



