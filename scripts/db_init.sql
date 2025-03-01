/* 
                                       CREATION OF DATABASE AND SCHEMAS
                                   ******************************************

This script creates a new database named 'Datawarehouse' and sets up 3 schemas (bronze, silver, and gold) within the Datawarehouse.

*/





--Create DataWarehouse
CREATE DATABASE Datawarehouse;
GO

Use Datawarehouse;

GO

--Create Schemas

CREATE SCHEMA bronze;

GO
CREATE SCHEMA silver;

GO

CREATE SCHEMA gold;
