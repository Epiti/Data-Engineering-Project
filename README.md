# Data-Engineering-Project
# 🏗️ SQL Server Data Warehouse – Medallion Architecture

This project showcases the design and implementation of a  **Data Warehouse** using **SQL Server**, based on the Medallion Architecture (Bronze, Silver, and Gold layers). It covers the complete ETL process from raw ingestion to analytical modeling.

---

## 📌 Project Overview

- **Data Architecture**: Implements a layered architecture (Bronze → Silver → Gold) to streamline data processing and transformation.
- **ETL Pipelines**: Loads structured data from CSV files into SQL Server, followed by cleansing and transformation.
- **Data Modeling**: Builds a star schema with fact and dimension tables optimized for analytical queries.
- **Analytics**: Prepares clean, structured data suitable for dashboarding and SQL-based insights.

---

## 📁 Medallion Architecture

### 🟫 Bronze Layer
- Raw data from source CSV files
- Minimal validation; stored as-is

### ⬜ Silver Layer
- Cleansed and standardized data
- Includes deduplication, null handling, and value mapping

### 🟨 Gold Layer *(Planned)*
- Business-ready star schema
- Fact and dimension tables prepared for analytics and reporting

---

## 🔁 Data Flow

Visual diagrams in this repository detail:
- Data Architecture  
- Data Flow (ETL Process)  
- Star Schema Model (Fact & Dimensions)  


---

## 🧠 Key Skills Demonstrated

✅ SQL Development  
✅ Data Cleansing with T-SQL  
✅ ETL Design & Automation  
✅ Data Modeling (Star Schema)  
✅ Layered Data Architecture (Bronze/Silver/Gold)  

---
