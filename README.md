# Databricks-DeltaLake-Project
Based on different sources like Blob, ADLS, API, Azure SQL DB.


This Project is based on different sources including Blob, ADLS, API, Azure SQL DB.

so, first created linked services along with dataset for source and sink both purposes and then created mount point in databricks to read those files and further did some transformation and putting on the Cleanse Layer, and then applied some business logic and fact and dim table and put it into the Publish Layer(Serving Layer).


Source -------->   Bronze (Raw Layer) --------> Silver (Cleansed Layer) --------> Gold (Publish layer)
files----------> Full Load here --------------> Incremental Load here -----------> Fact and dim table


-----------------------------------------------------------------------------------------------------------------------------------------------------------
Project: Databricks Delta Lake Pipeline with Multi-Source Integration
This project implements an end-to-end data pipeline using Azure Databricks and Delta Lake, integrating data from diverse sources:

Azure Blob Storage

Azure Data Lake Storage (ADLS Gen2)

REST APIs

Azure SQL Database

Architecture Overview
The pipeline follows a medallion architecture:

🔹 1. Bronze Layer (Raw Layer)
Purpose: Ingest raw data in its native format.

Source: Data is pulled from Blob, ADLS, API, and Azure SQL DB.

Implementation:

Created Linked Services and Datasets in Azure Data Factory (ADF) or Azure Synapse for source and sink.

For Databricks, created mount points using DBFS to access Blob and ADLS data.

Full Load is typically used here since it's the raw snapshot.

🔸 2. Silver Layer (Cleansed Layer)
Purpose: Perform data cleaning and standardization.

Implementation:

Read raw data from the Bronze layer using PySpark in Databricks.

Applied data quality checks, schema alignment, null handling, type casting, and standard transformations.

This layer prepares data for business logic by cleansing and structuring it.

Incremental Load begins here for performance and efficiency.

🟡 3. Gold Layer (Publish/Serving Layer)
Purpose: Store business-ready data for analytics, reporting, and ML.

Implementation:

Applied business logic and transformations.

Built fact and dimension tables (e.g., Sales Fact, Customer Dim).

Stored optimized Delta tables ready for Power BI, Synapse, or other consumers.

Data Flow Summary
pgsql
Copy
Edit
Multiple Data Sources (Blob, ADLS, API, SQL DB)
          │
          ▼
     🔹 Bronze Layer (Raw Ingestion)
     - Full load
          │
          ▼
     🔸 Silver Layer (Cleansed/Refined)
     - Incremental load
     - Standardized schema
          │
          ▼
     🟡 Gold Layer (Publish Layer)
     - Business logic
     - Fact and Dimension tables
Technologies Used
Azure Databricks (Delta Lake, PySpark)

Azure Data Factory (for orchestration)

Azure SQL Database

Azure Blob Storage & ADLS Gen2

REST APIs (via Python or ADF Web Activity)

Key Features
Delta Lake ensures ACID transactions, schema enforcement, and time travel.

The medallion architecture ensures separation of concerns and scalable processing.

Modular design allows easy debugging, monitoring, and reprocessing of specific layers.
