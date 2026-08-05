# 🚖 NYC Taxi Data Engineering Project on Azure

An end-to-end Azure Data Engineering project that automates the ingestion, transformation, and storage of NYC Taxi trip data using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, and Delta Lake following the **Medallion Architecture (Bronze → Silver → Gold)**.

---

# 📌 Project Overview

This project demonstrates a modern cloud-based data engineering pipeline that:

- Downloads NYC Taxi trip data from a public HTTP source.
- Automates data ingestion using Azure Data Factory.
- Stores raw data in Azure Data Lake Storage Gen2.
- Cleans and transforms data using PySpark in Azure Databricks.
- Stores processed data in Delta format for analytics.
- Demonstrates Delta Lake features such as Time Travel, Update, Delete, Restore, and Vacuum.

---

# 🏗️ Architecture

```
NYC Taxi Public Dataset
          │
          ▼
Azure Data Factory
(ForEach + If Condition + Copy Activity)
          │
          ▼
Azure Data Lake Storage Gen2
      Bronze Layer
          │
          ▼
Azure Databricks
(PySpark Transformations)
          │
          ▼
Azure Data Lake Storage Gen2
      Silver Layer
          │
          ▼
Azure Databricks
(Delta Lake)
          │
          ▼
Azure Data Lake Storage Gen2
       Gold Layer
          │
          ▼
Analytics & SQL Queries
```

---

# 🧱 Medallion Architecture

### 🟤 Bronze Layer
- Stores raw NYC Taxi data.
- Data is ingested directly from the public HTTP source.
- No transformations are performed.

### ⚪ Silver Layer
- Cleans and transforms raw data.
- Performs schema enforcement.
- Creates derived columns.
- Standardizes data for analytics.

### 🟡 Gold Layer
- Stores business-ready Delta tables.
- Supports SQL queries and analytical workloads.
- Demonstrates Delta Lake capabilities.

---

# ⚙️ Azure Services Used

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Delta Lake
- Apache Spark (PySpark)

---

# 📂 Repository Structure

```
NYC-Taxi-Data-Engineering-Project
│
├── Data/
├── Databricks_Notebooks/
├── Dataset/
├── Factory/
├── Linked Services/
├── Pipelines/
├── SQL/
├── Screenshots/
├── Workflow/
└── README.md
```

---

# 🔄 Pipeline Workflow

1. Azure Data Factory downloads NYC Taxi data from a public HTTP endpoint.
2. Raw data is stored in the Bronze container.
3. Azure Databricks reads Bronze data.
4. Data cleaning and transformations are performed.
5. Processed data is written to the Silver container.
6. Gold layer Delta tables are created.
7. SQL operations are performed for analytics and validation.

---

# ✨ Features

- Automated data ingestion using Azure Data Factory
- Dynamic pipelines with parameters
- ForEach and If Condition activities
- Medallion Architecture implementation
- PySpark data transformations
- Delta Lake integration
- Time Travel
- Update/Delete operations
- Restore table versions
- Vacuum operation
- End-to-end cloud data engineering workflow

---

# 📸 Project Screenshots

Project screenshots are available in the **Screenshots** folder and include:

- Azure Data Factory Pipelines
- Azure Data Lake Storage
- Azure Databricks Notebooks
- Pipeline Execution
- Medallion Architecture
- Delta Lake Operations

---

# 🛠️ Tech Stack

| Category | Technologies |
|----------|--------------|
| Cloud | Microsoft Azure |
| Data Integration | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Azure Databricks |
| Language | Python, PySpark, SQL |
| Data Format | Parquet, Delta Lake |

---

# 🔒 Note

Sensitive information such as storage account names, client IDs, client secrets, OAuth endpoints, and other credentials has been removed or replaced with placeholders before publishing this repository.

---

# 👩‍💻 Author

**Ishika Singh**

Final Year B.Tech (CSE - Artificial Intelligence)

Passionate about Data Engineering, Data Analytics, and Cloud Technologies.
