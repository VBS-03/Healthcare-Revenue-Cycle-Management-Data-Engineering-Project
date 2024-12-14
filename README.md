# Healthcare Revenue Cycle Management Data Engineering Project
Healthcare Revenue Cycle Management End-to-End Data Engineering Project on Azure
![Revenue-cycle-reimbursement](Revenue-cycle-reimbursement-flow-chart.jpg)
## Project Overview:
This project is a **metadata-driven data engineering solution** tailored for **Healthcare Revenue Cycle Management (RCM)**. Built on **Microsoft Azure**, the pipeline is designed to automate and optimize data ingestion, transformation, and historical data management processes while ensuring scalability and security.

### Key Highlights:
1. **Azure Services Utilized for the Project's Implementation:**
    - **Azure SQL Database:** Stores **EMR (Electronic Medical Record)** data of hospitals
    - **Azure Data Factory (ADF):** Orchestrates seamless data workflows.
    - **Azure Databricks:** Performs advanced transformations, data cleansing, and analytics with Unity Catalog for data governance.
    - **Azure Data Lake Storage Gen2:** Serves as a centralized repository for raw and processed data.
    - **Azure Key Vault:** Secures sensitive information like connection strings and API keys.

2. **Various Datasources Involved:**
    - Azure SQL Database
    - Flat Files
    - APIs

3. **Advanced Data Processing Support:**
    - **Full Load** : Ingests entire datasets.
    - **Incremental Load** : Updates data incrementally to optimize processing.
    - Implements **Slowly Changing Dimensions (SCD2)** for tracking historical changes.
    - Archives historical data for compliance and auditing.
    - **Metadata-Driven Flexibility** : Configurable pipeline parameters, source mappings, and transformation rules managed via metadata for agility and reduced hardcoding.
    - **Unity Catalog Integration** : Ensures fine-grained access control, centralized governance, and secure data sharing in Azure Databricks.
  
### Key Benefits: 
- **Healthcare-Focused** : Built to meet the specific needs of Healthcare RCM processes.
- **Scalable and Secure** : Leverages Azure's robust infrastructure for high performance and data security.
- **Automated and Adaptable** : Reduces manual efforts and allows quick adaptation to new requirements.

  
