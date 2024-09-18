# Formula1 Data Engineering Project

This project is part of the [DP203] course, which aims to provide hands-on experience with data engineering using Azure Databricks, Delta Lake, Unity Catalog, and Azure Data Factory. The project revolves around analyzing Formula1 racing data to derive meaningful insights and showcase the integration of various Azure data services.

## Prerequisites
- Azure subscription
- Basic understanding of Azure Databricks and Azure Data Factory
- Familiarity with Spark SQL

## Steps

### 1. Create a Databricks Workspace
Set up your Azure Databricks environment.

### 2. Create a Cluster

#### 2a. Cluster Policy
- Simplifies the user interface
- Cost control by the administrator
- Runtime version
- ... and much more
[Link for all the policies on Azure and examples](https://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policy-definition)

#### 2b. Cluster Configuration
- **Policy:** Unrestricted
- **Cluster Mode:** Single Node
- **Access Mode:** No isolation shared (Note: This mode doesn't allow the use of Unity Catalog)
- **Databricks Runtime Version:** 15.4 LTS (Scala 2.12, Spark 3.5.0)
- **Node Type:** Standard_DS3_V2 (14 GB Memory, 4 Cores). *(Note: Not all node types are available in all regions. Choose the closest in performance. 4 cores are usually sufficient for this kind of workload)*
- **Auto-Termination:** Set the terminate time to the desired time; otherwise, the cluster will keep running

### 3. Cost Control
- Set a budget alert to keep costs low.
- Note: Completing this course will cost approximately $30 (~300 SEK) for Azure services.

### 4. Create a Data Lake
Set up a Data Lake for storing and querying your data.

### 5. Use Azure Key Vaults to Store Keys
Securely store and manage your secrets using Azure Key Vault.

### 6. Use Shared Access Signature (SAS) to Access the Storage Account
Utilize SAS tokens to securely access your Azure storage account.

### 7. Useful Links
- [Ergast API to obtain the data](https://ergast.com/mrd/)
- [Spark SQL documentation](https://spark.apache.org/docs/latest/api/python/reference/index.html)



