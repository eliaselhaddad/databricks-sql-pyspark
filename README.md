# Introduction
This project is part of the [DP203] course, which aims to provide hands-on experience with data engineering using Azure Databricks, Delta Lake, Unity Catalog, and Azure Data Factory. The project revolves around analyzing Formula1 racing data to derive meaningful insights and showcase the integration of various Azure data services.

## Steps:
### 1. Create a databricks workspace
### 2. Create a cluster
#### 2a. Cluster policy:
- Simplifies the user interface
- Cost control by the administrator
- Runtime version
- ... and much more
Link for all the policies on Azure and examples: Lhttps://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policy-definition
#### 2b. Cluster configuration:
- Policy: Unrestricted
- Single node
- Access mode: No isolation shared. Note: This access mode doesn't allow the use of Unity Catalog
- Databricks runtime version: 15.4 LTS (Scala 2.12, Spark 3.5.0)
- Node type: Standard_DS3_V2 (14 GB Memory, 4 Cores). Note: Not all type of nodes are available in all regions. Choose the closest in performance. 4 cores are usually sufficient for this kind of workload
- Set terminate time to the desired time, otherwise the cluster will keep running
### 3. Cost control
- Set budget alert to keep the cost as low as possible
- Doing this course will cost you around 30$ ~ 300SEK for Azure services

### 4. Create a Delta Lake
### 5. Use Azure Key Vaults to store keys
### 6. Use Shared Access Signature (SAS) to access the storage account












