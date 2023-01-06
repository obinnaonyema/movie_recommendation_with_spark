# Introduction 
In this project, data is ingested into Azure Gen2 datalake storage using Azure Data Factory. This data is loaded and prepared for machine learning in Databricks so as to use the distributed processing engine provided by Spark.

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Build and Test
1. Create storage account with hierarchical namespace enabled and load movies and ratings files into blobl containers. 
2. Create Databricks workspace and mount data from Azure blob storage to databricks.
3. Create feature engineering, training and prediction script in Databricks.
4. Set up app registration and create Key Vault to store app secrets.
5. Create Data Factory and set up pipeline with datasets and email notification extension
6. Create Logic App to receipt http request from Data Factory and shoot out email message in the request body

# Contribute
Current challenges:
1. Scheduled trigger fails with unknown error. Manual trigger works
