# Databricks notebook source
adlsAccountName="moviedata983940"
adlsContainerName="validated"
adlsFolderName="Data"
mountPoint="/mnt/Files/Validated"
# Application (Client) ID
applicationId=dbutils.secrets.get(scope="moviescope", key="clientid")
# Application (Client) secret key
authenticationKey=dbutils.secrets.get(scope="moviescope",key="test1")
# Directory (tenant) ID
tenantId=dbutils.secrets.get(scope="moviescope", key="tenantid")
endpoint="https://login.microsoftonline.com/"+tenantId+"/oauth2/token"
source="abfss://"+adlsContainerName+"@"+adlsAccountName+".dfs.core.windows.net/"+adlsFolderName
#Connecting using service principal secrets and OAuth
configs={"fs.azure.account.auth.type":"OAuth",
      "fs.azure.account.oauth2.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id":applicationId,
        "fs.azure.account.oauth2.client.secret":authenticationKey,
        "fs.azure.account.oauth2.client.endpoint":endpoint}
# Mounting ADLS storage to DBFS
# Mount only if directory is not already mounted
if not any(mount.mountPoint==mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source=source,
    mount_point=mountPoint,
    extra_configs=configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/Files/Validated

# COMMAND ----------


