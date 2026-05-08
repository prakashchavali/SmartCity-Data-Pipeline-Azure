# -------------------------------------------------------------------------
# Project: Smart City Infrastructure Pipeline
# Version: 2.1 (Maintenance Update: May 2026)
# Maintenance Task: Audited Key Vault secret scopes and verified 
# compatibility with Databricks Unity Catalog for future migration.
# -------------------------------------------------------------------------
# Databricks notebook source
# --- Configuration Parameters ---
# Replace with your actual ADLS Gen2 storage account name
storage_account_name = "smartcitystoragelake"

# --- Retrieve Credentials from Databricks Secret Scope ---
# These keys ('client-id', 'client-secret', 'tenant-id') must exist in your
# Key Vault (e.g., 'kv-smartcity-secrets') and be accessible via your
# Databricks secret scope (e.g., 'smartcity-scope').
try:
    client_id = dbutils.secrets.get(scope="smartcity-scope", key="client-id")
    client_secret = dbutils.secrets.get(scope="smartcity-scope", key="client-secret")
    tenant_id = dbutils.secrets.get(scope="smartcity-scope", key="tenant-id")
except Exception as e:
    print(f"Error retrieving secrets from Key Vault: {e}")
    print("Please ensure 'smartcity-scope' exists and contains 'client-id', 'client-secret', 'tenant-id' secrets.")
    print("Also, confirm your Databricks workspace's Managed Identity has 'Get' and 'List' permissions on the Key Vault.")
    raise

# --- Configure Spark Session for ADLS Gen2 Direct Access (abfss://) ---
# These configurations tell Spark how to authenticate to your ADLS Gen2 using the Service Principal.
spark_session_configs = {
    f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net": "OAuth",
    f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net": client_id,
    f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net": client_secret,
    f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Apply these configurations to the Spark session
for key, value in spark_session_configs.items():
    spark.conf.set(key, value)

# --- Define Base Paths for your ADLS Gen2 Containers ---
# This makes it much easier to construct paths later.

# Base path for your storage account (without container)
base_storage_path = f"abfss://{{container_name}}@{storage_account_name}.dfs.core.windows.net/"

# Specific container paths
landing_path = base_storage_path.format(container_name="landing")
bronze_path = base_storage_path.format(container_name="bronze")
silver_path = base_storage_path.format(container_name="silver")
gold_path = base_storage_path.format(container_name="gold")	

# dbutils.fs.ls(landing_path)


# COMMAND ----------

