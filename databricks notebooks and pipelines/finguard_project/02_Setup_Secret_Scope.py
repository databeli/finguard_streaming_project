# Databricks notebook source
# =====================================================
# 1️⃣ Define Connection Variables (EDIT THESE)
# =====================================================

# Secret scope name (will be created if not exists)
secret_scope_name = "finguard-scope"

# COMMAND ----------

# Python cell in the same workspace notebook
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

api_url  = ctx.apiUrl().getOrElse(None)     # e.g. https://adb-...azuredatabricks.net
api_token = ctx.apiToken().getOrElse(None)  # personal access token for this session

print(api_url)
print(api_token)  # handle securely, do not log in real code


import requests
import json

# ----------------------------------------
# Configuration
# ----------------------------------------
DATABRICKS_INSTANCE = api_url  # Replace with your workspace URL
DATABRICKS_TOKEN = api_token  # Replace with your PAT

scope_name = secret_scope_name  # Scope to be created
backend_type = "DATABRICKS"     # Use "AZURE_KEYVAULT" if integrating with Key Vault

# COMMAND ----------



# ----------------------------------------
# API Endpoint
# ----------------------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/scopes/create"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope_name
}

# ----------------------------------------
# Send request
# ----------------------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

# ----------------------------------------
# Handle response
# ----------------------------------------
if response.status_code == 200:
    print(f"Secret scope '{scope_name}' created successfully.")
else:
    print("Failed to create secret scope.")
    print("Status Code:", response.status_code)
    print("Response:", response.text)


# COMMAND ----------

kafka_bootstrap_servers = 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
kafka_topic = 'credit_card_transactions'
kafka_api_key = '5OVF32EJBUO5UHI4'
kafka_api_secret = 'cfltbMZTBsGT29a6a7frYaxDiSAWPLD2synlHwqUkmQtbf9P2vttx7qISb+2PMzA'

kafka_connection_details = json.dumps({
    "bootstrap_servers": kafka_bootstrap_servers,
    "topic": kafka_topic,
    "api_key": kafka_api_key,
    "api_secret": kafka_api_secret
})

# COMMAND ----------

print(kafka_connection_details)

# COMMAND ----------

import requests
import json

scope = secret_scope_name          # Already existing scope
secret_key='kafka_connection_details'
secret_value=kafka_connection_details

# -------------------------------------------------
# API Endpoint
# -------------------------------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}

# -------------------------------------------------
# Send Request
# -------------------------------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

# -------------------------------------------------
# Output
# -------------------------------------------------
if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully in scope '{scope}'.")
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)


# COMMAND ----------

# =====================================================
# 5️⃣ Verify Secret Retrieval
# =====================================================

try:
    retrieved_json = dbutils.secrets.get(
        scope=secret_scope_name,
        key='kafka_connection_details'
    )
    
    print("Secret retrieved successfully.")
    
    parsed = json.loads(retrieved_json)
    print("Parsed JSON:")
    print(parsed)
    
except Exception as e:
    print("Secret verification failed:")
    print(str(e))


# COMMAND ----------

import requests
import json

scope = secret_scope_name          # Already existing scope
secret_key = 'gmail_api_key'       # Name of the secret entry
secret_value = 'lkls hreo ltin dqwz' # Value to store securely

# -------------------------------------------------
# API Endpoint
# -------------------------------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}

# -------------------------------------------------
# Send Request
# -------------------------------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

# -------------------------------------------------
# Output
# -------------------------------------------------
if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully in scope '{scope}'.")
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)
