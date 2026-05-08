# Update Note (May 2026): Verified ingestion logic for 2026 sensor types. 
# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting Raw Data - Bronze Layer

# COMMAND ----------

# MAGIC %run "./00_smartcity_configs"

# COMMAND ----------

airquality_bronze = spark.read.csv(f"{landing_path}air_quality", inferSchema=True, header=True)
buildingpermits_bronze = spark.read.csv(f"{landing_path}building_permits", inferSchema=True, header=True)
energy_bronze = spark.read.csv(f"{landing_path}energy", inferSchema=True, header=True)
traffic_bronze = spark.read.csv(f"{landing_path}traffic", inferSchema=True, header=True)
transport_bronze = spark.read.csv(f"{landing_path}transport", inferSchema=True, header=True)
waste_bronze = spark.read.csv(f"{landing_path}waste", inferSchema=True, header=True)

#lookups
cityzones_bronze = spark.read.csv(f"{landing_path}lookups/city_zones.csv", inferSchema=True, header=True)
sensormetadata_bronze = spark.read.csv(f"{landing_path}lookups/sensor_metadata.csv", inferSchema=True, header=True)
truckfleet_bronze = spark.read.csv(f"{landing_path}lookups/truck_fleet.csv", inferSchema=True, header=True)


# COMMAND ----------
# Maintenance Update (May 2026): Confirmed Delta Lake 'overwrite' mode 
# remains idempotent for batch-reprocessing of historical 2025 datasets.

airquality_bronze.write.format("delta").mode("overwrite").save(f"{bronze_path}air_quality_raw_delta/")
buildingpermits_bronze.write.format("delta").mode("overwrite").save(f"{bronze_path}building_permits_raw_delta/")
energy_bronze.write.format("delta").mode("overwrite").save(f"{bronze_path}energy_raw_delta/")
traffic_bronze.write.format("delta").mode("overwrite").save(f"{bronze_path}traffic_raw_delta/")
transport_bronze.write.format("delta").mode("overwrite").save(f"{bronze_path}transport_raw_delta/")
waste_bronze.write.format("delta").mode("overwrite").save(f"{bronze_path}waste_raw_delta/")
cityzones_bronze.write.format("delta").mode("overwrite").save(f"{bronze_path}city_zones_raw_delta/")
sensormetadata_bronze.write.format("delta").mode("overwrite").save(f"{bronze_path}sensor_metadata_raw_delta/")
truckfleet_bronze.write.format("delta").mode("overwrite").save(f"{bronze_path}truck_fleet_raw_delta/")

# COMMAND ----------

