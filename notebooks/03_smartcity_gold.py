# Project: Gold Layer Enrichment
# Update Note (May 2026): Performance Tuning – Verified window function 
# partitioning efficiency for transport and energy datasets.
# Databricks notebook source
# MAGIC %md
# MAGIC ### Enriching Data - Gold Layer

# COMMAND ----------

# MAGIC %run "./00_smartcity_configs"

# COMMAND ----------

from pyspark.sql.functions import col, when, avg, date_add, hour, regexp_replace, round, sum
from pyspark.sql.window import Window

# COMMAND ----------

airquality_silver = spark.read.format("delta").load(f"{silver_path}air_quality_cleaned_delta/")
building_permits_silver = spark.read.format("delta").load(f"{silver_path}building_permits_cleaned_delta/")
energy_silver = spark.read.format("delta").load(f"{silver_path}energy_cleaned_delta/")
traffic_silver = spark.read.format("delta").load(f"{silver_path}traffic_cleaned_delta/")
transport_silver = spark.read.format("delta").load(f"{silver_path}transport_cleaned_delta/")
waste_silver = spark.read.format("delta").load(f"{silver_path}waste_cleaned_delta/")
cityzones_silver = spark.read.format("delta").load(f"{silver_path}cityzones_cleaned_delta/")
sensormetadata_silver = spark.read.format("delta").load(f"{silver_path}sensormetadata_cleaned_delta/")
truckfleet_silver = spark.read.format("delta").load(f"{silver_path}truckfleet_cleaned_delta/")

# COMMAND ----------

air_quality_enrich = airquality_silver.withColumn("AQI", 
         when((col("pm2_5") < 50) & (col("pm10") < 50), "good")
        .when((col("pm2_5").between(101.0,200.0)) & (col("pm10").between(101.0, 200.0)), "unhealthy")
        .when((col("pm2_5") > 200) | (col("pm10") > 200) | (col("co_level") > 29.8) | (col("so2_level") > 214), "worst")
        .otherwise("moderate"))

# COMMAND ----------

building_permits_enrich = building_permits_silver \
    .withColumn("cost_category", when(col("estimated_cost") < 100000, "low")
                                .when(col("estimated_cost") < 500000, "medium")
                                .otherwise("high")) \
    .withColumn("completion_date", date_add("issue_date", 300))

# COMMAND ----------

engwin = Window.partitionBy("supply_area", "peak_load_time_hour")

energy_enrich = energy_silver.withColumn("peak_eng_consumption", round(avg(col("energy_kwh")).over(engwin), 2)) \
.withColumn("relative_usage_pct", round((col("energy_kwh") / col("peak_eng_consumption")) * 100, 2))

# COMMAND ----------
# Maintenance Update (May 2026): Left-join strategy with sensor_metadata 
# audited to ensure no data loss during Gold layer curation.

MAX_VEHICLE_COUNT_PER_HOUR = 1000
MAX_ALLOWED_SPEED = 100

traffic_enrich = traffic_silver \
.withColumn("traffic_density_score", 
round((col("vehicle_count")/MAX_VEHICLE_COUNT_PER_HOUR) * (1 - col("average_speed_kmph") / MAX_ALLOWED_SPEED), 2))

traffic_enrich = traffic_enrich \
    .join(sensormetadata_silver, "sensor_id", "left") \
    .select(traffic_enrich["*"], col("location_lat"), col("location_long"), col("sensor_type"))

# COMMAND ----------

transport_enrich = transport_silver.withColumn("hour", hour(col("timestamp")))

winhour = Window.partitionBy("route_number", "hour")

transport_enrich = transport_enrich \
    .withColumn("peak_hour_passenger", sum("passenger_count").over(winhour))

# COMMAND ----------

waste_enrich = waste_silver.withColumn("truck_id", regexp_replace(col("truck_id"), "TRK", "TRUCK"))

waste_enrich = waste_enrich.join(truckfleet_silver, "truck_id", "left") \
    .join(cityzones_silver, "zone_id", "left") \
    .select(waste_enrich["*"], "capacity_kg", "zone_name", "population_density", "area_sq_km")

waste_enrich = waste_enrich \
    .withColumn("waste_per_capita_kg", round(col("weight_kg")/col("population_density"), 2)) \
    .withColumn("truck_utilization_rate", round(col("weight_kg")/col("capacity_kg"),2)) \
    .withColumn("zone_efficiency", round(col("truck_utilization_rate") * col("population_density"), 2))

# COMMAND ----------

air_quality_enrich.write.mode("overwrite").format("delta").save(f"{gold_path}air_quality_enriched_delta/")
building_permits_enrich.write.mode("overwrite").format("delta").save(f"{gold_path}building_permits_enriched_delta/")
energy_enrich.write.mode("overwrite").format("delta").save(f"{gold_path}energy_enriched_delta/")
traffic_enrich.write.mode("overwrite").format("delta").save(f"{gold_path}traffic_enriched_delta/")
transport_enrich.write.mode("overwrite").format("delta").save(f"{gold_path}transport_enriched_delta/")
waste_enrich.write.mode("overwrite").format("delta").save(f"{gold_path}waste_enriched_delta/")

# COMMAND ----------

dbutils.fs.ls(gold_path)

# COMMAND ----------

