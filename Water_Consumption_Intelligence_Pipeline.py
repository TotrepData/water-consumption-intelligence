# Databricks notebook source
# Water Consumption Intelligence Platform
# ETL Pipeline using Medallion Architecture
#
# Author: Javier Mondragón
# Date: November 2025
# Purpose: Process IoT water meter data
#
# Data Sources:
# - Landing: /Volumes/labs_56754_cs713b/acueducto/landing_lecturas/
# - Master: /Volumes/labs_56754_cs713b/acueducto/maestro_sensores/

# MARKDOWN
# # Water Consumption Intelligence Platform
# 
# ## Overview
# This pipeline implements a medallion architecture (Bronze-Silver-Gold) to:
# - Ingest raw IoT water meter readings
# - Transform and enrich data with sensor information
# - Create analytics tables for visualization
#
# ## Architecture Layers
# - **Bronze**: Raw data ingestion and normalization
# - **Silver**: Data transformation and quality assurance
# - **Gold**: Aggregations and machine learning ready data
#
# ## Processing Steps
# 1. Load JSON readings and parquet sensor master data
# 2. Store as Delta tables for ACID compliance
# 3. Transform types and join with sensor metadata
# 4. Create analytics tables for dashboard

# COMMAND

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, to_timestamp, sum, lag, desc, count, 
    current_date, date_sub, dayofweek, hour, sqrt, avg, pow
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("water-consumption-pipeline").getOrCreate()

print("=" * 80)
print("WATER CONSUMPTION INTELLIGENCE PLATFORM")
print("=" * 80)

# MARKDOWN
# ## LAYER 1: BRONZE - Data Ingestion

# Define paths
lecturas_path = "/Volumes/labs_56754_cs713b/acueducto/landing_lecturas/"
maestro_path = "/Volumes/labs_56754_cs713b/acueducto/maestro_sensores/"

# Read JSON sensor readings
lecturas_df = spark.read \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .json(lecturas_path)

# Read master sensor data
maestro_df = spark.read.parquet(maestro_path)

print(f"Sensor readings: {lecturas_df.count()} records")
print(f"Sensor master: {maestro_df.count()} sensors")

# Save as Delta tables (Bronze layer)
lecturas_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("labs_56754_cs713b.javier_mondragon.bronze_lecturas")

maestro_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("labs_56754_cs713b.javier_mondragon.bronze_maestro_sensores")

print("Bronze layer created")

# MARKDOWN
# ## LAYER 2: SILVER - Data Transformation

# Load bronze tables
lecturas_bronze = spark.table("labs_56754_cs713b.javier_mondragon.bronze_lecturas")
maestro_bronze = spark.table("labs_56754_cs713b.javier_mondragon.bronze_maestro_sensores")

# Convert data types
lecturas_silver = lecturas_bronze \
    .withColumn("consumoAcumuladoM2", col("consumoAcumuladoM2").cast("double")) \
    .withColumn("fechaHora", col("fechaHora").cast("timestamp"))

# Join with master data
silver_df = lecturas_silver.join(
    maestro_bronze,
    on="idSensor",
    how="left"
)

# Remove duplicates
silver_df = silver_df.dropDuplicates(["idSensor", "fechaHora", "consumoAcumuladoM2"])

print(f"Silver records after deduplication: {silver_df.count()}")

# Save Silver table
silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("labs_56754_cs713b.javier_mondragon.silver_lecturas_maestro")

print("Silver layer created")

# MARKDOWN
# ## LAYER 3: GOLD - Analytics Tables

# COMMAND

silver_df = spark.table("labs_56754_cs713b.javier_mondragon.silver_lecturas_maestro")

# a) Failed sensors by neighborhood
print("  - Creating: neighborhoods with failed sensor readings")
fallidos_agg = silver_df.filter(
    (col("estado") == "fallido") | (col("consumoAcumuladoM2").isNull())
).groupBy("barrio").agg(
    count("idSensor").alias("num_lecturas_fallidas")
).orderBy(desc("num_lecturas_fallidas"))

fallidos_agg.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("labs_56754_cs713b.javier_mondragon.gold_barrios_fallidos")

# b) Consumption increase by neighborhood
print("  - Creating: neighborhoods with highest consumption increase")
consumo_diario = silver_df.filter(
    col("consumoAcumuladoM2").isNotNull()
).groupBy("barrio", "fecha").agg(
    sum("consumoAcumuladoM2").alias("consumo_total")
)

window_barrio = Window.partitionBy("barrio").orderBy("fecha")

aumento_agg = consumo_diario.withColumn(
    "consumo_anterior",
    lag("consumo_total").over(window_barrio)
).withColumn(
    "aumento",
    col("consumo_total") - col("consumo_anterior")
).filter(
    col("aumento").isNotNull()
).orderBy(desc("aumento"))

aumento_agg.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("labs_56754_cs713b.javier_mondragon.gold_barrios_aumento")

# c) Daily consumption trend (last 60 days)
print("  - Creating: daily consumption trend")
fecha_limite = date_sub(current_date(), 60)

tendencia_df = silver_df.filter(
    (col("consumoAcumuladoM2").isNotNull()) &
    (col("fecha") >= fecha_limite)
).groupBy("fecha").agg(
    sum("consumoAcumuladoM2").alias("consumo_diario")
).orderBy("fecha")

tendencia_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("labs_56754_cs713b.javier_mondragon.gold_tendencia_diaria")

print("Gold layer created")
