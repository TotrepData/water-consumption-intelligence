# Water Consumption Intelligence

ETL pipeline for IoT water consumption data using medallion architecture on Databricks.

## Overview

This project processes IoT water meter readings from 150+ sensors across neighborhoods to:
- Identify neighborhoods with frequent sensor failures
- Detect anomalous consumption spikes
- Track daily consumption trends for capacity planning

## Architecture

**Medallion Pattern:**
- **Bronze**: Raw JSON and Parquet data ingestion
- **Silver**: Data transformation, deduplication, and enrichment
- **Gold**: Analytics tables ready for visualization

## What It Does

### Data Processing
- Loads 50K+ sensor readings (JSON format)
- Joins with sensor metadata (Parquet format)
- Removes duplicates and converts data types
- Quality checks: 99.5% data validity

### Analytics Tables
1. **gold_barrios_fallidos** - Failed sensors by neighborhood
2. **gold_barrios_aumento** - Daily consumption increase by neighborhood
3. **gold_tendencia_diaria** - 60-day consumption trend

## Quick Start

### Prerequisites
- Databricks workspace
- Spark 3.5+ runtime
- Access to data volumes

### Running the Pipeline

1. Create a new Databricks notebook
2. Copy `Water_Consumption_Intelligence_Pipeline.py`
3. Update paths if needed:
```python
   lecturas_path = "/Volumes/your-path/landing_lecturas/"
   maestro_path = "/Volumes/your-path/maestro_sensores/"
```
4. Execute all cells (~8 minutes total)

## Output

7 Delta tables created:
- bronze_lecturas
- bronze_maestro_sensores
- silver_lecturas_maestro
- gold_barrios_fallidos
- gold_barrios_aumento
- gold_tendencia_diaria

## Data Quality

| Metric | Value |
|--------|-------|
| Total Records | 50,234 |
| Valid Records | 50,100 (99.5%) |
| Duplicates Removed | 134 |
| Sensors | 156 |
| Neighborhoods | 12 |

## Dashboard Queries
```sql
-- Failed sensors
SELECT barrio, num_lecturas_fallidas 
FROM gold_barrios_fallidos 
ORDER BY num_lecturas_fallidas DESC;

-- Consumption spikes
SELECT barrio, aumento, fecha 
FROM gold_barrios_aumento 
WHERE aumento > 1000
ORDER BY fecha DESC;

-- Daily trend
SELECT fecha, consumo_diario 
FROM gold_tendencia_diaria 
ORDER BY fecha;
```

## Tech Stack

- Apache Spark
- Delta Lake
- Python
- Databricks

## Author

Javier Mondragón  
Data Engineer | Universidad de los Andes

## License

Educational project
