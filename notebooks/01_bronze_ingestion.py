# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

spark.sql("USE CATALOG clinical_trials_dev")

# COMMAND ----------

display(
    dbutils.fs.ls(
        "/Volumes/clinical_trials_dev/bronze/raw_files/"
    )
)

# COMMAND ----------

patients_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(
        "dbfs:/Volumes/clinical_trials_dev/bronze/raw_files/patients.csv"
    )
)

# COMMAND ----------

display(patients_df)

# COMMAND ----------

patients_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

bronze_patients_df = (
    patients_df
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_file_name",
        F.col("_metadata.file_path")
    )
    .withColumn(
        "batch_id",
        F.lit("batch_001")
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws("||", *patients_df.columns),
            256
        )
    )
)

# COMMAND ----------

display(bronze_patients_df)

# COMMAND ----------

(
    bronze_patients_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "clinical_trials_dev.bronze.bronze_patients"
    )
)

# COMMAND ----------

# DBTITLE 1,Trials Dataset
trials_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(
        "dbfs:/Volumes/clinical_trials_dev/bronze/raw_files/trials.csv"
    )
)

# COMMAND ----------

display(trials_df)

# COMMAND ----------

trials_df.printSchema()

# COMMAND ----------

trials_df.count()

# COMMAND ----------

bronze_trials_df = (
    trials_df
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_file_name",
        F.col("_metadata.file_path")
    )
    .withColumn(
        "batch_id",
        F.lit("batch_001")
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws("||", *trials_df.columns),
            256
        )
    )
)

# COMMAND ----------

display(bronze_trials_df)

# COMMAND ----------

(
    bronze_trials_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "clinical_trials_dev.bronze.bronze_trials"
    )
)

# COMMAND ----------

# DBTITLE 1,Sites
sites_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(
        "dbfs:/Volumes/clinical_trials_dev/bronze/raw_files/sites.csv"
    )
)

# COMMAND ----------

display(sites_df)

# COMMAND ----------

bronze_sites_df = (
    sites_df
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_file_name",
        F.col("_metadata.file_path")
    )
    .withColumn(
        "batch_id",
        F.lit("batch_001")
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws("||", *sites_df.columns),
            256
        )
    )
)

# COMMAND ----------

(
    bronze_sites_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "clinical_trials_dev.bronze.bronze_sites"
    )
)

# COMMAND ----------

# DBTITLE 1,Visits
visits_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(
        "dbfs:/Volumes/clinical_trials_dev/bronze/raw_files/visits.csv"
    )
)

bronze_visits_df = (
    visits_df
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_file_name",
        F.col("_metadata.file_path")
    )
    .withColumn(
        "batch_id",
        F.lit("batch_001")
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws("||", *visits_df.columns),
            256
        )
    )
)

(
    bronze_visits_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "clinical_trials_dev.bronze.bronze_visits"
    )
)

# COMMAND ----------

# DBTITLE 1,Adverse events
ae_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(
        "dbfs:/Volumes/clinical_trials_dev/bronze/raw_files/adverse_events.csv"
    )
)

bronze_ae_df = (
    ae_df
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_file_name",
        F.col("_metadata.file_path")
       
    )
    .withColumn(
        "batch_id",
        F.lit("batch_001")
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws("||", *ae_df.columns),
            256
        )
    )
)

(
    bronze_ae_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "clinical_trials_dev.bronze.bronze_adverse_events"
    )
)



# COMMAND ----------

# DBTITLE 1,Protocol_Deviations
protocol_deviations_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("includeMetadata", "true")
    .load(
        "dbfs:/Volumes/clinical_trials_dev/bronze/raw_files/protocol_deviations.csv"
    )
)

bronze_protocol_deviations_df = (
    protocol_deviations_df
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_file_name",
        F.col("_metadata.file_path")
    )
    .withColumn(
        "batch_id",
        F.lit("batch_001")
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *protocol_deviations_df.columns
            ),
            256
        )
    )
)

(
    bronze_protocol_deviations_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "clinical_trials_dev.bronze.bronze_protocol_deviations"
    )
)

# COMMAND ----------

# DBTITLE 1,Drug_inventory
drug_inventory_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("includeMetadata", "true")
    .load(
        "dbfs:/Volumes/clinical_trials_dev/bronze/raw_files/drug_inventory.csv"
    )
)

bronze_drug_inventory_df = (
    drug_inventory_df
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_file_name",
        F.col("_metadata.file_path")
    )
    .withColumn(
        "batch_id",
        F.lit("batch_001")
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *drug_inventory_df.columns
            ),
            256
        )
    )
)

(
    bronze_drug_inventory_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "clinical_trials_dev.bronze.bronze_drug_inventory"
    )
)


# COMMAND ----------

# DBTITLE 1,Drug shipments
drug_shipments_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("includeMetadata", "true")
    .load(
        "dbfs:/Volumes/clinical_trials_dev/bronze/raw_files/drug_shipments.csv"
    )
)

bronze_drug_shipments_df = (
    drug_shipments_df
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_file_name",
        F.col("_metadata.file_path")
    )
    .withColumn(
        "batch_id",
        F.lit("batch_001")
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *drug_shipments_df.columns
            ),
            256
        )
    )
)

(
    bronze_drug_shipments_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "clinical_trials_dev.bronze.bronze_drug_shipments"
    )
)



# COMMAND ----------

# DBTITLE 1,Lab results
lab_results_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("includeMetadata", "true")
    .load(
        "dbfs:/Volumes/clinical_trials_dev/bronze/raw_files/lab_results.csv"
    )
)

bronze_lab_results_df = (
    lab_results_df
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_file_name",
        F.col("_metadata.file_path")
    )
    .withColumn(
        "batch_id",
        F.lit("batch_001")
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *lab_results_df.columns
            ),
            256
        )
    )
)

(
    bronze_lab_results_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "clinical_trials_dev.bronze.bronze_lab_results"
    )
)




# COMMAND ----------

# DBTITLE 1,Reference codes
reference_codes_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("includeMetadata", "true")
    .load(
        "dbfs:/Volumes/clinical_trials_dev/bronze/raw_files/reference_codes.csv"
    )
)

bronze_reference_codes_df = (
    reference_codes_df
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_file_name",
        F.col("_metadata.file_path")
    )
    .withColumn(
        "batch_id",
        F.lit("batch_001")
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *reference_codes_df.columns
            ),
            256
        )
    )
)

(
    bronze_reference_codes_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "clinical_trials_dev.bronze.bronze_reference_codes"
    )
)


# COMMAND ----------

patients_df.count()