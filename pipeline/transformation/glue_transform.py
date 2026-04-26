"""
Aero Shield v1.8 — AWS Glue / PySpark Transformation Job
==========================================================
Reads raw JSON from S3 raw zone, applies all calculation models,
and writes Parquet to S3 curated zone — partitioned for efficient
Redshift Spectrum and Athena querying.

Triggered by: Airflow GlueJobOperator (after GE validation passes).

Arguments (passed by Airflow):
    --run_date       YYYY-MM-DD of the pipeline run
    --raw_bucket     S3 bucket for raw JSON (input)
    --curated_bucket S3 bucket for curated Parquet (output)

Usage (local testing with mock data):
    python pipeline/transformation/glue_transform.py \
        --run_date 2026-03-15 \
        --raw_bucket aero-shield-raw-zone \
        --curated_bucket aero-shield-curated-zone \
        --local_mode true
"""

from __future__ import annotations

import sys
import os
import logging
from datetime import datetime

# Glue context setup (no-op when running locally)
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    GLUE_AVAILABLE = True
except ImportError:
    GLUE_AVAILABLE = False

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# Calculation models reused from src/ (also available in Glue via --extra-py-files)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from src.models import (
    calculate_mixing_height,
    calculate_ventilation_index,
    calculate_stagnation_penalty,
    calculate_source_penalty,
    calculate_inhaled_dose,
    calculate_cigarette_equivalence,
    calculate_protected_exposure,
    calculate_cigs_prevented,
    calculate_owpei,
    calculate_tam_index,
    classify_toxic_status,
    classify_risk_tier,
)
from src.utils import classify_urban_type, get_aspect_ratio, get_population_density

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# PySpark UDFs wrapping models.py functions
# ---------------------------------------------------------------------------

def register_udfs(spark: SparkSession):
    """Register all calculation functions as Spark UDFs."""

    spark.udf.register("udf_mixing_height",
        lambda t, p: float(calculate_mixing_height(t, p)), DoubleType())

    spark.udf.register("udf_ventilation_index",
        lambda w, h: float(calculate_ventilation_index(w, h)), DoubleType())

    spark.udf.register("udf_stagnation_penalty",
        lambda vi: float(calculate_stagnation_penalty(vi)), DoubleType())

    spark.udf.register("udf_source_penalty",
        lambda frc: float(calculate_source_penalty(frc)), DoubleType())

    spark.udf.register("udf_inhaled_dose",
        lambda pm25, stag, src: float(calculate_inhaled_dose(pm25, stag, src)), DoubleType())

    spark.udf.register("udf_cigarette_equiv",
        lambda dose: float(calculate_cigarette_equivalence(dose)), DoubleType())

    spark.udf.register("udf_protected_exposure",
        lambda cigs: float(calculate_protected_exposure(cigs)), DoubleType())

    spark.udf.register("udf_cigs_prevented",
        lambda cigs: float(calculate_cigs_prevented(cigs)), DoubleType())

    spark.udf.register("udf_owpei",
        lambda pm25, stag, src, sar: float(calculate_owpei(pm25, stag, src, sar)), DoubleType())

    spark.udf.register("udf_tam_index",
        lambda owpei, pop: float(calculate_tam_index(owpei, pop)), DoubleType())

    spark.udf.register("udf_toxic_status",
        lambda pm25: classify_toxic_status(pm25), StringType())

    spark.udf.register("udf_risk_tier",
        lambda cigs: classify_risk_tier(cigs), StringType())

    spark.udf.register("udf_urban_class",
        lambda name: classify_urban_type(name), StringType())

    spark.udf.register("udf_aspect_ratio",
        lambda cls: float(get_aspect_ratio(cls)), DoubleType())

    spark.udf.register("udf_pop_density",
        lambda cls: float(get_population_density(cls)), DoubleType())

    logger.info("All UDFs registered.")


# ---------------------------------------------------------------------------
# Transformation logic
# ---------------------------------------------------------------------------

def transform(spark: SparkSession, run_date: str, raw_bucket: str, curated_bucket: str):
    """
    Full transformation pipeline:
    1. Read raw JSON from S3 raw zone
    2. Join pollution + weather + traffic layers
    3. Apply calculation UDFs
    4. Write curated Parquet to S3 curated zone
    """
    year, month, day = run_date.split("-")
    raw_prefix = f"s3://{raw_bucket}/raw/{{layer}}/year={year}/month={month}/day={day}/"

    # ── Read raw layers ──
    logger.info("Reading raw pollution data...")
    df_pollution = spark.read.json(raw_prefix.format(layer="pollution"))

    logger.info("Reading raw weather data...")
    df_weather = spark.read.json(raw_prefix.format(layer="weather"))

    logger.info("Reading raw traffic data...")
    df_traffic = spark.read.json(raw_prefix.format(layer="traffic"))

    # ── Register UDFs ──
    register_udfs(spark)

    # ── Weather calculations ──
    df_weather = df_weather \
        .withColumn("mixing_height_m",
            F.expr("udf_mixing_height(temperature_c, pressure_hpa)")) \
        .withColumn("ventilation_index",
            F.expr("udf_ventilation_index(wind_speed_ms, mixing_height_m)"))

    # ── Join all layers ──
    df = df_pollution \
        .join(df_weather.select(
            "location_name", "run_timestamp",
            "wind_speed_ms", "pressure_hpa", "temperature_c", "humidity_pct",
            "mixing_height_m", "ventilation_index"
        ), on=["location_name", "run_timestamp"], how="inner") \
        .join(df_traffic.select(
            "location_name", "run_timestamp",
            "road_type_frc", "current_speed_kmh", "current_delay_sec"
        ), on=["location_name", "run_timestamp"], how="inner")

    # ── Urban morphology ──
    df = df \
        .withColumn("urban_class", F.expr("udf_urban_class(location_name)")) \
        .withColumn("street_aspect_ratio", F.expr("udf_aspect_ratio(urban_class)")) \
        .withColumn("pop_density_km2", F.expr("udf_pop_density(urban_class)"))

    # ── Penalty models ──
    df = df \
        .withColumn("stagnation_penalty",
            F.expr("udf_stagnation_penalty(ventilation_index)")) \
        .withColumn("source_penalty",
            F.expr("udf_source_penalty(road_type_frc)"))

    # ── Biomedical calculations ──
    df = df \
        .withColumn("inhaled_dose_ug_hr",
            F.expr("udf_inhaled_dose(pm25, stagnation_penalty, source_penalty)")) \
        .withColumn("shift_cigarette_equiv",
            F.expr("udf_cigarette_equiv(inhaled_dose_ug_hr)")) \
        .withColumn("protected_cigarette_equiv",
            F.expr("udf_protected_exposure(shift_cigarette_equiv)")) \
        .withColumn("cigs_prevented_per_shift",
            F.expr("udf_cigs_prevented(shift_cigarette_equiv)"))

    # ── Composite indices ──
    df = df \
        .withColumn("owpei",
            F.expr("udf_owpei(pm25, stagnation_penalty, source_penalty, street_aspect_ratio)")) \
        .withColumn("aero_shield_tam_index",
            F.expr("udf_tam_index(owpei, pop_density_km2)"))

    # ── Classification ──
    df = df \
        .withColumn("toxic_status", F.expr("udf_toxic_status(pm25)")) \
        .withColumn("risk_tier", F.expr("udf_risk_tier(shift_cigarette_equiv)"))

    # ── Add pipeline metadata ──
    df = df \
        .withColumn("pipeline_run_date", F.lit(run_date)) \
        .withColumn("processed_at", F.current_timestamp())

    # ── Write to S3 curated zone as Parquet (partitioned by date) ──
    output_path = (
        f"s3://{curated_bucket}/curated/worker_exposure"
        f"/year={year}/month={month}/day={day}/"
    )
    logger.info(f"Writing curated data to: {output_path}")

    df.write \
        .mode("overwrite") \
        .parquet(output_path)

    record_count = df.count()
    logger.info(f"Transformation complete. {record_count} records written to curated zone.")
    return record_count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if GLUE_AVAILABLE:
        args = getResolvedOptions(sys.argv, [
            "JOB_NAME", "run_date", "raw_bucket", "curated_bucket"
        ])
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        job.init(args["JOB_NAME"], args)
    else:
        # Local testing mode
        args = {
            "run_date": "2026-03-15",
            "raw_bucket": os.environ.get("S3_RAW_BUCKET", "aero-shield-raw-zone"),
            "curated_bucket": os.environ.get("S3_CURATED_BUCKET", "aero-shield-curated-zone"),
        }
        spark = SparkSession.builder \
            .appName("AeroShieldTransformation") \
            .master("local[*]") \
            .getOrCreate()
        logger.info("Running in LOCAL mode (no AWS Glue context).")

    transform(
        spark=spark,
        run_date=args["run_date"],
        raw_bucket=args["raw_bucket"],
        curated_bucket=args["curated_bucket"],
    )

    if GLUE_AVAILABLE:
        job.commit()
