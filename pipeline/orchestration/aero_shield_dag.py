"""
Aero Shield v1.8 — Airflow Orchestration DAG
==============================================
Schedules and coordinates the full data pipeline:

  Lambda Ingestion (APIs -> S3 raw)
       |
  Great Expectations Validation
       |
  AWS Glue Transformation (S3 raw -> S3 curated)
       |
  dbt Analytics Models (S3 curated -> Redshift)
       |
  Streamlit Dashboard Refresh

Schedule: Daily at 06:00 UTC (catches overnight Indian air quality readings).
Owner: Sunil Kaimootil
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.models import Variable

# ---------------------------------------------------------------------------
# DAG defaults
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "sunil.kaimootil",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_great_expectations_validation(**context) -> None:
    """
    Validate raw S3 data before transformation.
    Raises AirflowException on validation failure to halt the pipeline.
    """
    import json
    import great_expectations as gx
    from airflow.exceptions import AirflowException

    run_ts = context["ti"].xcom_pull(task_ids="ingest_from_apis", key="run_timestamp")
    context["ti"].log.info(f"Running GE validation for run: {run_ts}")

    ctx = gx.get_context()
    results = ctx.run_checkpoint(
        checkpoint_name="aero_shield_raw_checkpoint",
        batch_request={"run_timestamp": run_ts},
    )

    if not results["success"]:
        failed = [
            r for r in results.run_results.values()
            if not r["validation_result"]["success"]
        ]
        raise AirflowException(
            f"Great Expectations validation failed. "
            f"{len(failed)} expectation suite(s) did not pass. "
            f"Halting pipeline to prevent bad data reaching curated zone."
        )

    context["ti"].log.info("All validations passed. Proceeding to transformation.")


def run_dbt_models(**context) -> None:
    """
    Invoke dbt to build analytics models in Redshift.
    Runs: dbt run --select staging.* marts.*
    """
    import subprocess

    result = subprocess.run(
        [
            "dbt", "run",
            "--project-dir", "/opt/airflow/dags/pipeline/dbt",
            "--profiles-dir", "/opt/airflow/dags/pipeline/dbt",
            "--select", "staging.*", "marts.*",
            "--vars", '{"run_date": "' + context["ds"] + '"}'
        ],
        capture_output=True,
        text=True,
    )

    context["ti"].log.info(result.stdout)
    if result.returncode != 0:
        from airflow.exceptions import AirflowException
        raise AirflowException(f"dbt run failed:\n{result.stderr}")


def run_dbt_tests(**context) -> None:
    """Run dbt tests to validate mart outputs before dashboard refresh."""
    import subprocess

    result = subprocess.run(
        [
            "dbt", "test",
            "--project-dir", "/opt/airflow/dags/pipeline/dbt",
            "--profiles-dir", "/opt/airflow/dags/pipeline/dbt",
            "--select", "marts.*",
        ],
        capture_output=True,
        text=True,
    )

    context["ti"].log.info(result.stdout)
    if result.returncode != 0:
        from airflow.exceptions import AirflowException
        raise AirflowException(f"dbt tests failed:\n{result.stderr}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="aero_shield_daily_pipeline",
    description="Aero Shield v1.8 — Daily air quality ingestion and risk modeling",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",      # 06:00 UTC daily
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["aero-shield", "air-quality", "health-analytics"],
    doc_md=__doc__,
) as dag:

    # ── Step 1: Ingest from APIs into S3 raw zone ──
    ingest = LambdaInvokeFunctionOperator(
        task_id="ingest_from_apis",
        function_name="aero-shield-ingestion",
        payload='{"target_year_month": "{{ macros.ds_format(ds, \"%Y-%m-%d\", \"%Y-%m\") }}"}',
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        do_xcom_push=True,
    )

    # ── Step 2: Validate raw data with Great Expectations ──
    validate = PythonOperator(
        task_id="validate_raw_data",
        python_callable=run_great_expectations_validation,
    )

    # ── Step 3: Transform raw S3 -> curated S3 via AWS Glue ──
    transform = GlueJobOperator(
        task_id="transform_raw_to_curated",
        job_name="aero-shield-transformation",
        script_location="s3://aero-shield-assets/glue/glue_transform.py",
        s3_bucket="aero-shield-assets",
        aws_conn_id="aws_default",
        script_args={
            "--run_date": "{{ ds }}",
            "--raw_bucket": Variable.get("S3_RAW_BUCKET", "aero-shield-raw-zone"),
            "--curated_bucket": Variable.get("S3_CURATED_BUCKET", "aero-shield-curated-zone"),
        },
        concurrent_run_limit=1,
        retry_limit=1,
    )

    # ── Step 4: Build dbt analytics models in Redshift ──
    dbt_run = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt_models,
    )

    # ── Step 5: Test dbt marts ──
    dbt_test = PythonOperator(
        task_id="test_dbt_models",
        python_callable=run_dbt_tests,
    )

    # ── Step 6: Refresh Redshift materialized view for dashboard ──
    refresh_dashboard_view = RedshiftSQLOperator(
        task_id="refresh_dashboard_view",
        sql="""
            REFRESH MATERIALIZED VIEW analytics.mv_worker_exposure_daily;
        """,
        redshift_conn_id="redshift_default",
    )

    # ── Pipeline dependency chain ──
    ingest >> validate >> transform >> dbt_run >> dbt_test >> refresh_dashboard_view
