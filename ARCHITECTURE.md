# Architecture: Aero Shield v1.8 Intelligence Platform

Mathematical frameworks, engineering rationale, and data flow specifications.

---

## 1. Data Harvesting Layer (OpenAQ v3 Transition)

The foundation of the platform is a multi-stage discovery engine designed for the **OpenAQ v3 API**. Unlike legacy systems that rely on cached or stale measurements, this engine uses a "Live Pulse" harvesting strategy:

- **Real-Time Verification:** Strict temporal gatekeeper discards any sensor metadata lacking a validated `2026-03` timestamp.
- **Full Spectrum Monitoring:** Beyond PM2.5, the system captures NO₂, O₃, and PM10 for holistic urban atmospheric chemistry.
- **Sensor ID Mapping:** OpenAQ v3 uses parameter-to-sensor-ID mapping which the ingestion layer reconciles via join on the `sensors[].id` → `latest[].sensorsId` relationship.

---

## 2. Atmospheric Intelligence Layer

Moves the model from "ambient air quality" to "worker-level exposure" by integrating three atmospheric-physics signals:

### Ventilation Index

```
Mixing Height (m) = max(10, (max(2, temp_C − 15) × 150) / (pressure_hPa / 1000))
Ventilation Index = Wind Speed (m/s) × Mixing Height (m)
```

Values below 1000 m²/s indicate **stagnant air** — the "Invisible Ceiling" effect where pollutants accumulate at breathing level rather than dispersing.

### Urban Morphology

Current implementation uses a **rule-based urban classifier** against curated keywords for known Indian metro corridors, mapping to fixed Street Aspect Ratios:

| Urban Class                    | SAR | Representative Pop. Density (p/km²) |
| ------------------------------ | --- | ----------------------------------- |
| Dense Residential/Commercial   | 2.5 | 22,000                              |
| High-Rise Business District    | 3.5 | 15,000                              |
| Urban Mixed                    | 1.2 | 11,000                              |
| Open Space/Park                | 0.3 | 3,000                               |

Future work will replace the rule-based classifier with OpenStreetMap building-footprint data for data-driven aspect ratios.

### Traffic Source Penalties

TomTom Functional Road Classes (FRC) drive the source-proximity penalty:
- **FRC1/FRC2** (arterial / heavy commercial): 1.3× multiplier
- **FRC3+** (secondary / residential): 1.1× multiplier

This captures elevated diesel-soot exposure for workers near high-volume trucking corridors.

---

## 3. Biomedical Impact Engine (Cigarette Equivalence)

The project's core innovation: translating raw µg/m³ data into a human-centric health narrative.

### Active Worker Persona

Replaces the "resting adult" assumption (default in most epidemiological literature) with an **Active Worker Persona** at 25 L/min breathing rate — 3× the resting intake, reflecting sustained exertion by delivery riders, construction workers, and street vendors.

### Berkeley Earth Conversion

Uses the validated Berkeley Earth factor: **22 µg/m³ PM2.5 breathed for 24 hours ≈ 1 cigarette smoked**.

```
Inhaled Dose (µg/hr) = PM2.5 × 1.5 m³/hr × stagnation_penalty × source_penalty
Shift Cigs = (Inhaled Dose × Shift Hours) / (22 × 24)
```

### Hardware Grounding

In the interest of technical honesty, the platform models the **Aero Shield v1.7.1 E11 Pleated Filter** at a realistic **32.5% reduction** (rated filter efficiency minus Total Inward Leakage) — not at the nameplate lab-bench rating of 80%+.

---

## 4. Strategic Ranking Layer (TAM Index)

Final output is an Executive Launch Dashboard ranking cities by:

```
TAM_Index = (OWPEI × Population Density) / 1000

where:
    OWPEI = PM2.5 × stagnation_penalty × source_penalty × (SAR / 2)
```

This combines **intensity of health risk** with **density of at-risk workers** to surface highest-ROI launch markets. The top-ranked market in the March 2026 sample is **Vikas Sadan, Gurugram**, with an OWPEI of approximately 345 and a TAM Index of ~5,175.

---

## 5. Pipeline Data Flow (AWS)

```
EventBridge (hourly schedule)
    │
    ▼
Lambda Handler (pipeline/ingestion/lambda_handler.py)
    │  — fan-out: OpenAQ locations → per-location weather & traffic calls
    │  — writes: 3 partitioned JSON files per run
    ▼
S3 Raw Zone (s3://aero-shield-raw-zone/)
    └── year=YYYY/month=MM/day=DD/hour=HH/
        ├── pollution_<ts>.json
        ├── weather_<ts>.json
        └── traffic_<ts>.json
    │
    ▼
Great Expectations (pipeline/validation/expectations_suite.py)
    │  — validates PM2.5 range, non-null location, valid timestamps
    │  — fails pipeline if bad data detected
    ▼
AWS Glue / PySpark (pipeline/transformation/glue_transform.py)
    │  — applies src/models.py calculations at scale
    │  — writes Parquet with partitioning
    ▼
S3 Curated Zone (s3://aero-shield-curated-zone/)
    └── worker_exposure/dt=YYYY-MM-DD/
        └── *.parquet
    │
    ▼
Amazon Redshift (via Spectrum external schema)
    │
    ▼
dbt Models (pipeline/dbt/)
    ├── staging/stg_pollution_readings.sql (view)
    └── marts/mart_worker_exposure.sql    (materialized table)
    │
    ▼
Streamlit Dashboard (dashboard/app.py)
    └── Reads from mart_worker_exposure via Redshift connector
```

---

## 6. Design Principles

### Single source of truth for math
All calculation logic lives in `src/models.py`. The notebook, AWS Glue transformation, dbt SQL models, and Streamlit dashboard all produce numerically identical outputs because they apply the same formulas — verified by the pytest suite.

### Fail-fast data quality
A health metric built on corrupt sensor data is worse than no metric at all. Great Expectations gates between raw and curated zones prevent bad data from propagating.

### Deterministic outputs
Population density uses fixed census-derived constants, not random sampling. Re-running the pipeline on the same data produces identical TAM Index rankings.

### Honest hardware modelling
Filter efficiency is set to 32.5% (realistic field performance), not the 80%+ nameplate lab rating. Technical honesty sustains credibility with investors and partners.

---

**Designed and engineered by Sunil Kaimootil** · Aero Shield Project
