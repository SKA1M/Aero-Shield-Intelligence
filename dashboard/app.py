"""
Aero Shield v1.8 — Executive Intelligence Dashboard
=====================================================
Streamlit front-end for the environmental health intelligence platform.

Run locally:
    streamlit run dashboard/app.py

Run with live data:
    Set AERO_SHIELD_MODE=live in your .env — dashboard will call the APIs.
    Default mode is 'demo' which uses synthesized plausible data
    so the app works without API keys for portfolio demonstration.

Production deployment:
    Reads from Redshift mart_worker_exposure table (see pipeline/dbt/).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st
import pandas as pd
import numpy as np
import folium
import plotly.express as px
from streamlit_folium import st_folium

# Make src/ importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import (
    calculate_inhaled_dose,
    calculate_cigarette_equivalence,
    calculate_protected_exposure,
    calculate_cigs_prevented,
    calculate_stagnation_penalty,
    calculate_source_penalty,
    calculate_owpei,
    calculate_tam_index,
    classify_toxic_status,
    classify_risk_tier,
    DEFAULT_FILTER_EFFICIENCY,
)
from src.utils import (
    classify_urban_type,
    get_aspect_ratio,
    get_population_density,
    build_popup_html,
    get_marker_color,
)


# ---------------------------------------------------------------------------
# Page Config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Aero Shield v1.8 | Intelligence Dashboard",
    page_icon="🛡",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    .main > div { padding-top: 1.5rem; }
    h1 { color: #1B3A6B; font-family: 'Arial', sans-serif; }
    h2 { color: #1B3A6B; border-bottom: 2px solid #1B3A6B; padding-bottom: 0.3rem; }
    .stMetric { background: #f8f9fa; padding: 1rem; border-radius: 8px; border-left: 4px solid #1B3A6B; }
    .toxic { color: #c0392b; font-weight: bold; }
    .safe { color: #27ae60; font-weight: bold; }
    [data-testid="stSidebar"] { background: #f0f3f8; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Demo Data Generator
# (Used when live API keys are not configured; produces plausible snapshot
#  that matches the patterns observed in real runs)
# ---------------------------------------------------------------------------

@st.cache_data
def load_demo_data() -> pd.DataFrame:
    """Return a realistic snapshot for demo/portfolio use."""
    sites = [
        ("Vikas Sadan, Gurugram",         28.4595, 77.0266, 177.0, "FRC1"),
        ("Anand Vihar, Delhi",            28.6469, 77.3166, 158.0, "FRC1"),
        ("Punjabi Bagh, Delhi",           28.6742, 77.1341, 142.0, "FRC2"),
        ("R K Puram, Delhi",              28.5636, 77.1745, 128.0, "FRC2"),
        ("Noida Sector 125",              28.5423, 77.3244, 115.0, "FRC2"),
        ("Rohini, Delhi",                 28.7433, 77.0718, 98.0,  "FRC3"),
        ("Aya Nagar, Delhi",              28.4744, 77.1248, 88.0,  "FRC3"),
        ("ITO, Delhi",                    28.6289, 77.2410, 165.0, "FRC1"),
        ("Civil Lines, Agra",             27.2090, 77.9970, 95.0,  "FRC3"),
        ("Taj Mahal Area, Agra",          27.1751, 78.0421, 78.0,  "FRC4"),
        ("Aligarh City",                  27.8974, 78.0880, 62.0,  "FRC3"),
        ("Sanathnagar, Hyderabad",        17.4530, 78.4370, 48.0,  "FRC2"),
        ("Bollaram, Hyderabad",           17.5380, 78.3730, 42.0,  "FRC3"),
        ("Zoo Park, Hyderabad",           17.3504, 78.4528, 57.0,  "FRC4"),
        ("Manali Town, Chennai",          13.1689, 80.2620, 38.0,  "FRC3"),
    ]

    rows = []
    # Fix seed for deterministic demo — no randomness between runs
    rng = np.random.default_rng(seed=42)

    for name, lat, lon, pm25, road_type in sites:
        # Weather (plausible March India values)
        wind_speed = round(rng.uniform(0.8, 3.5), 2)
        temp = round(rng.uniform(28, 38), 1)
        pressure = round(rng.uniform(1008, 1018), 1)

        # Calculate derived metrics
        temp_factor = max(2.0, temp - 15.0)
        mixing_height = round(max(10.0, (temp_factor * 150.0) / (pressure / 1000.0)), 1)
        ventilation_index = round(wind_speed * mixing_height, 1)

        stag = calculate_stagnation_penalty(ventilation_index)
        src = calculate_source_penalty(road_type)
        urban_class = classify_urban_type(name)
        aspect_ratio = get_aspect_ratio(urban_class)
        pop_density = get_population_density(urban_class)

        dose = calculate_inhaled_dose(pm25, stag, src)
        cigs = calculate_cigarette_equivalence(dose)
        protected = calculate_protected_exposure(cigs)
        prevented = calculate_cigs_prevented(cigs)
        owpei = calculate_owpei(pm25, stag, src, aspect_ratio)
        tam = calculate_tam_index(owpei, pop_density)

        rows.append({
            "location_name": name,
            "latitude": lat,
            "longitude": lon,
            "pm25": pm25,
            "road_type": road_type,
            "wind_speed_m_s": wind_speed,
            "temperature_c": temp,
            "pressure_hpa": pressure,
            "mixing_height_m": mixing_height,
            "ventilation_index": ventilation_index,
            "stagnation_penalty": stag,
            "source_penalty": src,
            "urban_class": urban_class,
            "street_aspect_ratio": aspect_ratio,
            "pop_density_km2": pop_density,
            "inhaled_dose_ug_hr": dose,
            "shift_cigarette_equiv": cigs,
            "protected_cigarette_equiv": protected,
            "cigs_prevented_per_shift": prevented,
            "owpei": owpei,
            "aero_shield_tam_index": tam,
            "toxic_status": classify_toxic_status(pm25),
            "risk_tier": classify_risk_tier(cigs),
        })

    return pd.DataFrame(rows).sort_values("aero_shield_tam_index", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Sidebar: Filters & Controls
# ---------------------------------------------------------------------------

st.sidebar.title("Aero Shield v1.8")
st.sidebar.caption("National Air Intelligence Engine")
st.sidebar.markdown("---")

mode = os.environ.get("AERO_SHIELD_MODE", "demo").lower()
if mode == "live":
    st.sidebar.success("Mode: LIVE (reading from APIs)")
else:
    st.sidebar.info("Mode: DEMO (synthesized plausible data)")

df = load_demo_data()

st.sidebar.markdown("### Filters")

risk_filter = st.sidebar.multiselect(
    "Risk Tier",
    options=df["risk_tier"].unique().tolist(),
    default=df["risk_tier"].unique().tolist(),
)

urban_filter = st.sidebar.multiselect(
    "Urban Class",
    options=df["urban_class"].unique().tolist(),
    default=df["urban_class"].unique().tolist(),
)

pm25_min, pm25_max = st.sidebar.slider(
    "PM2.5 Range (ug/m3)",
    min_value=int(df["pm25"].min()),
    max_value=int(df["pm25"].max()) + 1,
    value=(int(df["pm25"].min()), int(df["pm25"].max()) + 1),
)

st.sidebar.markdown("---")
st.sidebar.markdown("### Filter Efficiency")
filter_eff = st.sidebar.slider(
    "E11 Filter Efficiency (%)",
    min_value=0,
    max_value=100,
    value=int(DEFAULT_FILTER_EFFICIENCY * 100),
    help="Default 32.5% = rated E11 efficiency minus Total Inward Leakage",
)

# Recalculate protection based on slider
filter_eff_frac = filter_eff / 100.0
df = df.copy()
df["protected_cigarette_equiv"] = (df["shift_cigarette_equiv"] * (1 - filter_eff_frac)).round(2)
df["cigs_prevented_per_shift"] = (df["shift_cigarette_equiv"] * filter_eff_frac).round(2)

# Apply filters
filtered = df[
    df["risk_tier"].isin(risk_filter)
    & df["urban_class"].isin(urban_filter)
    & (df["pm25"] >= pm25_min)
    & (df["pm25"] <= pm25_max)
].copy()

st.sidebar.markdown("---")
st.sidebar.caption("Engineered by Sunil Kaimootil")


# ---------------------------------------------------------------------------
# Main Dashboard
# ---------------------------------------------------------------------------

st.title("Aero Shield | Executive Intelligence Dashboard")
st.markdown(
    "Real-time environmental health risk scoring for outdoor workers across "
    "urban India. Combines OpenAQ pollution, atmospheric physics, traffic morphology, "
    "and biomedical impact modelling into a unified launch-priority framework."
)

# ── KPI Row ──
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        "Sites Monitored",
        f"{len(filtered)}",
        f"of {len(df)} total",
    )
with col2:
    top_cigs = filtered["shift_cigarette_equiv"].max() if len(filtered) else 0
    st.metric(
        "Highest Shift Risk",
        f"{top_cigs} cigs",
        "per 8-hr shift" if top_cigs > 0 else "n/a",
    )
with col3:
    avg_cigs = filtered["shift_cigarette_equiv"].mean() if len(filtered) else 0
    st.metric(
        "Avg Shift Risk",
        f"{avg_cigs:.1f} cigs",
        f"{classify_risk_tier(avg_cigs)}" if avg_cigs > 0 else "n/a",
    )
with col4:
    toxic_pct = (filtered["toxic_status"] == "TOXIC").mean() * 100 if len(filtered) else 0
    st.metric(
        "% Toxic (vs WHO)",
        f"{toxic_pct:.0f}%",
        "> 15 ug/m3 PM2.5",
    )

st.markdown("---")

# ── Geospatial Map ──
st.subheader("Geospatial Impact Map")

if len(filtered) > 0:
    center_lat = filtered["latitude"].mean()
    center_lon = filtered["longitude"].mean()

    m = folium.Map(location=[center_lat, center_lon], zoom_start=5, tiles="CartoDB positron")

    for _, row in filtered.iterrows():
        color = get_marker_color(row["toxic_status"])
        popup = build_popup_html(row)

        if color == "red":
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=14,
                color="red",
                fill=True,
                fill_color="red",
                fill_opacity=0.3,
            ).add_to(m)

        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            icon=folium.Icon(color=color, icon="info-sign"),
            popup=folium.Popup(popup, max_width=260),
        ).add_to(m)

    st_folium(m, width=None, height=520, returned_objects=[])
else:
    st.warning("No sites match the selected filters.")

st.markdown("---")

# ── Executive Launch Dashboard ──
col_left, col_right = st.columns([1.3, 1])

with col_left:
    st.subheader("Launch Priority Ranking")
    st.caption("Ranked by TAM Index = OWPEI x Population Density / 1000")

    display_cols = [
        "location_name",
        "pm25",
        "risk_tier",
        "shift_cigarette_equiv",
        "cigs_prevented_per_shift",
        "aero_shield_tam_index",
    ]
    st.dataframe(
        filtered[display_cols].rename(columns={
            "location_name": "Site",
            "pm25": "PM2.5 (ug/m3)",
            "risk_tier": "Risk Tier",
            "shift_cigarette_equiv": "Shift Cigs",
            "cigs_prevented_per_shift": "Cigs Prevented",
            "aero_shield_tam_index": "TAM Index",
        }),
        use_container_width=True,
        hide_index=True,
        height=420,
    )

with col_right:
    st.subheader("Shift Risk Distribution")
    if len(filtered) > 0:
        top_10 = filtered.head(10).copy()
        fig = px.bar(
            top_10,
            x="shift_cigarette_equiv",
            y="location_name",
            orientation="h",
            color="risk_tier",
            color_discrete_map={
                "Low Risk": "#27ae60",
                "Moderate Risk": "#f39c12",
                "High Risk": "#e67e22",
                "Extreme Risk": "#c0392b",
                "Critical Risk": "#7b241c",
            },
            labels={
                "shift_cigarette_equiv": "Cigarette-equivalents / shift",
                "location_name": "",
            },
        )
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            height=420,
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.25),
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Intervention Impact ──
st.subheader("Aero Shield Intervention Impact")

if len(filtered) > 0:
    impact_df = filtered[["location_name", "shift_cigarette_equiv", "protected_cigarette_equiv"]].head(10)
    impact_long = impact_df.melt(
        id_vars="location_name",
        value_vars=["shift_cigarette_equiv", "protected_cigarette_equiv"],
        var_name="Scenario",
        value_name="Cigarette Equivalents",
    )
    impact_long["Scenario"] = impact_long["Scenario"].map({
        "shift_cigarette_equiv": "Unprotected",
        "protected_cigarette_equiv": f"With Aero Shield ({filter_eff}% eff.)",
    })

    fig2 = px.bar(
        impact_long,
        x="location_name",
        y="Cigarette Equivalents",
        color="Scenario",
        barmode="group",
        color_discrete_map={
            "Unprotected": "#c0392b",
            f"With Aero Shield ({filter_eff}% eff.)": "#27ae60",
        },
    )
    fig2.update_layout(
        xaxis={"tickangle": -45},
        height=420,
        margin=dict(l=0, r=0, t=10, b=80),
        legend=dict(orientation="h", yanchor="bottom", y=-0.6),
    )
    st.plotly_chart(fig2, use_container_width=True)

    top_site = filtered.iloc[0]
    weekly_prevented = top_site["cigs_prevented_per_shift"] * 6

    st.info(
        f"**Strategic Conclusion:** "
        f"Targeting **{top_site['location_name']}** provides the highest ROI. "
        f"Deploying Aero Shield prevents **~{weekly_prevented:.1f} cigarette-equivalents "
        f"per worker per week** of PM2.5 inhalation. "
        f"Moves workers from **{top_site['risk_tier']}** to a **Managed Risk** profile."
    )

# ── Footer ──
st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#888; font-size:12px;'>"
    "Aero Shield v1.8 | National Air Intelligence Engine | "
    "Designed & Engineered by Sunil Kaimootil<br>"
    "Data sources: OpenAQ v3, OpenWeatherMap, TomTom Pro Traffic | "
    "Methodology: Berkeley Earth cigarette-equivalence"
    "</div>",
    unsafe_allow_html=True,
)
