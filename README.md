# Aero Shield v1.8: National Air Intelligence & Bio-Metric Engine

## Overview
Aero Shield is an environmental data platform designed to quantify the health risks of outdoor workers in urban India. By integrating real-time air quality, atmospheric physics, and urban mobility data, the platform translates raw pollutant concentrations into a human-centric health narrative: **Shift Cigarette Equivalence.**

This engine identifies high-priority markets for the Aero Shield wearable device (v1.8) by mapping the intersection of extreme pollution, stagnant ventilation, and high population density.

## Technical Architecture
The platform is built on a 6-layer geospatial pipeline:

1.  **National Discovery Layer:** A live harvesting engine for the **OpenAQ v3 API** that secures verified March 2026 multi-pollutant profiles ($PM_{2.5}, PM_{10}, NO_2, O_3$).
2.  **Atmospheric Intelligence:** Merges barometric and wind data to calculate the **Ventilation Index** and **Mixing Height**, identifying "Invisible Ceilings" that trap pollutants.
3.  **Urban Morphology:** Models "Urban Canyons" by integrating **Street Aspect Ratios** and **Population Density** data.
4.  **Mobility Layer:** Leverages **TomTom Pro Traffic API** to identify **Functional Road Classes (FRC)**, applying source penalties for workers near heavy-duty arterial corridors.
5.  **Biomedical Impact Engine:** Translates exposure into **Cigarette Equivalence** based on an **Active Worker Persona** ($25 L/min$ breathing rate) and Berkeley Earth research.
6.  **Grounded Hardware Modeling:** Calculates the life-saving impact of the **Aero Shield v1.7.1 E11 Pleated Filter**, accounting for a realistic $32.5\%$ reduction in total inhaled mass.

## Key Metrics
* **OWPEI:** Outdoor Worker Pollution Exposure Index (A composite risk score).
* **TAM Index:** Market Opportunity score (Risk $\times$ Population Density).
* **Cigs Prevented:** The specific health ROI of the Aero Shield device per 8-hour shift.

## Strategic Discovery (March 2026 Sample)
Based on real-time data harvesting, the platform identified **Vikas Sadan, Gurugram** as the highest-priority launch site.
* **Baseline Risk:** 7.85 Cigarettes / 8-hour shift.
* **Aero Shield Impact:** Prevents ~15.3 cigarettes worth of soot inhalation per week.
* **Narrative:** Moving workers from "Extreme Risk" to "Managed Risk" via hardware intervention.

## Tech Stack
* **Language:** Python 3.12
* **Data Science:** Pandas, NumPy
* **APIs:** OpenAQ v3, OpenWeatherMap, TomTom Pro
* **Visualization:** Folium (Interactive Geospatial Maps), Matplotlib

---
*Designed and Engineered by Sunil Kaimootil — Aero Shield Project.*
