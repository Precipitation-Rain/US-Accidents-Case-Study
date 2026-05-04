# 🚗 US Accidents — End-to-End Data Science Project

> A complete data science project on 7.7 million US traffic accidents — from raw messy data to cleaned datasets, deep exploratory analysis, an interactive Streamlit dashboard, and an upcoming ML severity predictor.

---

## 📌 Table of Contents

- [Project Overview](#-project-overview)
- [Dataset](#-dataset)
- [Project Flow](#-project-flow)
- [Project Structure](#-project-structure)
- [Project in Detail](#-project-in-detail)
- [Tech Stack](#-tech-stack)
- [How to Run](#-how-to-run)
- [Key Insights](#-key-insights)
- [Author](#-author)

---

## 🧭 Project Overview

Road accidents are one of the leading causes of fatalities in the United States. This project performs a full data science lifecycle on 7.7 million US accidents recorded across 49 states from 2016 to 2023 — starting from raw, messy data and ending at an interactive dashboard with an ML model coming next.

The pipeline covers four parts: **Data Cleaning** (fixing quality issues and engineering new features), **Exploratory Data Analysis** (finding patterns in time, weather, geography, and severity), **Interactive Dashboard** (a 6-page Streamlit app for visual exploration), and **ML Model** *(Coming Soon)* (predicting accident severity from road and weather conditions).

---

## 📦 Dataset

| Field | Details |
|---|---|
| **Name** | US-Accidents: A Countrywide Traffic Accident Dataset |
| **Dataset Author** | Sobhan Moosavi, Ohio State University |
| **Version** | V8 (2023) |
| **Records** | ~7.7 Million |
| **Period** | February 2016 – March 2023 |
| **Geography** | 49 US States |
| **Original Columns** | 46 |
| **License** | CC BY-NC-SA 4.0 (Research use only) |

---

## 🔄 Project Flow

```
US_Accidents.csv
       │
       ▼
┌──────────────────────┐
│  Part 1 — Cleaning    │  → Cleaned_Dataset.csv
│  Data_Cleaning.ipynb  │  → ML_Ready_Dataset.csv
└──────────────────────┘
       │
       ▼
┌──────────────────────┐
│  Part 2 — EDA         │  → Patterns & Insights
│  EDA.ipynb            │
└──────────────────────┘
       │
       ▼
┌──────────────────────┐
│  Part 3 — Dashboard   │  → Cleaned_Dataset → accidents.parquet
│  app.py               │     Queried live with DuckDB
└──────────────────────┘
       │
       ▼
┌──────────────────────┐
│  Part 4 — ML Model    │  🚧 Coming Soon
└──────────────────────┘
```

> **Note:** The Parquet file is created only for the dashboard — to make querying 7.7 million rows fast. This step is separate from data cleaning.

---

## 📁 Project Structure

```
├── app.py                           # Streamlit dashboard (Part 3)
├── Data_Cleaning.ipynb              # Data cleaning & feature engineering (Part 1)
├── Exploratory_Data_Analysis.ipynb  # EDA notebook (Part 2)
├── Cleaned_Dataset.csv              # Cleaned data — used for EDA
├── ML_Ready_Dataset.csv             # Feature-engineered data — used for ML
├── accidents.parquet                # Parquet file — used by dashboard
└── requirements.txt                 # Python dependencies
```

---

## 🔍 Project in Detail

### Part 1 — Data Cleaning

**Notebook:** `Data_Cleaning.ipynb`

The raw dataset had serious quality issues that needed fixing before any analysis. Key problems found and resolved:

- **Missing values** — lakhs of nulls across weather columns (temperature, humidity, wind speed, visibility) and location fields, all handled appropriately
- **Impossible entries** — wind speeds above 300 mph, pressure values of 0, and other physically impossible readings corrected
- **Abbreviations** — state codes (`CA`, `TX`), wind directions (`N`, `SW`, `ENE`), and timezone formats (`US/Eastern`) all expanded to full readable names
- **Format inconsistencies** — zipcodes in mixed 5-digit and 9-digit formats standardized; timestamps converted from plain text to proper date-time
- **Useless data** — one entire column had the same value across all 7.7 million rows and was dropped
- **New features added** — time of day, season, rush hour flag, accident duration, weather risk flags, and POI count columns created for richer analysis

**Output:** `Cleaned_Dataset.csv` (for EDA) and `ML_Ready_Dataset.csv` (for ML modelling)

---

### Part 2 — Exploratory Data Analysis

**Notebook:** `Exploratory_Data_Analysis.ipynb`

The cleaned data was explored across two levels of analysis:

**Univariate Analysis** — every column studied individually for distribution, outliers, and skewness across numerical columns (temperature, visibility, wind speed, precipitation, distance) and categorical columns (state, severity, weather condition, wind direction, time of day).

**Bivariate Analysis** — columns paired to find relationships: weather vs severity, state vs severity, time of day vs accident count, visibility vs severity, and more.

**Key Insights:**
- 🌍 California accounts for the most accidents — far ahead of any other state
- ⏰ Accidents spike during rush hours — 7–9 AM and 4–7 PM on weekdays
- ☀️ Most accidents happen in clear weather — because that is when most people drive
- 👁️ Low visibility (below 1 mile) is strongly linked to higher severity accidents
- 🗺️ Accidents cluster on the East and West coasts — the interior has far fewer
- 📊 Over 80% of accidents are mid-severity — truly critical accidents are rare

---

### Part 3 — Interactive Dashboard

**File:** `app.py`

A 6-page Streamlit dashboard — anyone can explore the data visually without writing a single line of code.

| Page | What It Shows |
|---|---|
| 🏠 **Home** | KPIs, year-over-year trend, severity distribution, seasonal patterns |
| 🗺️ **Geographic** | US choropleth maps for accident count and severity by state |
| 🕐 **Time** | Accident patterns by hour, day, month, and season |
| 🌦️ **Weather** | How temperature, humidity, visibility, and conditions affect accidents |
| 🛑 **Road Features** | Junctions, signals, and crossings most linked to accidents |
| ⚠️ **Risk Report** | Combined risk view across geography, time, and weather |

---

### Part 4 — ML Model

🚧 **Coming Soon**

A model that will predict how severe a given accident is likely to be — using weather conditions, road features, and time of day as inputs. Will be added as a live prediction page inside the dashboard.

---

## 🛠️ Tech Stack

| Layer | Tools |
|---|---|
| **Language** | Python 3 |
| **Dashboard** | Streamlit, streamlit-option-menu |
| **Query Engine** | DuckDB (fast SQL queries directly on Parquet files) |
| **Visualization** | Plotly Express, Matplotlib, Seaborn |
| **Data** | Pandas, NumPy |
| **Notebooks** | Jupyter Notebook |

---

## ▶️ How to Run

**1. Clone the repository**
```bash
git clone https://github.com/yourusername/us-accidents-analysis.git
cd us-accidents-analysis
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Add the data file**

Place `accidents.parquet` in the root folder (same directory as `app.py`).

**4. Launch the dashboard**
```bash
streamlit run app.py
```

---

## 💡 Key Insights

- **California, Texas, and Florida** account for the highest accident counts nationally
- **~81% of accidents** are Severity 2 — medium traffic impact
- Accidents peak during **morning (7–9 AM) and evening (4–7 PM) rush hours**
- **Clear weather** has the most accidents — driven by higher driving volume, not lower risk
- Visibility below **1 mile** is strongly linked to more severe accidents
- Accidents are concentrated on the **East and West coasts** — bimodal geographic pattern
- Most accidents affect **a single point on the road** — distance impact is minimal for the majority
- Several wind speed and pressure entries in the raw data were physically impossible — clear data errors

---

## 👤 Author

**Rajvardhan Shewale**

<a href="https://www.linkedin.com/in/rajvardhanshewale/">
  <img src="https://img.shields.io/badge/LinkedIn-Rajvardhan%20Shewale-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" />
</a>
&nbsp;
<a href="https://github.com/Precipitation-Rain">
  <img src="https://img.shields.io/badge/GitHub-Precipitation--Rain-181717?style=for-the-badge&logo=github&logoColor=white" />
</a>
&nbsp;
<a href="mailto:rajvardhanshewale1200@gmail.com">
  <img src="https://img.shields.io/badge/Gmail-rajvardhanshewale1200-D14836?style=for-the-badge&logo=gmail&logoColor=white" />
</a>

---

> **Dataset Author:** Sobhan Moosavi, Ohio State University — licensed under CC BY-NC-SA 4.0. For research and educational use only.
