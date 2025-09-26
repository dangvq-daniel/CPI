# Canadian CPI Dashboard – Data Engineering Project

An end-to-end **data engineering project** showcasing automated ingestion, cleaning, transformation, and preparation of Canada's Consumer Price Index (CPI) data for interactive analysis.

This project demonstrates the ability to handle real-world messy datasets, create reproducible data pipelines, and prepare data for downstream analytics and visualization.

---

## Project Overview

This project builds a robust pipeline for **monthly CPI data** from Statistics Canada:

- Automates downloading of CPI data from [StatsCan](https://www150.statcan.gc.ca/n1/tbl/csv/18100004-eng.zip).
- Extracts, cleans, and transforms raw CSV files into ready-to-use analytical tables.
- Supports filtering and aggregation by geography (province/city) and CPI categories.
- Prepares datasets for multiple metrics: **Index Value (`VALUE`)**, **Month-over-Month (`MoM`)**, **Year-over-Year (`YoY`)**.
- Optimized for reproducibility and efficiency with caching and modular design.

The visualization components are built with Streamlit and Plotly to verify and explore the processed data.

---

## Tech Stack

- **Python 3.11+** – primary programming language.
- **Pandas** – data cleaning, transformation, and aggregation.
- **Requests / Zipfile / io** – automated data download and extraction.
- **Streamlit** – optional interactive dashboard for visualization.
- **Plotly Express** – optional interactive line charts.
- **Folium & Shapely** – optional geospatial visualization.
- **Data Pipeline Practices**: caching, modular functions, reproducible transformations.

---

## Data Pipeline Features

1. **Automated Data Ingestion**
   - Downloads the latest StatsCan CPI dataset.
   - Handles compressed ZIP files and extracts CSVs programmatically.

2. **Data Cleaning & Transformation**
   - Converts date columns to `datetime`.
   - Handles missing values via forward/backward filling.
   - Converts CPI values to numeric types.
   - Normalizes province and city fields for consistency.
   - Replaces unreliable metadata values (`SYMBOL` = 't') with `NaN`.

3. **Data Aggregation**
   - Computes metrics at multiple granularities (province/city × category × month).
   - Supports multiple metrics (`VALUE`, `MoM`, `YoY`) for downstream analysis.

4. **Flexible Output**
   - Prepares datasets for plotting, analytics, or further machine learning pipelines.
   - Allows user-defined filtering and aggregation options.

---

## StatsCan CPI Dataset – Column Explanation

| Column | Description |
|--------|-------------|
| **REF_DATE** | Reference period (monthly). Format: `YYYY-MM-DD`. |
| **GEO** | Geography (province, city, or national). |
| **Products and product groups** | CPI category of the observation. |
| **UOM** | Unit of measure (base year index). |
| **VALUE** | CPI value for that period and category. |
| **MoM** | Month-over-Month change. |
| **YoY** | Year-over-Year change. |
| **City / Province** | Normalized location fields for filtering. |
| **Other Metadata** | Includes `SYMBOL`, `STATUS`, `DGUID`, `VECTOR`, `SCALAR_FACTOR`, etc. |

---

## How to Run

1. Clone the repository:

```bash
git clone https://github.com/yourusername/cpi-dashboard.git
cd cpi-dashboard
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the dashboard (optional, to explore processed data):

```bash
streamlit run streamlitapp.py
```

4. Or use the cleaned CSVs directly for analytics or machine learning.

---

## Data Engineering Skills Highlighted

* **Automated Data Ingestion** – downloading, extracting, and reading datasets programmatically.
* **Data Cleaning & Transformation** – handling messy real-world datasets with missing values and metadata issues.
* **Data Aggregation & Preparation** – producing analytical tables ready for visualization or modeling.
* **Reproducible Pipelines** – modular, cached, and efficient data processing functions.
* **Optional Visualization Verification** – using Streamlit and Plotly to validate processed data.

---

## Extensions / Next Steps

* Schedule automated updates of CPI data for **incremental ingestion pipelines**.
* Build **ETL pipelines** to load cleaned data into databases (PostgreSQL, BigQuery, etc.).
* Extend to **forecasting inflation trends** using processed datasets.
* Add **downloadable processed datasets** for analytics or machine learning workflows.
