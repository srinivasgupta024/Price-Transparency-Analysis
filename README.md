# 🏥 Price Transparency Analysis

This repository showcases the end-to-end data engineering and analytics pipeline built to analyze hospital price transparency data.

### 📁 Folder Structure

* **`master_excel/`,`raw/`, `bronze/`, `silver/`, `gold/`**
  Contains PySpark notebooks used for data transformations across each Medallion Architecture layer.
  
  * `Master_Excel`: Excel file that contains the hospital data links that ADF consumes 
  * `raw`: initial ingestion logic
  * `bronze`: cleaned and normalized data
  * `silver`: enriched and structured data
  * `gold`: Datamodels based on Schema

* **`visualizations/`**
  Final dashboards and visual insights built using Power BI and Tableau. These provide actionable insights from the transformed data.

* **`trail/`**
  A sandbox for experimentation and testing different transformation approaches—used primarily for trial-and-error development.

