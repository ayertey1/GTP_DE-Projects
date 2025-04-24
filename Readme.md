# TMDB Movie Data Analysis Project (PySpark Version)

Welcome to the **PySpark-powered** TMDB Movie Data Analysis project!  
This version leverages **Apache Spark** to scale data transformation, KPI calculations, and exploratory analysis on movie metadata fetched from **The Movie Database (TMDb) API**.

It showcases how to build an efficient, modular ETL pipeline capable of handling **large-scale nested JSON data**, perform feature engineering, and deliver insights — all while maintaining readability, modularity, and performance.

---

## Table of Contents

- [Project Description](#project-description)
- [Project Structure](#project-structure)
- [Installation and Setup](#installation-and-setup)
- [How to Run](#how-to-run)
- [Key Analysis Outputs](#key-analysis-outputs)
- [Key Insights](#key-insights)

---

## Project Description

This PySpark version automates:

-  **Extraction:** Fetching movie metadata from TMDb API (title, cast, crew, budget, etc.)
-  **Transformation:** Handling nested JSON structures using Spark SQL functions (`transform`, `explode`, etc.)
-  **Engineering:** Deriving KPIs like Profit, ROI, Cast Size, and Director Roles
-  **Analysis:** Ranking based on revenue, ROI, ratings, and franchise impact
-  **Visualization:** Converted to Pandas for Matplotlib/Seaborn visuals
-  **Modularity:** Extract/Clean/Analyze/Visualize in separate modules for reuse and clarity

---

## Project Structure

```bash
tmdb_movie_analysis_spark/
│
├── README.md                       # Project documentation
├── .gitignore                      # Ignore sensitive/environment files and data folder
├── main.ipynb                      # Main runner script for notebook version of the pipeline
├── utilityFunc.py                  # Holds the combined versions of dataExtraction.py, dataAnalysis.py, dataRefinery.py and visualizations.py
├── requirements.txt                # Holds necessay dependencies used in this project
│
│
├── src/
│   ├── main.py                     # Main runner script
│   ├── dataExtraction.py           # Extract movie data using API with fallback/retry
│   ├── dataRefinery.py             # Clean and transform nested movie data
│   ├── dataAnalysis.py             # Perform KPI and director/franchise success ranking
│   └── visualizations.py           # Visualize final results using Pandas

```
---

## Installation and Setup

#### Clone the Repository
```
git clone https://github.com/ayertey1/GTP_DE-Projects.git
cd GTP_DE-Projects
```
#### Install Dependencies
Activate your virtual environment and install required libraries:
```
pip install -r requirements.txt
```
#### Configure Environment
Create a .env file with your TMDb credentials:
```
API_KEY=your_tmdb_api_key_here
BASE_url=https://api.themoviedb.org/3/movie/
```
---
## How to Run
```
python src/main.py
```
This will:

 Extract movie metadata using the API (with retry and rate-limit handling)

 Clean nested JSON data using Spark’s SQL functions

 Analyze and rank movies by revenue, profit, ROI, etc.

 Generate visualizations (converted to Pandas using .toPandas())

---
## Key Analysis Outputs 

* Top 5 Revenue, Budget, and Profit movies

* Most Voted & Best Rated Films

* Lowest ROI and Most Popular Titles

* Franchise vs Standalone performance

* Director and Franchise Success Rankings

* Advanced Filters: Bruce Willis in Sci-Fi | Uma Thurman + Tarantino

---
## Sample Visuals **(via .toPandas())**

* Revenue vs Budget (Log-Log)

* ROI Distribution by Genre

* Popularity vs Rating

* Yearly Budget & Revenue Trends

* Franchise vs Standalone Comparisons

---

## Key Insights

* Franchise movies tend to outperform standalone films on average.

* ROI is highly variable in genres like Science Fiction and Adventure.

* Popularity ≠ Quality — ratings and votes often tell a different story.

* Some low-budget movies deliver extremely high ROI.

* Directors like James Cameron and Christopher Nolan dominate revenue.

