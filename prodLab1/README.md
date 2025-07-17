# 🎬 TMDB Movie Data Analysis Project

Welcome to the TMDB Movie Data Analysis project!  
This repository contains a full, professional-grade ETL pipeline that fetches, cleans, analyzes, and visualizes movie data from **The Movie Database (TMDb) API**.

Showcasing how to extract real-world API data, clean nested JSON data structures, engineer KPIs, and build beautiful visualizations — all following best practices like modularization, environment variables, and reproducibility.

---

## 📖 Table of Contents

- [Project Description](#project-description)
- [Project Structure](#project-structure)
- [Getting Started](#installation-and-setup)
- [How to Run](#how-to-run)
- [Visualization Outputs](#visualization-outputs)
- [Key Analysis KPIs](#key-analysis-kpis)
- [Key Insights](#key-insights)



---

## 📜 Project Description

This project automates the process of:

- Fetching movie metadata from TMDb API (movie titles, budgets, revenues, cast, crew, ratings, etc.)
- Cleaning and transforming the nested JSON fields (genres, production companies, spoken languages)
- Calculating important KPIs like Profit, ROI (Return on Investment), and Popularity
- Performing detailed analysis on franchises vs standalone movies
- Visualizing insights using Matplotlib and Pandas
- Following a clear, modular, production-grade codebase

---

## 🏗️ Project Structure

```bash
tmdb_movie_analysis/
│
├── README.md                # Project documentation Lab1
├── .gitignore               # ignore files(.env)
│
├── data/                    # Data storage
│   └── raw/                 # Raw unprocessed API data
│              
│
├── src/                     # Source code modules
│   ├── main.py              # Main entry point
│   ├── data_extraction.py   # Fetch data from TMDb API
│   ├── data_cleaning.py     # Clean and preprocess data
│   ├── data_analysis.py     # KPI calculation and analysis
|   ├── requirements.txt     # Python dependencies
│   └── visualization.py     # Data visualization functions
│               
│
├── notebooks/                  
|    └── dataWrangling.ipynb    # holds the entire project in ipynb for the sake of transformation and visuals
|
│               
│
├── BusinessRequirementDocs/                  
    └── Python Fundamentals      # Holds the ASK on the project
```
---
## 🚀 Getting Started

### 1. Clone the Repository
```bash
git clone https://github.com/ayertey1/GTP_DE-Projects.git
cd tmdb-movie-analysis
```
### 2. Install Dependencies
Install all required packages using:
```bash
pip install -r requirements.txt
```
### 3. Set up Environment Variables
Create a .env file in the root folder:
```bash
API_KEY=your_tmdb_api_key_here
BASE_url=https://api.themoviedb.org/3/movie/
```
This keeps your API key secure and hidden.

---

## 🏗️ How to Run the Project
Simply run:
```bash
python main.py
```
This will:

* ***Fetch the latest movie data from TMDb***

* ***Clean and process the dataset***

* ***Perform KPI analysis***

* ***Generate several visualizations:***

   - Revenue vs Budget Trends

   - ROI Distribution by Genre

   - Popularity vs Rating

   - Yearly Box Office Trends

   - Franchise vs Standalone Movie Success Comparison

---

## 📊 Visualizations

* ***Scatter Plot:*** Revenue vs Budget (log-log scale)

* ***Box Plot:*** ROI distribution across genres

* ***Scatter Plot:*** Popularity vs Rating

* ***Line Plot:*** Yearly budget and revenue trends

* ***Bar Plot:*** Franchise vs Standalone comparison


---

## 🔥 KPIs and Analysis

* Top Highest Revenue Movies

* Top Highest Budget Movies

* Top Most Profitable Movies

* Highest and Lowest ROI Movies

* ***Advanced Filtering:***

   * Best-rated Science Fiction movies starring Bruce Willis

   * Movies starring Uma Thurman directed by Quentin Tarantino

* Franchise Performance vs Standalone Movies

* Most Successful Franchises and Directors

---

## 📌 Key Insights
* ***Franchise movies*** generally generate higher revenue compared to ***standalone*** films.

* ***Science Fiction*** and ****Adventure*** genres show high variability in ROI.

* Big budgets don’t always guarantee high ***profits*** — ROI depends on effective spending.

* Highly popular movies are not always highly ***rated*** and vice versa.