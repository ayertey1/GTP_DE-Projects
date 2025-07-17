import pandas as pd

# --- Helper Function: Rank Movies ---
def rank_movies(df, sort_by, ascending=False, min_budget_musd=None, min_votes=None, top_n=5):
    """
    Rank movies by a specified metric with optional filters.
    """
    temp = df.copy()
    if min_budget_musd:
        temp = temp[temp['budget_musd'] >= min_budget_musd]
    if min_votes:
        temp = temp[temp['vote_count'] >= min_votes]
    temp = temp.sort_values(by=sort_by, ascending=ascending)
    return temp[['title', sort_by]].head(top_n)

# --- 1. Add Helper KPI Columns (Profit & ROI) --
def add_helper_columns(df):
    """
    Add profit and ROI columns to the DataFrame.
    """
    df['profit'] = df['revenue_musd'] - df['budget_musd']
    df['roi'] = df['revenue_musd'] / df['budget_musd']
    return df

# --- 2. KPI Ranking Functions ---
def top_revenue_movies(df, top_n=5):
    return rank_movies(df, sort_by='revenue_musd', ascending=False, top_n=top_n)

def top_budget_movies(df, top_n=5):
    return rank_movies(df, sort_by='budget_musd', ascending=False, top_n=top_n)

def top_profit_movies(df, top_n=5):
    return rank_movies(df, sort_by='profit', ascending=False, top_n=top_n)

def lowest_profit_movies(df, top_n=5):
    return rank_movies(df, sort_by='profit', ascending=True, top_n=top_n)

def highest_roi_movies(df, top_n=5):
    return rank_movies(df, sort_by='roi', ascending=False, min_budget_musd=10, top_n=top_n)

def lowest_roi_movies(df, top_n=5):
    return rank_movies(df, sort_by='roi', ascending=True, min_budget_musd=10, top_n=top_n)

def most_voted_movies(df, top_n=5):
    return rank_movies(df, sort_by='vote_count', ascending=False, top_n=top_n)

def highest_rated_movies(df, top_n=5):
    return rank_movies(df, sort_by='vote_average', ascending=False, min_votes=10, top_n=top_n)

def lowest_rated_movies(df, top_n=5):
    return rank_movies(df, sort_by='vote_average', ascending=True, min_votes=10, top_n=top_n)

def most_popular_movies(df, top_n=5):
    return rank_movies(df, sort_by='popularity', ascending=False, top_n=top_n)

# --- 3. Advanced Search Queries ---
def search_best_scifi_action_bruce(df):
    """
    Find the best-rated Science Fiction Action movies starring Bruce Willis.
    """
    query = df[
        df['genres'].astype(str).str.contains('Science Fiction', na=False) &
        df['genres'].astype(str).str.contains('Action', na=False) &
        df['cast'].astype(str).str.contains('Bruce Willis', na=False)
    ]
    return query.sort_values(by='vote_average', ascending=False)[['title', 'vote_average']]


def search_uma_thurman_tarentino(df):
    """
    Find movies starring Uma Thurman directed by Quentin Tarantino.
    """
    query = df[
        df['cast'].astype(str).str.contains('Uma Thurman', na=False) &
        df['director'].astype(str).str.contains('Quentin Tarantino', na=False)
    ]
    return query.sort_values(by='runtime', ascending=True)[['title', 'runtime']]

# --- 4. Franchise vs Standalone Analysis ---
def franchise_vs_standalone_performance(df):
    """
    Compare franchise vs standalone movies by revenue, ROI, budget, popularity, and rating.
    """
    temp = df.copy()
    temp['is_franchise'] = temp['belongs_to_collection'].notna()
    
    stats = temp.groupby('is_franchise').agg({
        'revenue_musd': 'mean',
        'roi': 'median',
        'budget_musd': 'mean',
        'popularity': 'mean',
        'vote_average': 'mean'
    }).rename(index={True: 'Franchise', False: 'Standalone'})
    
    return stats

# --- 5. Most Successful Franchises ---
def most_successful_franchises(df, top_n=5):
    """
    Find the most successful franchises by revenue and rating.
    """
    franchise_success = df.dropna(subset=['belongs_to_collection']).groupby('belongs_to_collection').agg({
        'id': 'count',
        'budget_musd': ['sum', 'mean'],
        'revenue_musd': ['sum', 'mean'],
        'vote_average': 'mean'
    }).sort_values(('revenue_musd', 'sum'), ascending=False)
    
    return franchise_success.head(top_n)

# --- 6. Most Successful Directors ---
def most_successful_directors(df, top_n=5):
    """
    Find the most successful directors by revenue and rating.
    """
    director_success = df.dropna(subset=['director']).groupby('director').agg({
        'id': 'count',
        'revenue_musd': 'sum',
        'vote_average': 'mean'
    }).sort_values('revenue_musd', ascending=False)
    
    return director_success.head(top_n)
