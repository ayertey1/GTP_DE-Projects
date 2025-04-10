import pandas as pd
import numpy as np
import ast

def clean_movie_data(df, verbose=False):
    """
    Clean and preprocess a raw movie dataset.

    Steps:
    - Drops irrelevant columns
    - Parses nested JSON fields (genres, companies, languages, collections)
    - Extracts cast, director, cast size, crew size from credits
    - Handles missing values, zero budgets, and invalid entries
    - Converts budget and revenue to million USD
    - Filters for released movies
    - Reorders and resets index
    - Optionally prints column value_counts for inspection
    
    Parameters:
    df (pd.DataFrame): Raw movie data.
    verbose (bool): If True, prints column summaries after cleaning.
    
    Returns:
    pd.DataFrame: Cleaned movie data.
    """

    # --- Helper function for safe JSON parsing ---
    def safe_json_parse(x):
        try:
            return ast.literal_eval(x) if pd.notnull(x) else {}
        except Exception:
            return {}

    # --- Helper function for list-based fields ---
    def parse_json_list(x, key='name', separator='|'):
        try:
            items = ast.literal_eval(x)
            if isinstance(items, list):
                return separator.join(item.get(key, '') for item in items if item.get(key))
        except Exception:
            return np.nan
        return np.nan

    # --- Drop irrelevant columns ---
    columns_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    df = df.drop(columns=columns_to_drop, errors='ignore')

    # --- Parse important nested columns ---
    df['genres'] = df['genres'].apply(lambda x: parse_json_list(x, key='name'))
    df['spoken_languages'] = df['spoken_languages'].apply(lambda x: parse_json_list(x, key='english_name'))
    df['production_countries'] = df['production_countries'].apply(lambda x: parse_json_list(x, key='name'))
    df['production_companies'] = df['production_companies'].apply(lambda x: parse_json_list(x, key='name'))

    df['belongs_to_collection'] = df['belongs_to_collection'].apply(
        lambda x: safe_json_parse(x).get('name') if pd.notnull(x) else np.nan
    )

    # --- Extract information from credits ---
    df['credits_parsed'] = df['credits'].apply(safe_json_parse)

    def get_cast(credit):
        return '|'.join([member.get('name') for member in credit.get('cast', []) if member.get('name')])

    def get_director(credit):
        for member in credit.get('crew', []):
            if member.get('job') == 'Director':
                return member.get('name')
        return None

    def get_cast_size(credit):
        return len(credit.get('cast', []))

    def get_crew_size(credit):
        return len(credit.get('crew', []))

    df['cast'] = df['credits_parsed'].apply(get_cast)
    df['director'] = df['credits_parsed'].apply(get_director)
    df['cast_size'] = df['credits_parsed'].apply(get_cast_size)
    df['crew_size'] = df['credits_parsed'].apply(get_crew_size)
    df.drop(columns=['credits_parsed'], inplace=True)

    # --- Clean up multiple-value fields by sorting alphabetically ---
    def sort_multi_values(x):
        if isinstance(x, str) and x.strip() != '':
            return '|'.join(sorted(x.split('|')))
        return np.nan  # Properly return real NaN if empty or invalid

    for col in ['genres', 'production_countries', 'spoken_languages']:
        df[col] = df[col].apply(sort_multi_values)

    # --- Optional: Print value counts ---
    if verbose:
        print("\n[INFO] Column Summaries After Cleaning:")
        for col in ['genres', 'belongs_to_collection', 'production_countries', 'production_companies', 'spoken_languages']:
            print(f"\n{col.upper()} value counts:")
            print(df[col].value_counts(dropna=False))

    # --- Handle missing values and type conversions ---
    numeric_columns = ['budget', 'id', 'popularity']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

    df[['budget', 'revenue', 'runtime']] = df[['budget', 'revenue', 'runtime']].replace(0, np.nan)

    # --- Create budget and revenue in million USD ---
    df['budget_musd'] = df['budget'] / 1e6
    df['revenue_musd'] = df['revenue'] / 1e6

    # --- Handle invalid vote averages ---
    df.loc[df['vote_count'] == 0, 'vote_average'] = np.nan

    # --- Replace invalid placeholders ---
    placeholders = ['No Data', 'No overview found.', 'No Tagline', '']
    df['overview'] = df['overview'].replace(placeholders, np.nan)
    df['tagline'] = df['tagline'].replace(placeholders, np.nan)

    # --- Remove duplicates and incomplete rows ---
    df = df.drop_duplicates(subset=['id', 'title'])
    df = df.dropna(subset=['id', 'title'])

    # Keep rows where at least 10 columns are non-NaN
    df = df[df.notna().sum(axis=1) >= 10]

    # --- Keep only Released movies ---
    if 'status' in df.columns:
        df = df[df['status'] == 'Released']
        df = df.drop(columns=['status'], errors='ignore')

    # --- Reorder columns ---
    final_cols = [
        'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
        'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
        'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
        'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size'
    ]
    df = df[[col for col in final_cols if col in df.columns]]

    df = df.reset_index(drop=True)
    return df
