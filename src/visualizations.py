import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd 

def plot_revenue_vs_budget(df):
    """Plot Revenue vs Budget trends."""
    plt.figure(figsize=(10,6))
    sns.scatterplot(data=df, x='budget_musd', y='revenue_musd')
    plt.title('Revenue vs Budget')
    plt.xlabel('Budget (Million USD)')
    plt.ylabel('Revenue (Million USD)')
    plt.xscale('log')
    plt.yscale('log')
    plt.grid(True, which="both", ls="--", linewidth=0.5)
    plt.show()

def plot_roi_distribution_by_genre(df):
    """Plot ROI Distribution by Genre."""
    df_exploded = df.dropna(subset=['genres']).copy()

    # FORCE genres to string before splitting
    df_exploded['genres'] = df_exploded['genres'].astype(str).str.split('|')
    df_exploded = df_exploded.explode('genres')

    # Drop fake 'nan' genres after exploding
    df_exploded = df_exploded[df_exploded['genres'].notna()]
    df_exploded = df_exploded[df_exploded['genres'] != 'nan']

    plt.figure(figsize=(14,8))
    sns.boxplot(data=df_exploded, x='genres', y='roi')
    plt.title('ROI Distribution by Genre')
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Return on Investment (ROI)')
    plt.grid(True, ls="--", linewidth=0.5)
    plt.show()



def plot_popularity_vs_rating(df):
    """Plot Popularity vs Rating."""
    plt.figure(figsize=(10,6))
    sns.scatterplot(data=df, x='vote_average', y='popularity')
    plt.title('Popularity vs Rating')
    plt.xlabel('Average Rating')
    plt.ylabel('Popularity')
    plt.grid(True, ls="--", linewidth=0.5)
    plt.show()

def plot_yearly_box_office_trends(df):
    """Plot Yearly Trends in Box Office Performance."""
    df['release_year'] = df['release_date'].dt.year
    yearly = df.groupby('release_year').agg({
        'revenue_musd': 'mean',
        'budget_musd': 'mean',
        'popularity': 'mean'
    }).dropna()
    
    plt.figure(figsize=(12,6))
    sns.lineplot(data=yearly, x=yearly.index, y='revenue_musd', label='Revenue')
    sns.lineplot(data=yearly, x=yearly.index, y='budget_musd', label='Budget')
    plt.title('Yearly Trends: Revenue and Budget')
    plt.xlabel('Year')
    plt.ylabel('Million USD')
    plt.legend()
    plt.grid(True, ls="--", linewidth=0.5)
    plt.show()

def plot_franchise_vs_standalone(df):
    """Plot Franchise vs Standalone Success."""
    df['is_franchise'] = df['belongs_to_collection'].notna()
    stats = df.groupby('is_franchise').agg({
        'revenue_musd': 'mean',
        'budget_musd': 'mean'
    }).rename(index={True: 'Franchise', False: 'Standalone'})
    
    stats.plot(kind='bar', figsize=(10,6))
    plt.title('Franchise vs Standalone: Revenue and Budget Comparison')
    plt.ylabel('Million USD')
    plt.xticks(rotation=0)
    plt.grid(True, ls="--", linewidth=0.5)
    plt.show()

def visualize_movies(df):
    """Run all visualizations in sequence."""
    sns.set(style="whitegrid")
    
    plot_revenue_vs_budget(df)
    plot_roi_distribution_by_genre(df)
    plot_popularity_vs_rating(df)
    plot_yearly_box_office_trends(df)
    plot_franchise_vs_standalone(df)
