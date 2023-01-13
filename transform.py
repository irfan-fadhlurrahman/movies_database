"""
Create entities as per conceptual data model in the ER Diagram (ER_Diagram.png). Then, all created tables are saved as csv file to data folder.

The following below are the entity with its attributes:
1. movies: movie_id, title, release_year, gross
2. genre: genre_id, genre_name
3. person: person_id, person_name
4. movies_information: info_id, movie_id, runtime, rating, votes_count, overview
5. movies_genre: movie_id, genre_id
6. movies_director: director_id, movie_id, person_id
7. movies_star: star_id, movie_id, person_id
"""

import os
import re
import pandas as pd
import numpy as np

DATA_LOCATION = f'{os.path.expanduser("~")}/movies_etl'

def transform(df):
    df = df.copy()
    
    # Seperate all cleaned raw data into multiple csv files
    # Base table
    df_movies = create_table_for_movies(df)
    df_genre = create_table_for_genre(df)
    df_person = create_table_for_person(df)
    
    # Table for join
    df_movies_information = create_table_for_movies_information(df, df_movies)
    df_movies_genre = create_table_for_movies_genre(df, df_movies, df_genre)
    df_movies_director = create_table_for_movies_role(df, df_movies, df_person, role='director')
    df_movies_star = create_table_for_movies_role(df, df_movies, df_person, role='star')
    
    # Save as csv files for ingestion later
    dataframe_tuple = [
        ('movies', df_movies), 
        ('genre', df_genre), 
        ('person', df_person), 
        ('movies_information', df_movies_information), 
        ('movies_genre', df_movies_genre),
        ('movies_director', df_movies_director), 
        ('movies_star', df_movies_star)
    ]
    for filename, df_table in dataframe_tuple:
        df_table.to_csv(f'{DATA_LOCATION}/data/{filename}.csv', sep=',', index=False)
    
    return {
        "movies": df_movies,
        "genre": df_genre,
        "person": df_person,
        "movies_information": df_movies_information,
        "movies_genre": df_movies_genre,
        "movies_director": df_movies_director,
        "movies_star": df_movies_star
    }

def remove_decimal(row):
    if isinstance(row, str):
        return re.sub(r'\.0(?!\d)', '', row)
    else:
        return row

def create_table_for_movies_role(df, df_movies, df_person, role='director'):
    # Copy to avoid overwrite original dataframe
    selected_columns = ['title', f'{role}_name']
    df_table = df[selected_columns].copy()
    df_movies = df_movies.copy()
    df_person = df_person.copy()
    
    # Expand the rows
    df_table = df_table.explode(f'{role}_name')
    
    # Drop duplicated data on df_table if any
    df_table = df_table.drop_duplicates().reset_index(drop=True)
    
    # Merge both df_table and df_genre to get unique id from movies table
    df_merge = pd.merge(
        df_table,
        df_movies[['movie_id', 'title']],
        on='title',
        how='left'
    )
    # Merge both df_table and df_person
    df_merge.columns = ['title', 'person_name', 'movie_id']
    df_merge = pd.merge(
        df_merge,
        df_person,
        on='person_name',
        how='left'
    )
    
    # Add info_id start from 1
    df_merge = df_merge.reset_index()
    df_merge.columns = [f'{role}_id', 'title', f'{role}_name', 'movie_id', 'person_id']
    df_merge[f'{role}_id'] = df_merge[f'{role}_id'] + 1
    
    # Re-organize the columns
    columns_reorder = [f'{role}_id', 'movie_id', 'person_id']
    
    # Fill missing values with '-'
    df_merge = df_merge[columns_reorder].copy()
    
    # Remove decimal (.0) if any
    for col in df_merge.columns.tolist():
        df_merge[col] = df_merge[col].apply(remove_decimal)
    
    return df_merge
    

def create_table_for_movies_genre(df, df_movies, df_genre):
    # Copy to avoid overwrite original dataframe
    selected_columns = ['title', 'genre_name']
    df_table = df[selected_columns].copy()
    df_movies = df_movies.copy()
    df_genre = df_genre.copy()
    
    # expand the row if there are multiple genre
    df_table = df_table.explode('genre_name')
    
    # Drop duplicated data on df_table if any
    #df_table = df_table.drop_duplicates().reset_index(drop=True)
    
    # Merge both df_table and df_genre to get unique id from movies table
    df_merge = pd.merge(
        df_table,
        df_movies[['movie_id', 'title']],
        on='title',
        how='left'
    )
    # Merge the above df_merge with df_genre
    df_merge = pd.merge(
        df_merge,
        df_genre,
        on='genre_name',
        how='left'
    )
    # Re-organize the columns
    columns_reorder = ['movie_id', 'genre_id']
        
    # Fill missing values with '-'    
    df_merge = df_merge[columns_reorder].reset_index(drop=True)
    
    # Remove decimal (.0) if any
    for col in df_merge.columns.tolist():
        df_merge[col] = df_merge[col].apply(remove_decimal)
    
    return df_merge
    
def create_table_for_movies_information(df, df_movies):
    """
    Missing value tracker:
    info_id           0
    movie_id          0
    title             0
    runtime        2435
    rating         1234
    votes_count    5149
    overview        652
    """
    # Copy to avoid overwrite original dataframe
    selected_columns = ['title', 'runtime', 'rating', 'votes_count', 'overview']
    df_table = df[selected_columns].copy()
    df_movies = df_movies.copy()
    
    # Drop duplicated data on df_table if any
    df_table = df_table.drop_duplicates().reset_index(drop=True)
    
    # Merge both df_table and df_movies to get unique id from movies table
    df_merge = pd.merge(
        df_table,
        df_movies[['movie_id', 'title']],
        on='title', 
        how='left'
    )
    # Add info_id start from 1
    df_merge = df_merge.reset_index()
    df_merge.columns = ['info_id', 'title', 'runtime', 'rating', 'votes_count', 'overview', 'movie_id']
    df_merge['info_id'] = df_merge['info_id'] + 1
    
    # Re-organize the columns
    columns_reorder = ['info_id', 'movie_id', 'runtime', 'rating', 'votes_count', 'overview']
    
    # Fill missing values with '-'
    df_merge = df_merge[columns_reorder]
    
    # Remove decimal (.0) if any
    for col in df_merge.columns.tolist():
        df_merge[col] = df_merge[col].apply(remove_decimal)
    
    return df_merge
    
def create_table_for_person(df):
    # Copy to avoid overwrite original dataframe
    selected_columns = ['title', 'director_name', 'star_name']
    df_table = df[selected_columns].copy()
    
    # Concatenate both director and star name into person_name
    person_name_list = []
    for idx, data in df_table.iterrows():
        if isinstance(data['director_name'], list):
            if isinstance(data['star_name'], list):
                person = data['director_name'] + data['star_name']
            else:
                person = data['director_name']
        else:
            if isinstance(data['star_name'], list):
                person = data['star_name']
            else:
                person = [np.nan]
        
        person_name_list.extend(person)
    
    # Create new table and expand the row if person name are more than one
    person_name_list = [
        (idx, person)
        for idx, person in enumerate(set(person_name_list), start=1)
    ]
    df_table = pd.DataFrame(person_name_list, columns=['person_id', 'person_name'])
    
    return df_table

def create_table_for_genre(df):
    # Copy to avoid overwrite original dataframe
    selected_columns = ['title', 'genre_name']
    df_table = df[selected_columns].copy()
    
    # Create multiple rows if genre are more than one
    df_table = df_table.explode('genre_name')
    
    # Get unique values from genre_name
    genre_name_list = df_table['genre_name'].unique().tolist()
    
    # add index
    genre_name_list = [
        (idx, genre)
        for idx, genre in enumerate(genre_name_list, start=1)
    ]
    
    # Create a dataframe with index
    df_table = pd.DataFrame(genre_name_list, columns=['genre_id', 'genre_name'])
    
    return df_table

def create_table_for_movies(df):
    # Copy to avoid overwrite original dataframe
    selected_columns = ['title', 'release_year', 'gross']
    df_table = df[selected_columns].copy()
    
    # Drop duplicated data if any
    df_table = df_table.drop_duplicates().reset_index()
    
    # Replace column names
    df_table.columns = ['movie_id', 'title', 'release_year', 'gross']
    
    # Add one to movie_id so it started from 1
    df_table['movie_id'] = df_table['movie_id'] + 1
    
    return df_table