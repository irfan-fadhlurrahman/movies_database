import os
import re
import pandas as pd
import numpy as np

def cleaning(df):
    # Copy to avoid overwrite original dataframe
    df = df.copy()
    
    # Remove duplicated data if any
    df = df.drop_duplicates()
    
    # Remove new line \n and unneccassary whitespace
    for col in df.columns.tolist():
        df[col] = df[col].apply(remove_symbols_for_movies)
    
    # Get release year from YEAR column
    df['release_year'] = df['YEAR'].apply(get_release_year)
    
    # Get list of Director and Cast name from STARS column
    df['director_name'] = df['STARS'].apply(get_director_name)
    df['cast_name'] = df['STARS'].apply(get_cast_name)
    
    # Get only numbers from Gross column
    df['Gross'] = df['Gross'].apply(get_gross_as_int)
    
    # Change GENRE dtype to list
    df['GENRE'] = df['GENRE'].apply(convert_genre_dtype)
    
    # Remove commas from VOTES
    df['votes_count'] = df['VOTES'].apply(remove_commas_in_votes)
    
    # Replace 'Add a Plot' in ONE-LINE column and create overview column
    df['overview'] = df['ONE-LINE'].apply(get_movie_overview)
    
    # Take only neccessary columns as per database design
    df.columns = df.columns.str.lower()
    columns_to_use = [
        'movies', 'release_year', 'gross', 
        'runtime', 'rating', 'votes_count', 'overview',
        'genre', 'director_name', 'cast_name'
    ]
    df = df[columns_to_use].copy()
    
    # Replace column names as per database design
    df.columns = [
        'title', 'release_year', 'gross', 
        'runtime', 'rating', 'votes_count', 'overview',
        'genre_name', 'director_name', 'star_name'
    ]
    
    # Save cleaned dataframe as json for backup to avoid preserve list type in some columns
    df.to_json(f'{os.path.expanduser("~")}/movies_etl/data/cleaned_movies.json')
    
    return df.reset_index(drop=True)


# User-defined function for cleaning
def remove_symbols_for_movies(row):
    try:
        row = re.sub(r'\n|\|', '', row.strip())
        return re.sub('\s{2,}', ' ', row)
    except AttributeError:
        return row
    
def convert_genre_dtype(row):
    if isinstance(row, str):
        return re.sub(r'\,\s', ',', row.strip()).split(',')
    else:
        return np.nan
    
def remove_commas_in_votes(row):
    if isinstance(row, str):
        return row.replace(',', '')
    else:
        return np.nan

def get_release_year(row):
    if isinstance(row, str):
        if re.search(r'\([I]+\)', row):
            row = re.sub(r'\([I]+\)', '', row)

        if re.search(r'\d{4}', row):
            return re.search(r'\d{4}', row).group()
    else:
        return row

def get_director_name(row):
    if isinstance(row, str):
        director_name = re.search(r'Directors?\:(.*?(?=Stars?|$))', row)
        if director_name:
            name = director_name.group(1).strip()
            return re.sub(r'\,\s', ',', name).split(',') 
        else:
            return np.nan
    else:
        return np.nan

def get_cast_name(row):
    if isinstance(row, str):
        cast_name = re.search(r'Stars?\:(.*)', row)
        if cast_name:
            name = cast_name.group(1).strip()
            return re.sub(r'\,\s', ',', name).split(',')  
        else:
            return np.nan
    else:
        return np.nan

def get_gross_as_int(row):
    if isinstance(row, str):
        total_gross = re.search(r'\$([\d\.]+)M', row)
        if total_gross:
            return float(total_gross.group(1).strip())
        else:
            return np.nan
    else:
        return np.nan

def get_movie_overview(row):
    if isinstance(row, str):
        remove_add_a_plot = re.search(r'Add a Plot', row)
        if remove_add_a_plot:
            return np.nan
        else:
            return row
    else:
        return np.nan