# For absolute path
import os

project_path = f'{os.path.expanduser("~")}/movies_etl'
directory_path = f'{project_path}/data'

# Connect to db and for mapping data type of each attributes in tables
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import INTEGER, REAL, VARCHAR, TEXT

DB_NAME = "movies_db"
ENGINE = create_engine(
    f"postgresql+psycopg2://postgres:abem1596@localhost:5432/{DB_NAME}"
)
DTYPES_DICT = {
    "movies": {
        "movie_id": INTEGER,
        "title": VARCHAR,
        "release_year": INTEGER,
        "gross": REAL,
    },
    "genre": {
        "genre_id": INTEGER,
        "genre_name": VARCHAR
    },
    "person": {
        "person_id": INTEGER,
        "person_name": VARCHAR
    },
    "movies_information": {
        "info_id": INTEGER,
        "movie_id": INTEGER,
        "runtime": INTEGER,
        "rating": REAL,
        "votes_count": INTEGER,
        "overview": TEXT
    },
    "movies_genre": {
        "movie_id": INTEGER,
        "genre_id": INTEGER
    },
    "movies_director": {
        "director_id": INTEGER,
        "movie_id": INTEGER,
        "person_id": INTEGER
    },
    "movies_star": {
        "star_id": INTEGER,
        "movie_id": INTEGER,
        "person_id": INTEGER
    },
    
}