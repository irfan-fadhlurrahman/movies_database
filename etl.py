import os
import re
import importlib
import pandas as pd
import numpy as np

import database_credentials as dc; importlib.reload(dc)
import cleaning; importlib.reload(cleaning)
import transform; importlib.reload(transform)
import ingest; importlib.reload(ingest)

from cleaning import cleaning
from transform import transform
from ingest import ingest

def etl_pipeline():
    # 1. Extract
    ## Read a raw data
    df = pd.read_csv(f'{dc.project_path}/movies.csv')
    
    # 2. Transform
    ## Data cleaning
    df_clean = cleaning(df)

    ## Data transformation
    multi_df_dict = transform(df_clean)
    
    # 3. Load
    ## Ingest to a relational database
    ingest(multi_df_dict)
    
    print('ETL Completed')

if __name__ == "__main__":
    etl_pipeline()
