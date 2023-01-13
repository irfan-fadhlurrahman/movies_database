import database_credentials as dc
import pandas as pd

def ingest(multi_df_dict, engine=dc.ENGINE):
    for table_name, _ in dc.DTYPES_DICT.items():
        ingest_single_table(engine, multi_df_dict[table_name], table_name)
        
def ingest_single_table(engine, df, table_name):
    df = df.copy()

    # Ingest to the database
    df.to_sql(
        name=table_name, 
        con=engine, 
        if_exists='append', 
        index=False,
        dtype=dc.DTYPES_DICT[table_name]
    )
    print(f'Table: {table_name} | Status: Ingested')