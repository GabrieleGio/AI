import pandas as pd
from sqlalchemy import create_engine, text

### LEGGI DATI DA PostgreSQL DB ###
SIMPLE_QUERY_1 = """
SELECT *
FROM public.passenger_info
"""
SIMPLE_QUERY_2 = """
SELECT *
FROM public.passenger_info
WHERE "Age" < 55
"""
#Set up della connessione ad un nostro database sul nostro PostgreSQL DB
engine = create_engine('postgresql+psycopg://postgres:postgres@postgresql:5432/prova_db')

print("\nQuery per la lettura dei dati sul nostro database PostgreSQL")
print(SIMPLE_QUERY_1)

# # Leggi i dati da PostgreSQL ad un DataFrame
df_from_db = pd.read_sql_query(text(SIMPLE_QUERY_1), con=engine.connect())

print("\nDati letti dal nostro database PostgreSQL")
print("\n")
print(df_from_db)