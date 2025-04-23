import pandas as pd
from sqlalchemy import create_engine

dict =     {

        "Name": [

            "Braund, Mr. Owen Harris",

            "Allen, Mr. William Henry",

            "Bonnell, Miss. Elizabeth",
            
            "Taylor, Miss. Jane"

        ],

        "Age": [22, 35, 58, 55],

        "Sex": ["male", "male", "female", "female"],
        
        "Location": ["Rome", "London", "Berlin", "New York"],

    }
 
df = pd.DataFrame(dict)
print("Un dataframe creato da un dizionario Python")
print(df)
print("\n")


### SCRIVI DATI IN PostgreSQL DB ###
#Set up della connessione ad un nostro database sul nostro PostgreSQL DB
engine = create_engine('postgresql+psycopg://postgres:postgres@postgresql:5432/prova_db')

# Prepara una tabella
passenger_info = df[['Name', 'Sex', 'Age', 'Location']]

# Scrivi la tabella in PostgreSQL
passenger_info.to_sql('passenger_info', engine, if_exists='replace', index=False)

print("\nTabella scritta nel nostro database PostgreSQL")
print("\n")