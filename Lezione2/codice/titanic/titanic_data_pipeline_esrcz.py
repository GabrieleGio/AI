import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import json
from typing import Tuple

class DataSourceConfig:
    """Configurazione sorgenti dati e destinazione output"""
    db_uri: str = "postgresql+psycopg://postgres:postgres@postgresql:5432/titanic_db"
    json_path: str = "../../dati/titanic/titanic_prefs_small.json"

class DataPipeline:
    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.data = None
    
    def load_from_database(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Carica dati da un database PostgreSQL"""
        queries = {
            'passenger': "SELECT * FROM public.passenger_info_small",
            'ticket': "SELECT * FROM public.ticket_info_small"
        }
        
        engine = create_engine(self.config.db_uri)
        try:
            with engine.connect() as conn:
                df1 = pd.read_sql_query(text(queries['passenger']), conn)
                df2 = pd.read_sql_query(text(queries['ticket']), conn)
        except SQLAlchemyError as e:
            print(f"Errore di lettura da database: {e}")
            df1 = pd.DataFrame()
            df1 = pd.DataFrame() 
        finally:
            engine.dispose()
        return df1, df2 
    
    def load_from_json(self) -> pd.DataFrame:
        """Carica dati da un file JSON"""
        return pd.read_json(self.config.json_path)
        
    def merge_data(self, db_df1: pd.DataFrame, db_df2: pd.DataFrame, json_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregazione dati di diverse sorgenti"""
        db_merged = pd.merge(db_df1, db_df2, on='PassengerId')
        return pd.merge(db_merged, json_df, on='PassengerId')
    
    def expand_json_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parsing ed espansione della colonna JSON di preferenze"""
        df['preferences'] = df['preferences'].apply(json.loads)
        prefs_expanded = pd.json_normalize(df['preferences'])
        return pd.concat([df.drop('preferences', axis=1), prefs_expanded], axis=1)
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Operazioni varie di pulizia dati"""
        
        # Studia il dataframe - Fase pre
        # ESERCIZIO 1 
        # Task: stampa i tipi del df e le colonne Age' ,'Fare', 'preferred_deck', 'dining_time', 'activity', 'Sex' 
        #       delle prime 10 ed ultime 5 righe        

        
        # Sostituisce valori nulli/assenti con NaN
        # ESERCIZIO 2 
        # Task: sostituire i '?' con numpy NaNs        

        
        # Limita gli outliers
        # ESERCIZIO 3 
        # Task: stabilisci una soglia max di età ('Age') a cui riportare gli outliers (soglia_max=80)


        # Sostituisce NaNs con il valor medio
        # ESERCIZIO 4
        # Task: applicare la sostituzione con il valor medio alle colonne 'Age' e 'Fare'        
    
        
        # Sositutisce NaNs con valori random di colonne categoriche
        decks = np.array(['A', 'B', 'C', 'D', 'E', 'F'])
        df["preferred_deck"] = df["preferred_deck"].apply(
            lambda x: np.random.choice(decks) if pd.isna(x) else x
        )
        times = np.array(['early', 'flexible', 'late'])
        df["dining_time"] = df["dining_time"].apply(
            lambda x: np.random.choice(times) if pd.isna(x) else x
        )
        
        # Sositutisce NaNs con il valore più frequente
        # ESERCIZIO 5 
        # Task: applicare alla colonna 'activity' la sostituzione con il valore più frequente        
        
        
        # Correzione di errori per contenuti standard
        # ESERCIZIO 6 
        # Task: correggere 'mael' con 'male' e 'femael' con 'female' nella colonna 'Sex'        
        
        
        # Conversione dei tipi di dato
        df = df.convert_dtypes()
        # ESERCIZIO 7
        # Task: rendere l'età di tipo float 
        
        # Studia il dataframe - Fase post        
        # ESERCIZIO 8 
        # Task: stampa i tipi del df e le colonne Age' ,'Fare', 'preferred_deck', 'dining_time', 'activity', 'Sex' 
        #       delle prime 10 ed ultime 5 righe, Cosa è cambiato rispetto all'output dell'ESERCIZIO 1?         

        return df
    
    def visualize(self, df: pd.DataFrame) -> None:
        """Crea e salva visualizzazioni"""
        # QUI INSERIAMO LE VISUALIZZAZIONI CON MATPLOTLIB E SEABORN
    
    def run_pipeline(self) -> pd.DataFrame:
        """Esegue la pipeline completa"""
        # Carica dati da più fonti
        db_df1, db_df2 = self.load_from_database()
        json_df = self.load_from_json()
        print("Letti dati da più fonti (db, JSON)")
        # Preprocessa dati (aggrega, espande e pulisce)
        merged_df = self.merge_data(db_df1, db_df2, json_df)
        print("Aggregati dati da più fonti (db, JSON)")        
        expanded_df = self.expand_json_data(merged_df)
        print("Effettuata espansione dati (JSON -> nuove colonne)")        
        cleaned_df = self.clean_data(expanded_df)
        print("Effettuata pulizia dati")
        # Visualizza risultati
        # self.visualize(cleaned_df)
        # print("Visualizzati risultati di analisi")
        self.data = cleaned_df
        return cleaned_df
        
if __name__ == "__main__":
    config = DataSourceConfig() # Usa default DataSourceConfig
    pipeline = DataPipeline(config) # Esegui la pipeline
    final_df = pipeline.run_pipeline()
    print("Pipeline (esrcz) completata con successo!")
    # print(final_df.head())