import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import json
from typing import Tuple

class DataSourceConfig:
    """Configurazione sorgenti dati e destinazione output"""
    json_path: str = "/home/user/Scrivania/AI/Programmi/Lezione2/dati/titanic/titanic_prefs_small.json"
    csv_path: str = "/home/user/Scrivania/AI/Programmi/Lezione2/dati/titanic/passenger_info_small.csv"
    csv_path2: str = "/home/user/Scrivania/AI/Programmi/Lezione2/dati/titanic/ticket_info_small.csv"

class DataPipeline:
    def __init__(self, config: DataSourceConfig):
        self.config = config
        self.data = None

    def load_from_json(self) -> pd.DataFrame:
        """Carica dati da un file JSON"""
        return pd.read_json(self.config.json_path)
    
    def load_from_csv(self) -> pd.DataFrame:
        """Carica dati da un file CSV"""
        return pd.read_csv(self.config.csv_path)
    
    def load_from_csv2(self) -> pd.DataFrame:
        return pd.read_csv(self.config.csv_path2)
    
    def merge_data(self, df1: pd.DataFrame, df2: pd.DataFrame, json_df: pd.DataFrame) -> pd.DataFrame:
        merged_df = pd.merge(df1,df2, on='PassengerId')
        return pd.merge(merged_df, json_df, on='PassengerId')
    
    def expand_json_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parsing ed espansione della colonna JSON di preferenze"""
        df['preferences'] = df['preferences'].apply(json.loads)
        prefs_expanded = pd.json_normalize(df['preferences'])
        return pd.concat([df.drop('preferences', axis=1), prefs_expanded], axis=1)
    
    #TO-DO
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Operazioni varie di pulizia dati"""

        # ESERCIZIO 1 - Fase pre: esplorazione
        print("TIPI COLONNE:")
        print(df.dtypes)
        print("\nPrime 10 righe:")
        print(df[['Age', 'Fare', 'preferred_deck', 'dining_time', 'activity', 'Sex']].head(10))
        print("\nUltime 5 righe:")
        print(df[['Age', 'Fare', 'preferred_deck', 'dining_time', 'activity', 'Sex']].tail(5))

        # ESERCIZIO 2 - Sostituzione '?' con NaN
        df.replace("?", np.nan, inplace=True)

        # ESERCIZIO 3 - Limita gli outlier in 'Age' a soglia max = 80
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')  # Assicurati che sia numerico
        df.loc[df['Age'] > 80, 'Age'] = 80

        # ESERCIZIO 4 - Sostituzione NaN con valore medio per Age e Fare
        df['Age'].fillna(df['Age'].mean(), inplace=True)
        df['Fare'].fillna(df['Fare'].mean(), inplace=True)

        # Sostituzioni già presenti per preferred_deck e dining_time
        decks = np.array(['A', 'B', 'C', 'D', 'E', 'F'])
        df["preferred_deck"] = df["preferred_deck"].apply(
            lambda x: np.random.choice(decks) if pd.isna(x) else x
        )
        times = np.array(['early', 'flexible', 'late'])
        df["dining_time"] = df["dining_time"].apply(
            lambda x: np.random.choice(times) if pd.isna(x) else x
        )

        # ESERCIZIO 5 - Sostituisci NaN in 'activity' con il valore più frequente
        most_frequent_activity = df['activity'].mode()[0]
        df['activity'].fillna(most_frequent_activity, inplace=True)

        # ESERCIZIO 6 - Correggi errori in 'Sex'
        df['Sex'] = df['Sex'].replace({'mael': 'male', 'femael': 'female'})

        # ESERCIZIO 7 - Forza Age a float (già lo è, ma lo facciamo in modo esplicito)
        df['Age'] = df['Age'].astype(float)

        # ESERCIZIO 8 - Fase post: riesplora il df dopo la pulizia
        print("\nTIPI COLONNE DOPO PULIZIA:")
        print(df.dtypes)
        print("\nPrime 10 righe dopo pulizia:")
        print(df[['Age', 'Fare', 'preferred_deck', 'dining_time', 'activity', 'Sex']].head(10))
        print("\nUltime 5 righe dopo pulizia:")
        print(df[['Age', 'Fare', 'preferred_deck', 'dining_time', 'activity', 'Sex']].tail(5))

        return df

    
    def visualize(self, df: pd.DataFrame) -> None:
        """Crea e salva visualizzazioni"""
        # QUI INSERIAMO LE VISUALIZZAZIONI CON MATPLOTLIB E SEABORN
    
    def run_pipeline(self) -> pd.DataFrame:
        """Esegue la pipeline completa"""
        # Carica dati da più fonti
        df1 = self.load_from_csv()
        df2 = self.load_from_csv2()
        json_df = self.load_from_json()
        print("Letti dati da più fonti (CSV, CSV, JSON)")
        # Preprocessa dati (aggrega, espande e pulisce)
        merged_df = self.merge_data(df1, df2, json_df)
        print("Aggregati dati da più fonti (CSV MERGED, JSON)")        
        expanded_df = self.expand_json_data(merged_df)
        print("Effettuata espansione dati (JSON -> nuove colonne)")        
        cleaned_df = self.clean_data(expanded_df)
        print("Effettuata pulizia dati")
        #Visualizza risultati
        # self.visualize(cleaned_df)
        # print("Visualizzati risultati di analisi")
        self.visualize(cleaned_df)
        self.data = cleaned_df
        return cleaned_df #dovrà essere cleaned
    
if __name__ == "__main__":
    config = DataSourceConfig() # Usa default DataSourceConfig
    pipeline = DataPipeline(config) # Esegui la pipeline
    final_df = pipeline.run_pipeline()
    print("Pipeline (esrcz) completata con successo!")
    print(final_df)
    print("ANALIZZIAMOLO")
    print(f"Righe e colonne: {final_df.shape}")
    print("Tipi di dato:")
    print(final_df.dtypes)

