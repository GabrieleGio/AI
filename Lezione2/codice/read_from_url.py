
import pandas as pd
import os

# Preparazione
code_dir = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(code_dir, '../dati/autos/auto.csv')

# # Url file remoto
# filename = 'https://archive.ics.uci.edu/ml/machine-learning-databases/autos/imports-85.data'

# headers = ["symboling","normalized-losses","make","fuel-type","aspiration", "num-of-doors","body-style",
#           "drive-wheels","engine-location","wheel-base", "length","width","height","curb-weight","engine-type",
#           "num-of-cylinders", "engine-size","fuel-system","bore","stroke","compression-ratio","horsepower",
#           "peak-rpm","city-mpg","highway-mpg","price"]

# # Leggi un file da remoto
# df = pd.read_csv(filename, names = headers)
# print("Letto da URL remoto")

# # Scrivi i dati relativi alle auto su file CSV
# df.to_csv(data_path)
# print("\nAuto DataFrame salvato come file CSV")

# Leggi un file locale
df = pd.read_csv(data_path)
print("\nLetto da file locale")

print("\n")
print(df.dtypes)
print("\n")
print(df.shape)
