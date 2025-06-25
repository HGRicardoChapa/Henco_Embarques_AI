import duckdb
import pandas as pd

# Cargar los dos datasets ya procesados
df1 = pd.read_parquet("data/embarques_dataset_TFreehand.parquet")
df2 = pd.read_parquet("data/embarques_dataset_FFreehand.parquet")

# Concatenar y eliminar columnas duplicadas
df = pd.concat([df1, df2], ignore_index=True)
df = df.loc[:, ~df.columns.duplicated()]

# Convertir fechas
df["ETD"] = pd.to_datetime(df["ETD"], errors="coerce")
df["ETA"] = pd.to_datetime(df["ETA"], errors="coerce")
df["DuracionViaje"] = (df["ETA"] - df["ETD"]).dt.days

# Crear o abrir base de datos
con = duckdb.connect("data/henco.duckdb")


print("✅ Tabla 'embarques' cargada correctamente en henco.duckdb")
