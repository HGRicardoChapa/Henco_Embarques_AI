import duckdb
import pandas as pd

con = duckdb.connect()

df = con.execute("SELECT * FROM 'data/embarques_dataset_unificado.parquet'").fetchdf()

df = df.loc[:, ~df.columns.duplicated()]

df["ETD"] = pd.to_datetime(df["ETD"], errors="coerce")
df["ETA"] = pd.to_datetime(df["ETA"], errors="coerce")

df["DuracionViaje"] = (df["ETA"] - df["ETD"]).dt.days

df["TEUS"] = df["TEUS"].astype("int8", errors="ignore")
df["TotalRealProfit"] = df["TotalRealProfit"].astype("float32")

# Mostrar información relevante
print("Vista previa del dataset:")
print(df.head(10))
print("\nNúmero total de registros:", len(df))

print("\nConteo de valores 'UNKNOWN':")
unknown_counts = (df == "UNKNOWN").sum()
print(unknown_counts[unknown_counts > 0])

print("\nConteo de valores -127:")
neg127_counts = (df == -127).sum()
print(neg127_counts[neg127_counts > 0])

print("\nTipos de datos por columna:")
print(df.dtypes)

# Guardar archivo limpio
df.to_parquet("data/embarques_final.parquet", index=False)
print("\nArchivo 'embarques_final.parquet' generado correctamente.")
