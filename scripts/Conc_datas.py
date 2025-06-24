import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Leer ambos archivos .parquet
df_true = pd.read_parquet("data/embarques_dataset_TFreehand.parquet")
df_false = pd.read_parquet("data/embarques_dataset_FFreehand.parquet")

# Concatenar y eliminar columnas duplicadas
df_concat = pd.concat([df_true, df_false], ignore_index=True)
df_concat = df_concat.loc[:, ~df_concat.columns.duplicated()]

# Guardar como archivo unificado
table = pa.Table.from_pandas(df_concat)
pq.write_table(table, "data/embarques_dataset_unificado.parquet")

print("Archivo unificado generado con éxito: data/embarques_dataset_unificado.parquet")
