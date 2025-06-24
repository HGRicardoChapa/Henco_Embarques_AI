import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from requests.auth import HTTPBasicAuth

# Configuración
url = "https://funcionesKinetic.henco.com.mx/EpicorLive/api/v2/efx/HCO/SpaceMgt/GetOperations"
usuario = "spaceManagement"
contrasena = '"s82Rj6v,G8r5kEyKi'

payload = {
    "Company": "HCO",
    "HencoReference": "",
    "Trade": "",
    "VendorID": "",
    "FreeHand": False,
    "ContractNum": "",
    "Itineraries": ""
}

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-api-Key": "wzoEIXrZV1PFXL2pAOx2MTSgenDH0YM0SDXZNiY8f8zTE"
}

# Función auxiliar para obtener valores anidados
def get_nested_value(obj, key, subkey, default):
    items = obj.get(key, [])
    if items and isinstance(items, list):
        return items[0].get(subkey, default)
    return default

# Llamada a la API
response = requests.post(url, json=payload, auth=HTTPBasicAuth(usuario, contrasena), headers=headers)

if response.status_code == 200:
    data = response.json().get("Operations", {}).get("Operations", [])
    registros = []

    for op in data:
        containers = op.get("Containers", [])
        total_profit = sum(float(c.get("RealProfit") or -127) for c in containers) if containers else -127

        registros.append({
            "ID": op.get("ID", "UNKNOWN"),
            "HencoReference": op.get("HencoReference", "UNKNOWN"),
            "CustomerID": get_nested_value(op, "Customer", "CustID", "UNKNOWN"),
            "CustomerName": get_nested_value(op, "Customer", "Name", "UNKNOWN"),
            "CarrierID": get_nested_value(op, "Carrier", "VendorID", "UNKNOWN"),
            "CarrierShortName": get_nested_value(op, "Carrier", "ShortName", "UNKNOWN"),
            "OriginPortID": get_nested_value(op, "OriginPort", "ID", "UNKNOWN"),
            "OriginPortName": get_nested_value(op, "OriginPort", "Name", "UNKNOWN"),
            "DestinationPortID": get_nested_value(op, "DestinationPort", "ID", "UNKNOWN"),
            "DestinationPortName": get_nested_value(op, "DestinationPort", "Name", "UNKNOWN"),
            "ETD": get_nested_value(op, "Schedule", "ETD", "UNKNOWN"),
            "ETA": get_nested_value(op, "Schedule", "ETA", "UNKNOWN"),
            "ContractNumber": op.get("ContractNumber", "UNKNOWN"),
            "ContractDescription": op.get("ContractDescription", "UNKNOWN"),
            "ContractType": op.get("ContractType", "UNKNOWN"),
            "Vessel": op.get("Vessel", "UNKNOWN"),
            "Voyage": op.get("Voyage", "UNKNOWN"),
            "TEUS": op.get("TEUS", -127),
            "Status": op.get("Status", "UNKNOWN"),
            "Trade": op.get("Trade", "UNKNOWN"),
            "FreeHand": op.get("FreeHand", "UNKNOWN"),
            "Supervisor": op.get("Supervisor", "UNKNOWN"),
            "HencoExecutive": op.get("HencoExecutive", "UNKNOWN"),
            "TotalRealProfit": total_profit
        })

    # Guardar en Parquet
    df = pd.DataFrame(registros)

    # Convertir tipos si es posible
    df["TEUS"] = df["TEUS"].astype("int8", errors="ignore")
    df["TotalRealProfit"] = df["TotalRealProfit"].astype("float32", errors="ignore")

    table = pa.Table.from_pandas(df)
    pq.write_table(table, "data/embarques_dataset_FFreehand.parquet")

    print("Archivo generado con éxito: data/embarques_dataset_FFreehand.parquet")

else:
    print(f"Error {response.status_code}: {response.text}")
