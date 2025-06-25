import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from requests.auth import HTTPBasicAuth

# Configuración base
url = "https://funcionesKinetic.henco.com.mx/EpicorLive/api/v2/efx/HCO/SpaceMgt/GetOperations"
usuario = "spaceManagement"
contrasena = '"s82Rj6v,G8r5kEyKi'
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-api-Key": "wzoEIXrZV1PFXL2pAOx2MTSgenDH0YM0SDXZNiY8f8zTE"
}

# Funciones auxiliares
def get_nested_value(obj, key, subkey, default):
    items = obj.get(key, [])
    if items and isinstance(items, list):
        return items[0].get(subkey, default)
    return default

def get_safe_str(val, default="UNKNOWN"):
    if val is None or str(val).strip() == "":
        return default
    return str(val).strip()

def get_safe_num(val, default=-127):
    try:
        return float(val)
    except:
        return default

# Función para obtener y guardar datos
def obtener_datos_y_guardar(freehand_flag):
    print(f"Descargando datos con FreeHand={freehand_flag}...")
    
    payload = {
        "Company": "HCO",
        "HencoReference": "",
        "Trade": "",
        "VendorID": "",
        "FreeHand": freehand_flag,
        "ContractNum": "",
        "Itineraries": ""
    }

    response = requests.post(url, json=payload, auth=HTTPBasicAuth(usuario, contrasena), headers=headers)

    if response.status_code == 200:
        data = response.json().get("Operations", {}).get("Operations", [])
        registros = []

        for op in data:
            containers = op.get("Containers", [])
            total_profit = sum(get_safe_num(c.get("RealProfit")) for c in containers) if containers else -127

            registros.append({
                "ID": get_safe_str(op.get("ID")),
                "HencoReference": get_safe_str(op.get("HencoReference")),
                "CustomerID": get_safe_str(get_nested_value(op, "Customer", "CustID", "UNKNOWN")),
                "CustomerName": get_safe_str(get_nested_value(op, "Customer", "Name", "UNKNOWN")),
                "CarrierID": get_safe_str(get_nested_value(op, "Carrier", "VendorID", "UNKNOWN")),
                "CarrierShortName": get_safe_str(get_nested_value(op, "Carrier", "ShortName", "UNKNOWN")),
                "OriginPortID": get_safe_str(get_nested_value(op, "OriginPort", "ID", "UNKNOWN")),
                "OriginPortName": get_safe_str(get_nested_value(op, "OriginPort", "Name", "UNKNOWN")),
                "DestinationPortID": get_safe_str(get_nested_value(op, "DestinationPort", "ID", "UNKNOWN")),
                "DestinationPortName": get_safe_str(get_nested_value(op, "DestinationPort", "Name", "UNKNOWN")),
                "ETD": get_safe_str(get_nested_value(op, "Schedule", "ETD", "UNKNOWN")),
                "ETA": get_safe_str(get_nested_value(op, "Schedule", "ETA", "UNKNOWN")),
                "ContractNumber": get_safe_str(op.get("ContractNumber")),
                "ContractDescription": get_safe_str(op.get("ContractDescription")),
                "ContractType": get_safe_str(op.get("ContractType")),
                "Vessel": get_safe_str(op.get("Vessel")),
                "Voyage": get_safe_str(op.get("Voyage")),
                "TEUS": int(get_safe_num(op.get("TEUS"))),
                "Status": get_safe_str(op.get("Status")),
                "Trade": get_safe_str(op.get("Trade")),
                "FreeHand": str(freehand_flag),
                "Supervisor": get_safe_str(op.get("Supervisor")),
                "HencoExecutive": get_safe_str(op.get("HencoExecutive")),
                "TotalRealProfit": float(total_profit)
            })

        df = pd.DataFrame(registros)
        df["TEUS"] = df["TEUS"].astype("int8", errors="ignore")
        df["TotalRealProfit"] = df["TotalRealProfit"].astype("float32", errors="ignore")

        file_suffix = "TFreehand" if freehand_flag else "FFreehand"
        output_path = f"data/embarques_dataset_{file_suffix}.parquet"
        pq.write_table(pa.Table.from_pandas(df), output_path)
        print(f"Archivo generado con éxito: {output_path}")
    else:
        print(f"Error {response.status_code}: {response.text}")

# Ejecutar para ambos casos
obtener_datos_y_guardar(True)
obtener_datos_y_guardar(False)
