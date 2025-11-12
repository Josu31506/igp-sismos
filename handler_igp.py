import os
import json
import time
import uuid
from decimal import Decimal

import boto3
import requests
from boto3.dynamodb.conditions import Key

# Nombre de la tabla (configurado en serverless.yml)
DDB_TABLE = os.environ.get("DDB_TABLE", "TablaSismosIGP")

# Endpoint ArcGIS REST que usa la web del IGP internamente (retorna JSON)
ARCGIS_URL = (
    "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/"
    "SismosReportados/MapServer/0/query"
)

# Campos que queremos traer
FIELDS = ",".join([
    "objectid","fecha","hora","lat","lon","prof","ref","int_","profundidad",
    "sentido","magnitud","departamento","fechaevento","mag","code"
])

def _to_decimal(x):
    if isinstance(x, float):
        return Decimal(str(x))
    return x

def fetch_last_10():
    """Consulta los 10 últimos sismos (orderBy fechaevento desc)."""
    params = {
        "where": "1=1",
        "outFields": FIELDS,
        "orderByFields": "fechaevento desc",
        "resultRecordCount": 10,
        "f": "json",
        "returnGeometry": "false",
    }
    r = requests.get(ARCGIS_URL, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    feats = data.get("features", [])
    items = []
    for f in feats:
        a = (f or {}).get("attributes", {}) or {}

        code = str(a.get("code") or uuid.uuid4())

        item = {
            # PK
            "code": code,

            # Metadatos IGP (algunos vienen duplicados/múltiples variantes)
            "reporte": str(a.get("mag") or ""),             # p.ej. "IGP/CENSIS/RS 2025-0745"
            "fecha": int(a["fecha"]) if a.get("fecha") else None,              # epoch ms (local)
            "hora": str(a.get("hora") or ""),
            "fechaevento": int(a["fechaevento"]) if a.get("fechaevento") else None,  # epoch ms (UTC)
            "lat": _to_decimal(a.get("lat")) if a.get("lat") is not None else None,
            "lon": _to_decimal(a.get("lon")) if a.get("lon") is not None else None,
            "prof_km": a.get("prof"),
            "profundidad_cat": str(a.get("profundidad") or ""),  # Superficial / Intermedio / Profundo
            "referencia": str(a.get("ref") or ""),               # “36 km al N de …”
            "intensidad": str(a.get("int_") or ""),              # (si aplica)
            "sentido": str(a.get("sentido") or ""),              # “Percibido”, etc.
            "magnitud": _to_decimal(a.get("magnitud")) if a.get("magnitud") is not None else None,
            "departamento": str(a.get("departamento") or ""),

            # Auditoría
            "ingresado_ts": int(time.time() * 1000),
            "source": "IGP-ArcGIS"
        }
        items.append(item)
    return items

def upsert_items(items):
    """Inserta/actualiza por PK=code (idempotente)."""
    ddb = boto3.resource("dynamodb")
    table = ddb.Table(DDB_TABLE)
    # En versiones recientes, overwrite_by_pkeys funciona con batch_writer
    with table.batch_writer(overwrite_by_pkeys=["code"]) as batch:
        for it in items:
            batch.put_item(Item=it)

def lambda_ingestar(event, context):
    """GET /igp/sismos/ingestar — trae del IGP y guarda 10 en DynamoDB."""
    try:
        items = fetch_last_10()
        if items:
            upsert_items(items)
        body = {"ingresados": len(items), "items": items}
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps(body, default=str),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(e)}),
        }

def lambda_listar(event, context):
    """GET /igp/sismos/listar — devuelve los 10 más recientes desde DynamoDB.

    Si deseas ordenar por fechaevento, lo ideal sería un índice secundario global (GSI) con
    PK constante y SK = fechaevento; para simplicidad, aquí hacemos un Scan limitado y
    luego ordenamos en memoria.
    """
    try:
        ddb = boto3.resource("dynamodb")
        table = ddb.Table(DDB_TABLE)

        # Scan limitado (en tablas grandes crea GSI; aquí basta para taller/demo)
        resp = table.scan(Limit=50)  # traemos un poco más y luego quedamos en 10
        items = resp.get("Items", [])

        # Ordenar por fechaevento desc si existe, si no por ingresado_ts
        def sort_key(it):
            return it.get("fechaevento") or it.get("ingresado_ts") or 0

        items_sorted = sorted(items, key=sort_key, reverse=True)[:10]

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"count": len(items_sorted), "items": items_sorted}, default=str),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(e)}),
        }
