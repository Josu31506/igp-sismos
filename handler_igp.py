import os, json, time, uuid
from decimal import Decimal
import boto3, requests

DDB_TABLE = os.environ.get("DDB_TABLE", "TablaSismosIGP")
ARCGIS_URL = ("https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/"
              "SismosReportados/MapServer/0/query")
FIELDS = ",".join([
    "objectid","fecha","hora","lat","lon","prof","ref","int_","profundidad",
    "sentido","magnitud","departamento","fechaevento","mag","code"
])

def _dec(x):
    return Decimal(str(x)) if isinstance(x, float) else x

def fetch_last_10():
    params = {
        "where":"1=1","outFields":FIELDS,
        "orderByFields":"fechaevento desc","resultRecordCount":10,
        "returnGeometry":"false","f":"json"
    }
    r = requests.get(ARCGIS_URL, params=params, timeout=15); r.raise_for_status()
    feats = r.json().get("features", [])
    items = []
    for f in feats:
        a = (f or {}).get("attributes", {}) or {}
        code = str(a.get("code") or uuid.uuid4())
        items.append({
            "code": code,
            "reporte": str(a.get("mag") or ""),
            "fecha": a.get("fecha"),
            "hora": str(a.get("hora") or ""),
            "fechaevento": a.get("fechaevento"),
            "lat": _dec(a.get("lat")) if a.get("lat") is not None else None,
            "lon": _dec(a.get("lon")) if a.get("lon") is not None else None,
            "prof_km": a.get("prof"),
            "profundidad_cat": str(a.get("profundidad") or ""),
            "referencia": str(a.get("ref") or ""),
            "intensidad": str(a.get("int_") or ""),
            "sentido": str(a.get("sentido") or ""),
            "magnitud": _dec(a.get("magnitud")) if a.get("magnitud") is not None else None,
            "departamento": str(a.get("departamento") or ""),
            "ingresado_ts": int(time.time()*1000),
            "source": "IGP-ArcGIS"
        })
    # DynamoDB no acepta atributos None
    return [{k:v for k,v in it.items() if v is not None} for it in items]

def upsert(items):
    ddb = boto3.resource("dynamodb").Table(DDB_TABLE)
    with ddb.batch_writer(overwrite_by_pkeys=["code"]) as b:
        for it in items: b.put_item(Item=it)

def lambda_ingestar(event, context):
    try:
        items = fetch_last_10(); upsert(items)
        return {"statusCode":200,"headers":{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"},
                "body":json.dumps({"ingresados":len(items),"items":items}, default=str)}
    except Exception as e:
        return {"statusCode":500,"body":json.dumps({"error":str(e)})}

def lambda_listar(event, context):
    try:
        ddb = boto3.resource("dynamodb").Table(DDB_TABLE)
        resp = ddb.scan(Limit=50)
        items = resp.get("Items", [])
        items.sort(key=lambda x: x.get("fechaevento", x.get("ingresado_ts", 0)), reverse=True)
        items = items[:10]
        return {"statusCode":200,"headers":{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"},
                "body":json.dumps({"count":len(items),"items":items}, default=str)}
    except Exception as e:
        return {"statusCode":500,"body":json.dumps({"error":str(e)})}
