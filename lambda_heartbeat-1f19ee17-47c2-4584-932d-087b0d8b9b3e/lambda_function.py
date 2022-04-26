import pymysql
from datetime import date, timedelta,datetime
import pytz 
import json,os,ast
endpoint = os.environ.get("RDS_HOSTNAME")
username =  os.environ.get("RDS_USERNAME")
password = os.environ.get("RDS_PASSWORD")
database_name = os.environ.get("RDS_DATABASE")
    

_connection = pymysql.connect(endpoint, user=username,passwd=password, db=database_name)

def lambda_handler(event, context):
    try:
        print(event)
        cursor = _connection.cursor()
        id_agente = event['id_agente']
        SV = pytz.timezone('America/El_Salvador') 
        
        for key,value in event.items():
            hora = datetime.now(SV).strftime('%Y-%m-%d %H:%M:%S')
            if key != "id_agente":
                cursor.execute(f"""REPLACE INTO dbc_estados_dispositivos (elemento, valor, fh_ingreso, id_equipo) VALUES ("{key}","{value}","{hora}","{id_agente}");""")
                cursor.execute(f"""INSERT INTO dbc_historial_estados_dispositivos (elemento, valor, fh_ingreso, id_equipo) VALUES ("{key}","{value}","{hora}","{id_agente}");""")
                _connection.commit()

    except Exception as e:
        print(f"Encontramos un ERROR: {e}")
        return({
            "statusCode": 200,
            "ERROR": e,
        })
    finally:
        cursor.close()
