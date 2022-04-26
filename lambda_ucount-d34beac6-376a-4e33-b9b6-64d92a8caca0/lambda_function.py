import pymysql
import json,os,ast
from datetime import datetime 
import pytz 

endpoint = os.environ.get("RDS_HOSTNAME")
username =  os.environ.get("RDS_USERNAME")
password = os.environ.get("RDS_PASSWORD")
database_name = os.environ.get("RDS_DATABASE")
    

_connection = pymysql.connect(endpoint, user=username,passwd=password, db=database_name)

def lambda_handler(event, context):
    try:
        print(event)
        cursor = _connection.cursor()
        # conteo_data = (ast.literal_eval(event['data']))
        conteo_data = event['data']
        # print(conteo_data)
        id_agente = event['id']
        print(id_agente)
        print(conteo_data)
        for item in conteo_data:
            print(item)
            # hf_conteo = str(item['fh_estadistica'])
            # total = str(item['total'])
            # direccion = str(item['direccion'])
            # id_dispositivo = str(item['id_dispositivo'])
            
            # cursor.execute(f"""REPLACE INTO dbuc_conteo_temp (id_conteo, hf_conteo, total_conteo, direccion_conteo, dispositivo_conteo, id_agente) VALUES (null,"{fh_estadistica}","{total}","{direccion}","{id_dispositivo}","{id_agente}");""")
            
            cursor.execute(f"""REPLACE INTO dbuc_conteo_temp (id_conteo, hf_conteo, total_conteo, direccion_conteo, dispositivo_conteo, id_agente) VALUES (null,"{str(item['fh_estadistica'])}","{str(item['total'])}","{ str(item['direccion'])}","{str(item['id_dispositivo'])}","{id_agente}");""")
            
            _connection.commit()
        print("Replace exitoso!")

    except Exception as e:
        print(f"Encontramos un ERROR: {e}")
        return({
            "statusCode": 200,
            "ERROR": e,
        })
    finally:
        cursor.close()
