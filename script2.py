import boto3
import mysql.connector
import json
import os
from boto3.dynamodb.conditions import Attr
from dotenv import load_dotenv
from decimal import Decimal


load_dotenv()
#conseguir los datos de dynamo
def obtener_dynamodb_filtrada():
    session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION"))


    dynamodb = session.resource('dynamodb')

    tabla_usuario = dynamodb.Table('Usuario')
    tabla_oferta = dynamodb.Table('Oferta_Empleo')


    response_usuarios = tabla_usuario.scan(
        FilterExpression=Attr('Edad').gt(25)
    )
    usuarios = response_usuarios.get('Items', [])


    response_ofertas = tabla_oferta.scan(
        FilterExpression=Attr('Sector').eq('IT')
    )
    ofertas = response_ofertas.get('Items', [])

    return {'usuarios': usuarios, 'ofertas': ofertas}

#Conseguir los datos de rds
def obtener_rds_filtrada():
    session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION"))


    rds = session.client('rds')

    DB_INSTANCE_ID = os.getenv("DB_INSTANCE_ID")
    info = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)
    endpoint = info['DBInstances'][0]['Endpoint']['Address']

    datos = {}


    try:
        mariadb_conn = mysql.connector.connect(
            host=endpoint,
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        datos = {}
        with mariadb_conn.cursor() as cursor:
            cursor.execute("SELECT * FROM Usuarios WHERE Edad > 30;")
            clientes = cursor.fetchall()
            datos['usuarios'] = clientes

            cursor.execute("SELECT * FROM Oferta_Empleo WHERE Salario > 1000;")
            pedidos = cursor.fetchall()
            datos['ofertas'] = pedidos
    except mysql.connector.Error as err:
        print(f"Error de MySQL: {err}")
    finally:
        if 'mariadb_conn' in locals() and mariadb_conn.is_connected():
            mariadb_conn.close()

    return datos

def default_json_serializer(obj):
    """Convierte objetos Decimal a float para que sean serializables en JSON."""
    if isinstance(obj, Decimal):

        return float(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

# Unir y guardar en JSON
def unir_y_guardar_json(dynamodb_data, rds_data, filename='datos_combinados.json'):
    
    datos_combinados = {
        'dynamodb': dynamodb_data,
        'rds': rds_data
    }

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(
            datos_combinados, 
            f, 
            ensure_ascii=False, 
            indent=4,
            default=default_json_serializer
        )

    print(f"Archivo JSON creado: {filename}")


def main():
    dynamodb_data = obtener_dynamodb_filtrada()
    rds_data = obtener_rds_filtrada()
    unir_y_guardar_json(dynamodb_data, rds_data)

if __name__ == "__main__":
    main()
