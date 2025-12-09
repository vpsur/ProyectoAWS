import boto3
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv
from boto3.dynamodb.conditions import Key, Attr
load_dotenv()

def conectarDynamo():
    session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION"))


    dynamodb = session.resource('dynamodb')
    return dynamodb



def conectarDynamoclient():
    session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION"))


    dynamodb = session.client('dynamodb')
    return dynamodb

def crearTablas(dynamodb):
    try:
        tabla1 = dynamodb.create_table(
            TableName='Usuario',
            AttributeDefinitions=[
                {'AttributeName': 'DNI', 'AttributeType': 'S'}
            ],
            KeySchema=[
                {'AttributeName': 'DNI', 'KeyType': 'HASH'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        tabla1.meta.client.get_waiter('table_exists').wait(TableName='Usuario')
        print("Tabla Usuario creada correctamente")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            tabla1 = dynamodb.Table('Usuario')
            print("Tabla Usuario ya existe, se utilizará la existente")
        else:
            raise e

    try:
        tabla2 = dynamodb.create_table(
            TableName='Formacion',
            AttributeDefinitions=[
                {'AttributeName': 'Curso', 'AttributeType': 'S'},
                {'AttributeName': 'Año', 'AttributeType': 'S'},
                {'AttributeName': 'Tipo', 'AttributeType': 'S'}
            ],
            KeySchema=[
                {'AttributeName': 'Curso', 'KeyType': 'HASH'},
                {'AttributeName': 'Año', 'KeyType': 'RANGE'}
            ],
            LocalSecondaryIndexes=[
                {
                    'IndexName': 'CursoIndex', 
                    'KeySchema': [
                        {'AttributeName': 'Curso', 'KeyType': 'HASH'},
                        {'AttributeName': 'Tipo', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }

        )
        tabla2.meta.client.get_waiter('table_exists').wait(TableName='Formacion')
        print("Tabla Formacion creada correctamente")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            tabla2 = dynamodb.Table('Formacion')
            print("Tabla Formacion ya existe, se utilizará la existente")
        else:
            raise e

    try:
        tabla3 = dynamodb.create_table(
            TableName='Oferta_Empleo',
            AttributeDefinitions=[
                {'AttributeName': 'ID_Oferta', 'AttributeType': 'S'},
                {'AttributeName': 'Sector', 'AttributeType': 'S'}
            ],
            KeySchema=[
                {'AttributeName': 'ID_Oferta', 'KeyType': 'HASH'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'SectorIndex',
                    'KeySchema': [
                        {'AttributeName': 'Sector', 'KeyType': 'HASH'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        tabla3.meta.client.get_waiter('table_exists').wait(TableName='Oferta_Empleo')
        print("Tabla Oferta_Empleo creada correctamente")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            tabla3 = dynamodb.Table('Oferta_Empleo')
            print("Tabla Oferta_Empleo ya existe, se utilizará la existente")
        else:
            raise e

    return tabla1, tabla2, tabla3


def crearRegistros(tabla_usuario, tabla_formacion, tabla_oferta):
    registros_usuario = [
        {'DNI': '12345678A', 'Nombre': 'Juan Pérez', 'Edad': 30},
        {'DNI': '87654321B', 'Nombre': 'María López', 'Edad': 25},
        {'DNI': '11223344C', 'Nombre': 'Carlos Sánchez', 'Edad': 40}
    ]
    registros_formacion = [
        {'Curso': 'Python', 'Año': '2025', 'Tipo': 'Online', 'Duracion': '3 meses'},
        {'Curso': 'AWS', 'Año': '2024', 'Tipo': 'Presencial', 'Duracion': '2 meses'},
        {'Curso': 'Data Science', 'Año': '2025', 'Tipo': 'Online', 'Duracion': '4 meses'}
    ]
    registros_oferta = [
        {'ID_Oferta': 'OF001', 'Titulo': 'Desarrollador Python', 'Sector': 'IT', 'Salario': 35000},
        {'ID_Oferta': 'OF002', 'Titulo': 'Administrador AWS', 'Sector': 'Cloud', 'Salario': 40000},
        {'ID_Oferta': 'OF003', 'Titulo': 'Analista de Datos', 'Sector': 'IT', 'Salario': 38000}
    ]

    for item in registros_usuario:
        tabla_usuario.put_item(Item=item)
    print("Registros insertados en Usuario")

    for item in registros_formacion:
        tabla_formacion.put_item(Item=item)
    print("Registros insertados en Formacion")

    for item in registros_oferta:
        tabla_oferta.put_item(Item=item)
    print("Registros insertados en Oferta_Empleo")

def obtener_registros(tabla_usuario, tabla_formacion, tabla_oferta):
    try:
        response = tabla_usuario.get_item(
            Key={'DNI': '12345678A'}
        )
        item = response.get('Item')
        print("Usuario:", item)
    except Exception as e:
        print("Error al obtener Usuario:", e)

    try:
        response = tabla_formacion.get_item(
            Key={'Curso': 'Python', 'Año': '2025'}
        )
        item = response.get('Item')
        print("Formacion:", item)
    except Exception as e:
        print("Error al obtener Formacion:", e)

    try:
        response = tabla_oferta.get_item(
            Key={'ID_Oferta': 'OF001'}
        )
        item = response.get('Item')
        print("Oferta_Empleo:", item)
    except Exception as e:
        print("Error al obtener Oferta_Empleo:", e)

def actualizar_registros(tabla_usuario, tabla_formacion, tabla_oferta):
    try:
        response = tabla_usuario.update_item(
            Key={'DNI': '12345678A'},
            UpdateExpression='SET Edad = :nueva_edad',
            ExpressionAttributeValues={':nueva_edad': 31},
            ReturnValues='ALL_NEW'
        )
        print("Usuario actualizado:", response['Attributes'])
    except Exception as e:
        print("Error al actualizar Usuario:", e)

    try:
        response = tabla_formacion.update_item(
            Key={'Curso': 'Python', 'Año': '2025'},
            UpdateExpression='SET Duracion = :nueva_duracion',
            ExpressionAttributeValues={':nueva_duracion': '3.5 meses'}, 
            ReturnValues='ALL_NEW'
        )
        print("Formacion actualizada:", response['Attributes'])
    except Exception as e:
        print("Error al actualizar Formacion:", e)

    try:
        response = tabla_oferta.update_item(
            Key={'ID_Oferta': 'OF001'},
            UpdateExpression='SET Salario = :nuevo_salario',
            ExpressionAttributeValues={':nuevo_salario': 36000},
            ReturnValues='ALL_NEW'
        )
        print("Oferta_Empleo actualizada:", response['Attributes'])
    except Exception as e:
        print("Error al actualizar Oferta_Empleo:", e)

def eliminar_registros(tabla_usuario, tabla_formacion, tabla_oferta):
    try:
        tabla_usuario.delete_item(
            Key={'DNI': '12345678A'}
        )
        print("Registro de Usuario eliminado")
    except Exception as e:
        print("Error al eliminar Usuario:", e)

    try:
        tabla_formacion.delete_item(
            Key={'Curso': 'Python', 'Año': '2025'}
        )
        print("Registro de Formacion eliminado")
    except Exception as e:
        print("Error al eliminar Formacion:", e)

    try:
        tabla_oferta.delete_item(
            Key={'ID_Oferta': 'OF001'}
        )
        print("Registro de Oferta_Empleo eliminado")
    except Exception as e:
        print("Error al eliminar Oferta_Empleo:", e)

def obtener_todos_los_registros(tabla_usuario, tabla_formacion, tabla_oferta):
    try:
        response = tabla_usuario.scan()
        items = response.get('Items', [])
        print("Todos los registros de Usuario:")
        for item in items:
            print(item)
    except Exception as e:
        print("Error al obtener todos los registros de Usuario:", e)

    try:
        response = tabla_formacion.scan()
        items = response.get('Items', [])
        print("\nTodos los registros de Formacion:")
        for item in items:
            print(item)
    except Exception as e:
        print("Error al obtener todos los registros de Formacion:", e)

    try:
        response = tabla_oferta.scan()
        items = response.get('Items', [])
        print("\nTodos los registros de Oferta_Empleo:")
        for item in items:
            print(item)
    except Exception as e:
        print("Error al obtener todos los registros de Oferta_Empleo:", e)

def obtener_registros_filtrados(tabla_usuario, tabla_formacion, tabla_oferta):
    try:
        response = tabla_usuario.scan(
            FilterExpression=Attr('Edad').gt(30)
        )
        items = response.get('Items', [])
        print("Usuarios con Edad > 30:")
        for item in items:
            print(item)
    except Exception as e:
        print("Error al filtrar Usuario:", e)

    try:
        response = tabla_formacion.scan(
            FilterExpression=Attr('Tipo').eq('Online')
        )
        items = response.get('Items', [])
        print("\nFormaciones Online:")
        for item in items:
            print(item)
    except Exception as e:
        print("Error al filtrar Formacion:", e)

    try:
        response = tabla_oferta.scan(
            IndexName='SectorIndex',
            FilterExpression=Attr('Sector').eq('IT')
        )
        items = response.get('Items', [])
        print("\nOfertas de empleo en Sector IT:")
        for item in items:
            print(item)
    except Exception as e:
        print("Error al filtrar Oferta_Empleo:", e)

def eliminar_condicional(tabla_usuario, tabla_formacion, tabla_oferta):
    # 1. Usuario: eliminar solo si Edad es 31
    try:
        response = tabla_usuario.delete_item(
            Key={'DNI': '12345678A'},
            ConditionExpression=Attr('Edad').eq(31),
            ReturnValues='ALL_OLD'  # devuelve el item eliminado
        )
        print("Usuario eliminado condicionalmente:", response.get('Attributes'))
    except Exception as e:
        print("Usuario no se eliminó (condición no cumplida):", e)

    # 2. Formacion: eliminar solo si Tipo = 'Online'
    try:
        response = tabla_formacion.delete_item(
            Key={'Curso': 'Python', 'Año': '2025'},
            ConditionExpression=Attr('Tipo').eq('Online'),
            ReturnValues='ALL_OLD'
        )
        print("Formacion eliminada condicionalmente:", response.get('Attributes'))
    except Exception as e:
        print("Formacion no se eliminó (condición no cumplida):", e)

    # 3. Oferta_Empleo: eliminar usando GSI (SectorIndex) solo si Sector = 'IT'
    # NOTA: delete_item no permite eliminar usando GSI directamente, debes obtener la PK primero
    try:
        # 1. Buscar el ID_Oferta a eliminar usando query sobre el GSI
        response_query = tabla_oferta.query(
            IndexName='SectorIndex',
            KeyConditionExpression=Key('Sector').eq('IT')
        )
        items = response_query.get('Items', [])
        for item in items:
            tabla_oferta.delete_item(
                Key={'ID_Oferta': item['ID_Oferta']},
                ConditionExpression=Attr('Sector').eq('IT'),
                ReturnValues='ALL_OLD'
            )
            print("Oferta_Empleo eliminada condicionalmente:", item['ID_Oferta'])
    except Exception as e:
        print("Oferta_Empleo no se eliminó (condición no cumplida):", e)

def obtener_registros_varios_filtros(tabla_usuario, tabla_formacion, tabla_oferta):
    # 1. Usuario: Edad > 25 y Nombre que comience con 'J'
    try:
        response = tabla_usuario.scan(
            FilterExpression=Attr('Edad').gt(25) & Attr('Nombre').begins_with('J')
        )
        items = response.get('Items', [])
        print("Usuarios con Edad > 25 y Nombre comienza con 'J':")
        for item in items:
            print(item)
    except Exception as e:
        print("Error al filtrar Usuario:", e)

    # 2. Formacion: Tipo='Online' y Duracion >= '3 meses'
    try:
        response = tabla_formacion.scan(
            FilterExpression=Attr('Tipo').eq('Online') & Attr('Duracion').gte('3 meses')
        )
        items = response.get('Items', [])
        print("\nFormaciones Online con duración >= 3 meses:")
        for item in items:
            print(item)
    except Exception as e:
        print("Error al filtrar Formacion:", e)

    # 3. Oferta_Empleo: Sector='IT' y Salario > 35000 usando GSI
    try:
        # Primero obtener items desde el GSI SectorIndex
        response_query = tabla_oferta.query(
            IndexName='SectorIndex',
            KeyConditionExpression=Key('Sector').eq('IT'),
            FilterExpression=Attr('Salario').gt(35000)
        )
        items = response_query.get('Items', [])
        print("\nOfertas IT con salario > 35000:")
        for item in items:
            print(item)
    except Exception as e:
        print("Error al filtrar Oferta_Empleo:", e)

def usar_partiql(dynamodb):
    # 1. Usuario: SELECT
    try:
        response = dynamodb.execute_statement(
            Statement="SELECT * FROM Usuario WHERE DNI='12345678A'"
        )
        items = response.get('Items', [])
        print("Usuario con PartiQL:", items)
    except Exception as e:
        print("Error al usar PartiQL en Usuario:", e)

    # 2. Formacion: INSERT
    try:
        response = dynamodb.execute_statement(
            Statement="""
            INSERT INTO Formacion VALUE {'Curso': 'Java', 'Año': '2025', 'Tipo': 'Presencial', 'Duracion': '2 meses'}
            """
        )
        print("Formacion insertada con PartiQL")
    except Exception as e:
        print("Error al insertar en Formacion con PartiQL:", e)

    # 3. Oferta_Empleo: UPDATE
    try:
        response = dynamodb.execute_statement(
            Statement="""
            UPDATE Oferta_Empleo
            SET Salario = 37000
            WHERE ID_Oferta='OF002'
            """
        )
        print("Oferta_Empleo actualizada con PartiQL")
    except Exception as e:
        print("Error al actualizar Oferta_Empleo con PartiQL:", e)

    # 4. Oferta_Empleo: DELETE
    try:
        response = dynamodb.execute_statement(
            Statement="DELETE FROM Oferta_Empleo WHERE ID_Oferta='OF003'"
        )
        print("Oferta_Empleo eliminada con PartiQL")
    except Exception as e:
        print("Error al eliminar Oferta_Empleo con PartiQL:", e)

def main():
    dynamodb = conectarDynamo()
    dynamodbclient = conectarDynamoclient()
    tabla_usuario, tabla_formacion, tabla_oferta = crearTablas(dynamodb)
    crearRegistros(tabla_usuario, tabla_formacion, tabla_oferta)
    obtener_registros(tabla_usuario, tabla_formacion, tabla_oferta)
    actualizar_registros(tabla_usuario, tabla_formacion, tabla_oferta)
    eliminar_registros(tabla_usuario, tabla_formacion, tabla_oferta)
    obtener_todos_los_registros(tabla_usuario, tabla_formacion, tabla_oferta)
    obtener_registros_filtrados(tabla_usuario, tabla_formacion, tabla_oferta)
    eliminar_condicional(tabla_usuario, tabla_formacion, tabla_oferta)
    obtener_registros_varios_filtros(tabla_usuario, tabla_formacion, tabla_oferta)
    usar_partiql(dynamodbclient)
    

if __name__ == "__main__":
    main()