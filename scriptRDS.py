import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import os
import mysql.connector
from faker import Faker
import random
import json
from decimal import Decimal
from datetime import date

load_dotenv()


def conectarse():
    session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION"))


    rds = session.client('rds')

    response = rds.describe_db_instances()
    print(response)
    return rds


def crear_instanciaRDS(rds):
    DB_INSTANCE_ID = os.getenv("DB_INSTANCE_ID")

    try:
        print("Buscando Instancia")
        info = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)

    except ClientError as e:
        print("Creando Instancia")
        rds.create_db_instance(
           DBInstanceIdentifier=DB_INSTANCE_ID, #Nombre de la instancia del RDS
           AllocatedStorage= 20, #El tamaño del RDS
           DBInstanceClass="db.t4g.micro", #Tipo de clase de la base de datos
           Engine="mariadb", # motor de base de datos
           MasterUsername=os.getenv("DB_USER"), #usuario de la base de datos
           MasterUserPassword=os.getenv("DB_PASSWORD"), #password del usuario admin
           PubliclyAccessible=True #Publicar el RDS
        )

        waiter = rds.get_waiter('db_instance_available') #Usaremos el waiter para continuar con el código cuando esté disponible el RDS
        waiter.wait(DBInstanceIdentifier=DB_INSTANCE_ID)

        info = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)
    
    endpoint = info['DBInstances'][0]['Endpoint']['Address']
    return endpoint

def crear_baseDatos(endpoint):
    config = {
       "user": os.getenv("DB_USER"),
       "password": os.getenv("DB_PASSWORD"),
       "host": endpoint
      
    }

    DB_NAME = os.getenv("DB_NAME")
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(dictionary=True)

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cursor.execute(f"USE {DB_NAME}")

    TABLES = {}

    TABLES['Usuarios'] = (
        "CREATE TABLE IF NOT EXISTS Usuarios ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " DNI VARCHAR(20) UNIQUE,"
        " Nombre VARCHAR(100),"
        " Apellido VARCHAR(100),"
        " Edad INT,"
        " Email VARCHAR(150) UNIQUE,"
        " Telefono VARCHAR(20),"
        " Sexo ENUM('M', 'F', 'Otro'),"
        " Nacionalidad VARCHAR(100),"
        " Contrasena VARCHAR(255),"
        " Domicilio VARCHAR(255),"
        " Disponibilidad VARCHAR(100)"
        ")"
    )

    TABLES['Experiencia_Laboral'] = (
        "CREATE TABLE IF NOT EXISTS Experiencia_Laboral ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " ID_Usuario INT,"
        " Empresa VARCHAR(150),"
        " Puesto VARCHAR(150),"
        " Fecha_Inicio DATE,"
        " Fecha_Fin DATE,"
        " Descripcion TEXT,"
        " Sector VARCHAR(100),"
        " FOREIGN KEY (ID_Usuario) REFERENCES Usuarios(ID) ON DELETE CASCADE"
        ")"
    )

    TABLES['Portal'] = (
        "CREATE TABLE IF NOT EXISTS Portal ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " Nombre VARCHAR(150),"
        " URL VARCHAR(255)"
        ")"
    )

    TABLES['Oferta_Empleo'] = (
        "CREATE TABLE IF NOT EXISTS Oferta_Empleo ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " ID_Portal INT,"
        " Titulo VARCHAR(150),"
        " Empresa VARCHAR(150),"
        " Ubicacion VARCHAR(150),"
        " Descripcion TEXT,"
        " Salario DECIMAL(10,2),"
        " URL_Oferta VARCHAR(255),"
        " Sector VARCHAR(100),"
        " Tipo_Contrato VARCHAR(100),"
        " Duracion VARCHAR(100),"
        " Jornada VARCHAR(100),"
        " FOREIGN KEY (ID_Portal) REFERENCES Portal(ID) ON DELETE SET NULL"
        ")"
    )

    TABLES['Habilidades'] = (
        "CREATE TABLE IF NOT EXISTS Habilidades ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " Nombre VARCHAR(100),"
        " Categoria VARCHAR(100)"
        ")"
    )

    TABLES['Usuario_Habilidad'] = (
        "CREATE TABLE IF NOT EXISTS Usuario_Habilidad ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " ID_Usuario INT,"
        " ID_Habilidad INT,"
        " Nivel VARCHAR(50),"
        " FOREIGN KEY (ID_Usuario) REFERENCES Usuarios(ID) ON DELETE CASCADE,"
        " FOREIGN KEY (ID_Habilidad) REFERENCES Habilidades(ID) ON DELETE CASCADE"
        ")"
    )

    TABLES['Formacion'] = (
        "CREATE TABLE IF NOT EXISTS Formacion ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " Nombre VARCHAR(150),"
        " Tipo VARCHAR(100),"
        " Institucion VARCHAR(150),"
        " Area VARCHAR(100)"
        ")"
    )

    TABLES['Usuario_Formacion'] = (
        "CREATE TABLE IF NOT EXISTS Usuario_Formacion ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " ID_Usuario INT,"
        " ID_Formacion INT,"
        " Anio_Inicio YEAR,"
        " Anio_Fin YEAR,"
        " Titulacion VARCHAR(150),"
        " FOREIGN KEY (ID_Usuario) REFERENCES Usuarios(ID) ON DELETE CASCADE,"
        " FOREIGN KEY (ID_Formacion) REFERENCES Formacion(ID) ON DELETE CASCADE"
        ")"
    )

    TABLES['Oferta_Habilidad'] = (
        "CREATE TABLE IF NOT EXISTS Oferta_Habilidad ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " ID_Oferta INT,"
        " ID_Habilidad INT,"
        " Nivel VARCHAR(50),"
        " FOREIGN KEY (ID_Oferta) REFERENCES Oferta_Empleo(ID) ON DELETE CASCADE,"
        " FOREIGN KEY (ID_Habilidad) REFERENCES Habilidades(ID) ON DELETE CASCADE"
        ")"
    )

    TABLES['Oferta_Formacion'] = (
        "CREATE TABLE IF NOT EXISTS Oferta_Formacion ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " ID_Oferta INT,"
        " ID_Formacion INT,"
        " FOREIGN KEY (ID_Oferta) REFERENCES Oferta_Empleo(ID) ON DELETE CASCADE,"
        " FOREIGN KEY (ID_Formacion) REFERENCES Formacion(ID) ON DELETE CASCADE"
        ")"
    )

    TABLES['Recomendaciones'] = (
        "CREATE TABLE IF NOT EXISTS Recomendaciones ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " ID_Oferta INT,"
        " ID_Usuario INT,"
        " Afinidad DECIMAL(5,2),"
        " FOREIGN KEY (ID_Oferta) REFERENCES Oferta_Empleo(ID) ON DELETE CASCADE,"
        " FOREIGN KEY (ID_Usuario) REFERENCES Usuarios(ID) ON DELETE CASCADE"
        ")"
    )

    TABLES['Formacion_Sugerida'] = (
        "CREATE TABLE IF NOT EXISTS Formacion_Sugerida ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " ID_Formacion INT,"
        " ID_Usuario INT,"
        " Descripcion TEXT,"
        " FOREIGN KEY (ID_Formacion) REFERENCES Formacion(ID) ON DELETE CASCADE,"
        " FOREIGN KEY (ID_Usuario) REFERENCES Usuarios(ID) ON DELETE CASCADE"
        ")"
    )

    TABLES['Tendencias_Laborales'] = (
        "CREATE TABLE IF NOT EXISTS Tendencias_Laborales ("
        " ID INT AUTO_INCREMENT PRIMARY KEY,"
        " ID_Formacion INT,"
        " ID_Habilidad INT,"
        " Sector VARCHAR(100),"
        " Crecimiento_Demanda VARCHAR(100),"
        " Periodo VARCHAR(50),"
        " FOREIGN KEY (ID_Formacion) REFERENCES Formacion(ID) ON DELETE SET NULL,"
        " FOREIGN KEY (ID_Habilidad) REFERENCES Habilidades(ID) ON DELETE SET NULL"
        ")"
    )

    for name, ddl in TABLES.items():
        print(f"Creando tabla {name}...")
        cursor.execute(ddl)
    
    cursor.close()
    cnx.close()

def crearDatos(endpoint):
        
    #Estos son los providers usados:
    #1. faker.providers.person          → Nombres, apellidos, sexo


    # CONEXIONES A LAS BASES DE DATOS

    fake = Faker('es_ES')

    mariadb_conn = mysql.connector.connect(
        host=endpoint,
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

    mariadb_cursor = mariadb_conn.cursor()


    mariadb_cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

    # Primero truncar las tablas que dependen de otras
    mariadb_cursor.execute("TRUNCATE TABLE Usuario_Habilidad")
    mariadb_cursor.execute("TRUNCATE TABLE Usuario_Formacion")
    mariadb_cursor.execute("TRUNCATE TABLE Oferta_Habilidad")
    mariadb_cursor.execute("TRUNCATE TABLE Oferta_Formacion")
    mariadb_cursor.execute("TRUNCATE TABLE Recomendaciones")
    mariadb_cursor.execute("TRUNCATE TABLE Formacion_Sugerida")
    mariadb_cursor.execute("TRUNCATE TABLE Tendencias_Laborales")
    mariadb_cursor.execute("TRUNCATE TABLE Experiencia_Laboral")
    mariadb_cursor.execute("TRUNCATE TABLE Oferta_Empleo")
    mariadb_cursor.execute("TRUNCATE TABLE Portal")
    mariadb_cursor.execute("TRUNCATE TABLE Habilidades")
    mariadb_cursor.execute("TRUNCATE TABLE Formacion")
    mariadb_cursor.execute("TRUNCATE TABLE Usuarios")

    mariadb_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    # FUNCIÓN GENÉRICA DE INSERCIÓN
    def ejecutar_en_todas(query, valores=None):
        try:
            if valores:
                mariadb_cursor.execute(query, valores)
            else:
                mariadb_cursor.execute(query)
        except Exception as e:
            print("Error:", e)

    # FUNCIONES DE INSERCIÓN DE DATOS BASE
    def insertar_usuarios(n=30):
        for _ in range(n):
            dni = fake.unique.random_int(10000000, 99999999)
            nombre = fake.first_name()
            apellido = fake.last_name()
            edad = random.randint(18, 65)
            email = fake.unique.email()
            telefono = fake.phone_number()
            sexo = random.choice(['M', 'F', 'Otro'])
            nacionalidad = fake.country()
            contrasena = fake.password()
            domicilio = fake.address().replace('\n', ', ')
            disponibilidad = random.choice(['Tiempo completo', 'Medio tiempo', 'Remoto', 'Freelance'])

            query = """
                INSERT INTO Usuarios (DNI, Nombre, Apellido, Edad, Email, Telefono, Sexo, Nacionalidad, Contrasena, Domicilio, Disponibilidad)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            ejecutar_en_todas(query, (dni, nombre, apellido, edad, email, telefono, sexo, nacionalidad, contrasena, domicilio, disponibilidad))

    def insertar_portales(n=5):
        for _ in range(n):
            nombre = fake.company()
            url = fake.url()
            query = "INSERT INTO Portal (Nombre, URL) VALUES (%s, %s)"
            ejecutar_en_todas(query, (nombre, url))

    def insertar_habilidades(n=15):
        for _ in range(n):
            nombre = fake.word().capitalize()
            categoria = random.choice(['Técnica', 'Blanda', 'Gestión', 'Idioma'])
            query = "INSERT INTO Habilidades (Nombre, Categoria) VALUES (%s,%s)"
            ejecutar_en_todas(query, (nombre, categoria))

    def insertar_formaciones(n=15):
        for _ in range(n):
            nombre = fake.job()
            tipo = random.choice(['Grado', 'Máster', 'Curso', 'Diplomado'])
            institucion = fake.company()
            area = random.choice(['Informática', 'Administración', 'Educación', 'Salud'])
            query = "INSERT INTO Formacion (Nombre, Tipo, Institucion, Area) VALUES (%s,%s,%s,%s)"
            ejecutar_en_todas(query, (nombre, tipo, institucion, area))

    def insertar_ofertas(n=20):
        for _ in range(n):
            titulo = fake.job()
            empresa = fake.company()
            ubicacion = fake.city()
            descripcion = fake.text(200)
            salario = round(random.uniform(1000, 8000), 2)
            url = fake.url()
            sector = random.choice(['Tecnología', 'Salud', 'Educación', 'Marketing', 'Finanzas'])
            tipo_contrato = random.choice(['Indefinido', 'Temporal', 'Prácticas', 'Freelance'])
            duracion = random.choice(['6 meses', '1 año', 'Indefinido'])
            jornada = random.choice(['Completa', 'Parcial', 'Remota'])
            id_portal = random.randint(1, 5)

            query = """
                INSERT INTO Oferta_Empleo (ID_Portal, Titulo, Empresa, Ubicacion, Descripcion, Salario, URL_Oferta, Sector, Tipo_Contrato, Duracion, Jornada)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            ejecutar_en_todas(query, (id_portal, titulo, empresa, ubicacion, descripcion, salario, url, sector, tipo_contrato, duracion, jornada))

    def insertar_experiencia(n=40):
        for _ in range(n):
            id_usuario = random.randint(1, 30)
            empresa = fake.company()
            puesto = fake.job()
            fecha_inicio = fake.date_between(start_date='-10y', end_date='-2y')
            fecha_fin = fake.date_between(start_date=fecha_inicio, end_date='today')
            descripcion = fake.text(200)
            sector = random.choice(['Tecnología', 'Educación', 'Salud', 'Finanzas'])
            query = """
                INSERT INTO Experiencia_Laboral (ID_Usuario, Empresa, Puesto, Fecha_Inicio, Fecha_Fin, Descripcion, Sector)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """
            ejecutar_en_todas(query, (id_usuario, empresa, puesto, fecha_inicio, fecha_fin, descripcion, sector))

    def insertar_usuario_habilidad(n=60):
        for _ in range(n):
            id_usuario = random.randint(1, 30)
            id_habilidad = random.randint(1, 15)
            nivel = random.choice(['Básico', 'Intermedio', 'Avanzado', 'Experto'])
            query = "INSERT INTO Usuario_Habilidad (ID_Usuario, ID_Habilidad, Nivel) VALUES (%s,%s,%s)"
            ejecutar_en_todas(query, (id_usuario, id_habilidad, nivel))

    def insertar_usuario_formacion(n=50):
        for _ in range(n):
            id_usuario = random.randint(1, 30)
            id_formacion = random.randint(1, 15)
            anio_inicio = random.randint(2005, 2020)
            anio_fin = anio_inicio + random.randint(1, 3)
            titulacion = fake.job()
            query = """
                INSERT INTO Usuario_Formacion (ID_Usuario, ID_Formacion, Anio_Inicio, Anio_Fin, Titulacion)
                VALUES (%s,%s,%s,%s,%s)
            """
            ejecutar_en_todas(query, (id_usuario, id_formacion, anio_inicio, anio_fin, titulacion))

    def insertar_oferta_habilidad(n=40):
        for _ in range(n):
            id_oferta = random.randint(1, 20)
            id_habilidad = random.randint(1, 15)
            nivel = random.choice(['Básico', 'Intermedio', 'Avanzado'])
            query = "INSERT INTO Oferta_Habilidad (ID_Oferta, ID_Habilidad, Nivel) VALUES (%s,%s,%s)"
            ejecutar_en_todas(query, (id_oferta, id_habilidad, nivel))

    def insertar_oferta_formacion(n=30):
        for _ in range(n):
            id_oferta = random.randint(1, 20)
            id_formacion = random.randint(1, 15)
            query = "INSERT INTO Oferta_Formacion (ID_Oferta, ID_Formacion) VALUES (%s,%s)"
            ejecutar_en_todas(query, (id_oferta, id_formacion))

    def insertar_recomendaciones(n=50):
        for _ in range(n):
            id_oferta = random.randint(1, 20)
            id_usuario = random.randint(1, 30)
            afinidad = round(random.uniform(50, 100), 2)
            query = "INSERT INTO Recomendaciones (ID_Oferta, ID_Usuario, Afinidad) VALUES (%s,%s,%s)"
            ejecutar_en_todas(query, (id_oferta, id_usuario, afinidad))

    def insertar_formacion_sugerida(n=20):
        for _ in range(n):
            id_formacion = random.randint(1, 15)
            id_usuario = random.randint(1, 30)
            descripcion = fake.text(150)
            query = "INSERT INTO Formacion_Sugerida (ID_Formacion, ID_Usuario, Descripcion) VALUES (%s,%s,%s)"
            ejecutar_en_todas(query, (id_formacion, id_usuario, descripcion))

    def insertar_tendencias_laborales(n=20):
        for _ in range(n):
            id_formacion = random.randint(1, 15)
            id_habilidad = random.randint(1, 15)
            sector = random.choice(['Tecnología', 'Educación', 'Salud', 'Finanzas'])
            crecimiento = random.choice(['Alta', 'Media', 'Baja'])
            periodo = random.choice(['2023', '2024', '2025'])
            query = """
                INSERT INTO Tendencias_Laborales (ID_Formacion, ID_Habilidad, Sector, Crecimiento_Demanda, Periodo)
                VALUES (%s,%s,%s,%s,%s)
            """
            ejecutar_en_todas(query, (id_formacion, id_habilidad, sector, crecimiento, periodo))

    # EJECUCIÓN PRINCIPAL

    print("Insertando datos falsos en MariaDB")

    insertar_usuarios()
    insertar_portales()
    insertar_habilidades()
    insertar_formaciones()
    insertar_ofertas()
    insertar_experiencia()
    insertar_usuario_habilidad()
    insertar_usuario_formacion()
    insertar_oferta_habilidad()
    insertar_oferta_formacion()
    insertar_recomendaciones()
    insertar_formacion_sugerida()
    insertar_tendencias_laborales()

    # Confirmar transacciones
    mariadb_conn.commit()

    # Cerrar cursores y conexiones
    mariadb_cursor.close()
    mariadb_conn.close()
    print("Tarea Realizada")

def fetch_all(cursor, query):
    cursor.execute(query)
    cols = [desc[0] for desc in cursor.description]
    result = []
    for row in cursor.fetchall():
        row_dict = {}
        for col, val in zip(cols, row):
            # Convertir Decimal a float
            if isinstance(val, Decimal):
                val = float(val)
            row_dict[col] = val
            if isinstance(val, date):
                val = val.isoformat()
        result.append(row_dict)
    return result

def save_json(filename, data):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Archivo generado: {filename}")

def consultas(endpoint):
    mariadb_conn = mysql.connector.connect(
        host=endpoint,
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

    mariadb_cursor = mariadb_conn.cursor()

    # 1️ Recomendaciones con mayor afinidad por usurios. Esto nos permite que oferta encaja mejor con cada usuario
    q1 = """
        SELECT u.Nombre AS Usuario, o.Titulo AS Oferta, r.Afinidad
        FROM Recomendaciones r
        JOIN Usuarios u ON r.ID_Usuario = u.ID
        JOIN Oferta_Empleo o ON r.ID_Oferta = o.ID
        ORDER BY r.Afinidad DESC
        LIMIT 10;
    """

    # 2️ Promedio de afinidad por sector que permita identificar qué sectores tienen mejor afinidad con los usuarios
    q2 = """
        SELECT o.Sector, AVG(r.Afinidad) AS Afinidad_Promedio
        FROM Recomendaciones r
        JOIN Oferta_Empleo o ON r.ID_Oferta = o.ID
        GROUP BY o.Sector;
    """

    data = {
        "Top10_Recomendaciones": fetch_all(mariadb_cursor, q1),
        "Afinidad_Promedio_Por_Sector": fetch_all(mariadb_cursor, q2),
        "Descripcion": "Análisis de afinidad entre usuarios y ofertas en MariaDB"
    }

    
    mariadb_cursor.close()
    save_json("analisis_mariadb.json", data)

def main():
    rds = conectarse()
    endpoint=crear_instanciaRDS(rds)
    crear_baseDatos(endpoint)
    crearDatos(endpoint)
    consultas(endpoint)
    
if __name__ == "__main__":
    main()