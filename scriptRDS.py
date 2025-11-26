import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import os
import mysql.connector
from esquema import SQL_MYSQL

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



def main():
    rds = conectarse()
    endpoint=crear_instanciaRDS(rds)
    crear_baseDatos(endpoint)




if __name__ == "__main__":
    main()