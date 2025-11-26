SQL_MYSQL = """
CREATE TABLE IF NOT EXISTS Usuarios (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    DNI VARCHAR(20) UNIQUE,
    Nombre VARCHAR(100),
    Apellido VARCHAR(100),
    Edad INT,
    Email VARCHAR(150) UNIQUE,
    Telefono VARCHAR(20),
    Sexo ENUM('M', 'F', 'Otro'),
    Nacionalidad VARCHAR(100),
    Contrasena VARCHAR(255),
    Domicilio VARCHAR(255),
    Disponibilidad VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS Experiencia_Laboral (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    ID_Usuario INT,
    Empresa VARCHAR(150),
    Puesto VARCHAR(150),
    Fecha_Inicio DATE,
    Fecha_Fin DATE,
    Descripcion TEXT,
    Sector VARCHAR(100),
    FOREIGN KEY (ID_Usuario) REFERENCES Usuarios(ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Prota (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Nombre VARCHAR(150),
    URL VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Oferta_Empleo (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    ID_Portal INT,
    Titulo VARCHAR(150),
    Empresa VARCHAR(150),
    Ubicacion VARCHAR(150),
    Descripcion TEXT,
    Salario DECIMAL(10,2),
    URL_Oferta VARCHAR(255),
    Sector VARCHAR(100),
    Tipo_Contrato VARCHAR(100),
    Duracion VARCHAR(100),
    Jornada VARCHAR(100),
    FOREIGN KEY (ID_Portal) REFERENCES Prota(ID) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS Habilidades (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Nombre VARCHAR(100),
    Categoria VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS Usuario_Habilidad (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    ID_Usuario INT,
    ID_Habilidad INT,
    Nivel VARCHAR(50),
    FOREIGN KEY (ID_Usuario) REFERENCES Usuarios(ID) ON DELETE CASCADE,
    FOREIGN KEY (ID_Habilidad) REFERENCES Habilidades(ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Formacion (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    Nombre VARCHAR(150),
    Tipo VARCHAR(100),
    Institucion VARCHAR(150),
    Area VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS Usuario_Formacion (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    ID_Usuario INT,
    ID_Formacion INT,
    Anio_Inicio YEAR,
    Anio_Fin YEAR,
    Titulacion VARCHAR(150),
    FOREIGN KEY (ID_Usuario) REFERENCES Usuarios(ID) ON DELETE CASCADE,
    FOREIGN KEY (ID_Formacion) REFERENCES Formacion(ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Oferta_Habilidad (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    ID_Oferta INT,
    ID_Habilidad INT,
    Nivel VARCHAR(50),
    FOREIGN KEY (ID_Oferta) REFERENCES Oferta_Empleo(ID) ON DELETE CASCADE,
    FOREIGN KEY (ID_Habilidad) REFERENCES Habilidades(ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Oferta_Formacion (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    ID_Oferta INT,
    ID_Formacion INT,
    FOREIGN KEY (ID_Oferta) REFERENCES Oferta_Empleo(ID) ON DELETE CASCADE,
    FOREIGN KEY (ID_Formacion) REFERENCES Formacion(ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Recomendaciones (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    ID_Oferta INT,
    ID_Usuario INT,
    Afinidad DECIMAL(5,2),
    FOREIGN KEY (ID_Oferta) REFERENCES Oferta_Empleo(ID) ON DELETE CASCADE,
    FOREIGN KEY (ID_Usuario) REFERENCES Usuarios(ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Formacion_Sugerida (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    ID_Formacion INT,
    ID_Usuario INT,
    Descripcion TEXT,
    FOREIGN KEY (ID_Formacion) REFERENCES Formacion(ID) ON DELETE CASCADE,
    FOREIGN KEY (ID_Usuario) REFERENCES Usuarios(ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Tendencias_Laborales (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    ID_Formacion INT,
    ID_Habilidad INT,
    Sector VARCHAR(100),
    Crecimiento_Demanda VARCHAR(100),
    Periodo VARCHAR(50),
    FOREIGN KEY (ID_Formacion) REFERENCES Formacion(ID) ON DELETE SET NULL,
    FOREIGN KEY (ID_Habilidad) REFERENCES Habilidades(ID) ON DELETE SET NULL
);
"""