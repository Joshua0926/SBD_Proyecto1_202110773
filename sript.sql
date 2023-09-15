
-- Crear Tabla temporal Partido
CREATE TEMPORARY TABLE tmp_Partido (
    id_partido INT PRIMARY KEY,
    nombrePartido VARCHAR(255),
    Siglas VARCHAR(255),
    Fundacion DATE
)

-- Crear table temporal Ciudadano
CREATE TEMPORARY TABLE tmp_Ciudadano (
    DPI VARCHAR(255) PRIMARY KEY,
    Nombre VARCHAR(255),
    Apellido VARCHAR(255),
    Direccion VARCHAR(255),
    Telefono VARCHAR(255),
    Edad INT,
    Genero VARCHAR(255)
)