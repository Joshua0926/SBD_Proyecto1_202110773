------------- Creacion de Tablas Temporales -------------

-- Crear Tabla temporal Partido
CREATE TEMPORARY TABLE tmp_Partido (
    id_partido INT PRIMARY KEY,
    nombrePartido VARCHAR(50),
    Siglas VARCHAR(10),
    Fundacion DATE
)

-- Crear Tabla temporal Ciudadano
CREATE TEMPORARY TABLE tmp_Ciudadano (
    DPI VARCHAR(13) PRIMARY KEY,
    Nombre VARCHAR(50),
    Apellido VARCHAR(50),
    Direccion VARCHAR(100),
    Telefono VARCHAR(10),
    Edad INT,
    Genero VARCHAR(1)
)

-- Crear Tabla temporal Departamento
CREATE TEMPORARY TABLE tmp_Departamento (
    id_departamento INT PRIMARY KEY,
    nombre VARCHAR(20)
)

-- Crear Tabla temporal Mesa
CREATE TEMPORARY TABLE tmp_Mesa (
    id_mesa INT PRIMARY KEY,
    id_departamento INT
)

-- Crear Tabla temporal Cargo
CREATE TEMPORARY TABLE tmp_Cargo (
    id_cargo INT PRIMARY KEY,
    cargo VARCHAR(40)
)

-- Crear Tabla temporal Candidato
CREATE TEMPORARY TABLE tmp_Candidato (
    id_candidato INT PRIMARY KEY,
    nombre VARCHAR(50),
    fecha_nacimiento DATE,
    id_partido INT,
    id_cargo INT
)

-- Crear Tabla temporal Voto
CREATE TEMPORARY TABLE tmp_Voto (
    id_voto INT,
    id_candidato INT,
    dpi VARCHAR(13),
    id_mesa INT,
    fecha_hora DATETIME
)


------------- Creacion de Tablas Permanentes -------------

-- Crear Tabla Permanente Partido
CREATE TABLE Partido (
    id_partido INT PRIMARY KEY,
    nombrePartido VARCHAR(50),
    Siglas VARCHAR(10),
    Fundacion DATE
)

-- Crear Tabla Permanente Ciudadano
CREATE TABLE Ciudadano (
    DPI VARCHAR(13) PRIMARY KEY,
    Nombre VARCHAR(50),
    Apellido VARCHAR(50),
    Direccion VARCHAR(100),
    Telefono VARCHAR(10),
    Edad INT,
    Genero VARCHAR(1)
)

-- Crear Tabla Permanente Departamento
CREATE TABLE Departamento (
    id_departamento INT PRIMARY KEY,
    nombre VARCHAR(20)
)

-- Crear Tabla Permanente Mesa
CREATE TABLE Mesa (
    id_mesa INT PRIMARY KEY,
    id_departamento INT,
    FOREIGN KEY (id_departamento) REFERENCES Departamento(id_departamento)
)

-- Crear Tabla Permanente Cargo
CREATE TABLE Cargo (
    id_cargo INT PRIMARY KEY,
    cargo VARCHAR(40)
)

-- Crear Tabla Permanente Candidato
CREATE TABLE Candidato (
    id_candidato INT PRIMARY KEY,
    nombre VARCHAR(50),
    fecha_nacimiento DATE,
    id_partido INT,
    id_cargo INT,
    FOREIGN KEY (id_partido) REFERENCES Partido(id_partido),
    FOREIGN KEY (id_cargo) REFERENCES Cargo(id_cargo)
)

-- Crear Tabla Permanente Voto
CREATE TABLE Voto (
    id_voto INT PRIMARY KEY,
    dpi VARCHAR(13),
    id_mesa INT,
    fecha_hora DATETIME,
    FOREIGN KEY (dpi) REFERENCES Ciudadano(DPI),
    FOREIGN KEY (id_mesa) REFERENCES Mesa(id_mesa)
)

-- Crear Tabla Permanente Detalle Voto
CREATE TABLE Detalle_Voto (
    id_detalle INT AUTO_INCREMENT PRIMARY KEY,
    id_voto INT,
    id_candidato INT,
    FOREIGN KEY (id_voto) REFERENCES Voto(id_voto),
    FOREIGN KEY (id_candidato) REFERENCES Candidato(id_candidato)
)


------------- Inserción de Datos a las Tablas Temporales -------------

INSERT INTO tmp_Partido (id_partido, nombrePartido, Siglas, Fundacion) VALUES (:id_partido, :nombrePartido, :Siglas, :Fundacion)
INSERT INTO tmp_Ciudadano (DPI, Nombre, Apellido, Direccion, Telefono, Edad, Genero) VALUES (:DPI, :Nombre, :Apellido, :Direccion, :Telefono, :Edad, :Genero)
INSERT INTO tmp_Departamento (id_departamento, nombre) VALUES (:id_departamento, :nombre)
INSERT INTO tmp_Mesa (id_mesa, id_departamento) VALUES (:id_mesa, :id_departamento)
INSERT INTO tmp_Cargo (id_cargo, cargo) VALUES (:id_cargo, :cargo)
INSERT INTO tmp_Candidato (id_candidato, nombre, fecha_nacimiento, id_partido, id_cargo) VALUES (:id_candidato, :nombre, :fecha_nacimiento, :id_partido, :id_cargo)
INSERT INTO tmp_Voto (id_voto, id_candidato, dpi, id_mesa, fecha_hora) VALUES (:id_voto, :id_candidato, :dpi, :id_mesa, :fecha_hora)


------------- Transferencia de Tablas Temporales a Permanentes -------------

-- Transferir Tabla temporal Partido a Permanente
INSERT INTO Partido (id_partido, nombrePartido, Siglas, Fundacion)
SELECT id_partido, nombrePartido, Siglas, Fundacion
FROM tmp_Partido

-- Transferir Tabla temporal Ciudadano a Permanente
INSERT INTO Ciudadano (DPI, Nombre, Apellido, Direccion, Telefono, Edad, Genero)
SELECT DPI, Nombre, Apellido, Direccion, Telefono, Edad, Genero
FROM tmp_Ciudadano

-- Transferir Tabla temporal Departamento a Permanente
INSERT INTO Departamento (id_departamento, nombre)
SELECT id_departamento, nombre
FROM tmp_Departamento

-- Transferir Tabla temporal Mesa a Permanente
INSERT INTO Mesa (id_mesa, id_departamento)
SELECT id_mesa, id_departamento
FROM tmp_Mesa

-- Transferir Tabla temporal Cargo a Permanente
INSERT INTO Cargo (id_cargo, cargo)
SELECT id_cargo, cargo
FROM tmp_Cargo

-- Transferir Tabla temporal Candidato a Permanente
INSERT INTO Candidato (id_candidato, nombre, fecha_nacimiento, id_partido, id_cargo)
SELECT id_candidato, nombre, fecha_nacimiento, id_partido, id_cargo
FROM tmp_Candidato

-- Transferir Tabla temporal Voto a Permanente Voto
INSERT IGNORE INTO Voto (id_voto, dpi, id_mesa, fecha_hora)
SELECT id_voto, dpi, id_mesa, fecha_hora
FROM tmp_Voto

-- Transferir Tabla temporal Voto a Permanente Detalle Voto
INSERT INTO Detalle_Voto (id_voto, id_candidato)
SELECT id_voto, id_candidato
FROM tmp_Voto

------------- Eliminacion de Tablas del Modelo -------------

DROP TABLE Detalle_Voto
DROP TABLE Voto
DROP TABLE Candidato
DROP TABLE Partido
DROP TABLE Ciudadano
DROP TABLE Mesa
DROP TABLE Departamento
DROP TABLE Cargo


------------- Consultas a la Base de Datos -------------

-- Candidatos a Presidentes y Vicepresidentes por Partido
SELECT c1.nombre AS nombre_presidente, c2.nombre AS nombre_vicepresidente, p.nombrePartido AS partido
FROM Candidato c1
JOIN Candidato c2 ON c1.id_partido = c2.id_partido
JOIN Partido p ON c1.id_partido = p.id_partido
WHERE c1.id_cargo = 1 AND c2.id_cargo = 2

-- Número de Candidatos a Diputados
SELECT p.nombrePartido AS partido, COUNT(*) AS num_candidatos
FROM Candidato c
JOIN Partido p ON c.id_partido = p.id_partido
WHERE c.id_cargo IN (3, 4, 5)  # IDs de los cargos de diputados
GROUP BY p.nombrePartido

--  Nombre Candidatos a Alcalde por Partido
SELECT p.nombrePartido AS partido, c.nombre AS nombre_candidato
FROM Candidato c
JOIN Partido p ON c.id_partido = p.id_partido
WHERE c.id_cargo = 6  # ID del cargo de alcalde

-- Cantidad de Candidatos por Partido
SELECT p.nombrePartido AS partido, COUNT(*) AS total_candidatos
FROM Candidato c
JOIN Partido p ON c.id_partido = p.id_partido
GROUP BY p.nombrePartido

-- Cantidad de Votaciones por Departamentos
SELECT d.nombre AS departamento, COUNT(*) AS cantidad_votaciones
FROM Voto v
JOIN Mesa m ON v.id_mesa = m.id_mesa
JOIN Departamento d ON m.id_departamento = d.id_departamento
GROUP BY d.nombre

-- Cantidad de Votos Nulos
SELECT COUNT(*) AS cantidad_votos_nulos
FROM Detalle_Voto
WHERE id_candidato = -1

-- Top 10 de eEdad de Ciudadanos que Realizaron su Voto
SELECT Edad, COUNT(*) AS cantidad_votantes
FROM Ciudadano c
INNER JOIN Voto v ON c.DPI = v.dpi
GROUP BY Edad
ORDER BY cantidad_votantes DESC, Edad ASC
LIMIT 10

-- Top 10 de Candidatos más Votados para Presidente y Vicepresidente
SELECT c1.nombre AS nombre_presidente, c2.nombre AS nombre_vicepresidente, 
        p.nombrePartido AS partido, COUNT(*) AS total_votos
FROM Candidato c1
JOIN Candidato c2 ON c1.id_partido = c2.id_partido
JOIN Partido p ON c1.id_partido = p.id_partido
JOIN Detalle_Voto dv ON c1.id_candidato = dv.id_candidato
WHERE c1.id_cargo = 1 AND c2.id_cargo = 2
GROUP BY c1.id_candidato
ORDER BY total_votos DESC
LIMIT 10

-- Top 5 de Mesas más Frecuentadas
SELECT m.id_mesa AS numero_mesa, d.nombre AS departamento, COUNT(*) AS total_votos
FROM Mesa m
JOIN Departamento d ON m.id_departamento = d.id_departamento
JOIN Voto v ON m.id_mesa = v.id_mesa
GROUP BY m.id_mesa, d.nombre
ORDER BY total_votos DESC
LIMIT 5

-- Top 5 la Hora más Concurrida en que los Ciudadanos Fueron a Votar
SELECT DATE_FORMAT(fecha_hora, '%H:%i:%s') AS hora, COUNT(*) AS total_votos
FROM Voto
GROUP BY hora
ORDER BY total_votos DESC
LIMIT 5

-- Cantidad de Votos por Genero
SELECT c.Genero, COUNT(v.id_voto) AS Cantidad_Votos
FROM Ciudadano c
LEFT JOIN Voto v ON c.DPI = v.dpi
GROUP BY c.Genero