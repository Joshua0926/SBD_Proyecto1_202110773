import csv
from fastapi import APIRouter, UploadFile, File
from sqlalchemy import create_engine, text
from config.db import engine
from config.db import meta_data
from io import StringIO
from datetime import datetime
from typing import List


user = APIRouter()

@user.get("/")
def root():
    return "bases de datos Tribunal Supremo Electoral"
     

# CREAR LA TABLA TEMPORAL PARTIDO
def create_temp_partido_table(connection):
    connection.execute(
        """
        CREATE TEMPORARY TABLE tmp_Partido (
            id_partido INT PRIMARY KEY,
            nombrePartido VARCHAR(50),
            Siglas VARCHAR(10),
            Fundacion DATE
        )
        """
    )


# CREAR LA TABLA PERMANENTE PARTIDO
def create_partido_table(connection):
    # Borra todos los datos de la tabla 'Partido' si ya existe
    if engine.has_table('Partido'):
        connection.execute("DELETE FROM Partido")

    # Crea la tabla 'Partido' si no existe utilizando un script de instrucciones SQL
    if not engine.has_table('Partido'):
        create_table_sql = """
            CREATE TABLE Partido (
                id_partido INT PRIMARY KEY,
                nombrePartido VARCHAR(50),
                Siglas VARCHAR(10),
                Fundacion DATE
            )
        """
        connection.execute(create_table_sql)


# TRANSFERIR LOS DATOS DE LA TABLA TEMPORAL A LA PERMANENTE PARTIDO

def move_data_from_temp_to_permanent_table(connection):
    # Ejecutar la consulta en la conexión proporcionada (conn2)
    stmt = text("""
        INSERT INTO Partido (id_partido, nombrePartido, Siglas, Fundacion)
        SELECT id_partido, nombrePartido, Siglas, Fundacion
        FROM tmp_Partido
    """)
    connection.execute(stmt)




# CREAR LA TABLA TEMPORAL CIUDADANO
def create_temp_ciudadano_table(connection):
    connection.execute(
        """
        CREATE TEMPORARY TABLE tmp_Ciudadano (
            DPI VARCHAR(13) PRIMARY KEY,
            Nombre VARCHAR(50),
            Apellido VARCHAR(50),
            Direccion VARCHAR(100),
            Telefono VARCHAR(10),
            Edad INT,
            Genero VARCHAR(1)
        )
        """
    )

# CREAR LA TABLA PERMANENTE CIUDADANO
def create_ciudadano_table(connection):
    # Borra todos los datos de la tabla 'Ciudadano' si ya existe
    if engine.has_table('Ciudadano'):
        connection.execute("DELETE FROM Ciudadano")

    # Crea la tabla 'Ciudadano' si no existe utilizando un script de instrucciones SQL
    if not engine.has_table('Ciudadano'):
        create_table_sql = """
            CREATE TABLE Ciudadano (
                DPI VARCHAR(13) PRIMARY KEY,
                Nombre VARCHAR(50),
                Apellido VARCHAR(50),
                Direccion VARCHAR(100),
                Telefono VARCHAR(10),
                Edad INT,
                Genero VARCHAR(1)
            )
        """
        connection.execute(create_table_sql)

# TRANSFERIR LOS DATOS DE LA TABLA TEMPORAL A LA PERMANENTE CIUDADANO
def move_data_from_temp_to_permanent_table_ciudadano(connection):
    # Ejecutar la consulta en la conexión proporcionada
    stmt = text("""
        INSERT INTO Ciudadano (DPI, Nombre, Apellido, Direccion, Telefono, Edad, Genero)
        SELECT DPI, Nombre, Apellido, Direccion, Telefono, Edad, Genero
        FROM tmp_Ciudadano
    """)
    connection.execute(stmt)




# CREAR LA TABLA TEMPORAL DEPARTAMENTO
def create_temp_departamento_table(connection):
    connection.execute(
        """
        CREATE TEMPORARY TABLE tmp_Departamento (
            id_departamento INT PRIMARY KEY,
            nombre VARCHAR(20)
        )
        """
    )

# CREAR LA TABLA PERMANENTE DEPARTAMENTO
def create_departamento_table(connection):
    # Borra todos los datos de la tabla 'Departamento' si ya existe
    if engine.has_table('Departamento'):
        connection.execute("DELETE FROM Departamento")

    # Crea la tabla 'Departamento' si no existe utilizando un script de instrucciones SQL
    if not engine.has_table('Departamento'):
        create_table_sql = """
            CREATE TABLE Departamento (
                id_departamento INT PRIMARY KEY,
                nombre VARCHAR(20)
            )
        """
        connection.execute(create_table_sql)

# TRANSFERIR LOS DATOS DE LA TABLA TEMPORAL A LA PERMANENTE DEPARTAMENTO
def move_data_from_temp_to_permanent_table_departamento(connection):
    # Ejecutar la consulta en la conexión proporcionada
    stmt = text("""
        INSERT INTO Departamento (id_departamento, nombre)
        SELECT id_departamento, nombre
        FROM tmp_Departamento
    """)
    connection.execute(stmt)




# CREAR LA TABLA TEMPORAL MESA
def create_temp_mesa_table(connection):
    connection.execute(
        """
        CREATE TEMPORARY TABLE tmp_Mesa (
            id_mesa INT PRIMARY KEY,
            id_departamento INT
        )
        """
    )

# CREAR LA TABLA PERMANENTE MESA
def create_mesa_table(connection):
    # Borra todos los datos de la tabla 'Mesa' si ya existe
    if engine.has_table('Mesa'):
        connection.execute("DELETE FROM Mesa")

    # Crea la tabla 'Mesa' si no existe utilizando un script de instrucciones SQL
    if not engine.has_table('Mesa'):
        create_table_sql = """
            CREATE TABLE Mesa (
                id_mesa INT PRIMARY KEY,
                id_departamento INT,
                FOREIGN KEY (id_departamento) REFERENCES Departamento(id_departamento)
            )
        """
        connection.execute(create_table_sql)

# TRANSFERIR LOS DATOS DE LA TABLA TEMPORAL A LA PERMANENTE MESA
def move_data_from_temp_to_permanent_table_mesa(connection):
    # Ejecutar la consulta en la conexión proporcionada
    stmt = text("""
        INSERT INTO Mesa (id_mesa, id_departamento)
        SELECT id_mesa, id_departamento
        FROM tmp_Mesa
    """)
    connection.execute(stmt)



# CREAR LA TABLA TEMPORAL CARGO
def create_temp_cargo_table(connection):
    connection.execute(
        """
        CREATE TEMPORARY TABLE tmp_Cargo (
            id_cargo INT PRIMARY KEY,
            cargo VARCHAR(40)
        )
        """
    )

# CREAR LA TABLA PERMANENTE CARGO
def create_cargo_table(connection):
    # Borra todos los datos de la tabla 'Cargo' si ya existe
    if engine.has_table('Cargo'):
        connection.execute("DELETE FROM Cargo")

    # Crea la tabla 'Cargo' si no existe utilizando un script de instrucciones SQL
    if not engine.has_table('Cargo'):
        create_table_sql = """
            CREATE TABLE Cargo (
                id_cargo INT PRIMARY KEY,
                cargo VARCHAR(40)
            )
        """
        connection.execute(create_table_sql)

# TRANSFERIR LOS DATOS DE LA TABLA TEMPORAL A LA PERMANENTE CARGO
def move_data_from_temp_to_permanent_table_cargo(connection):
    # Ejecutar la consulta en la conexión proporcionada
    stmt = text("""
        INSERT INTO Cargo (id_cargo, cargo)
        SELECT id_cargo, cargo
        FROM tmp_Cargo
    """)
    connection.execute(stmt)




# CREAR LA TABLA TEMPORAL CANDIDATO
def create_temp_candidato_table(connection):
    connection.execute(
        """
        CREATE TEMPORARY TABLE tmp_Candidato (
            id_candidato INT PRIMARY KEY,
            nombre VARCHAR(50),
            fecha_nacimiento DATE,
            id_partido INT,
            id_cargo INT
        )
        """
    )

# CREAR LA TABLA PERMANENTE CANDIDATO
def create_candidato_table(connection):
    # Borra todos los datos de la tabla 'Candidato' si ya existe
    if engine.has_table('Candidato'):
        connection.execute("DELETE FROM Candidato")

    # Crea la tabla 'Candidato' si no existe utilizando un script de instrucciones SQL
    if not engine.has_table('Candidato'):
        create_table_sql = """
            CREATE TABLE Candidato (
                id_candidato INT PRIMARY KEY,
                nombre VARCHAR(50),
                fecha_nacimiento DATE,
                id_partido INT,
                id_cargo INT,
                FOREIGN KEY (id_partido) REFERENCES Partido(id_partido),
                FOREIGN KEY (id_cargo) REFERENCES Cargo(id_cargo)
            )
        """
        connection.execute(create_table_sql)

# TRANSFERIR LOS DATOS DE LA TABLA TEMPORAL A LA PERMANENTE CANDIDATO
def move_data_from_temp_to_permanent_table_candidato(connection):
    # Ejecutar la consulta en la conexión proporcionada
    stmt = text("""
        INSERT INTO Candidato (id_candidato, nombre, fecha_nacimiento, id_partido, id_cargo)
        SELECT id_candidato, nombre, fecha_nacimiento, id_partido, id_cargo
        FROM tmp_Candidato
    """)
    connection.execute(stmt)






# CREAR LA TABLA TEMPORAL VOTO
def create_temp_voto_table(connection):
    connection.execute(
        """
        CREATE TEMPORARY TABLE tmp_Voto (
            id_voto INT,
            id_candidato INT,
            dpi VARCHAR(13),
            id_mesa INT,
            fecha_hora DATETIME
        )
        """
    )

# CREAR LA TABLA PERMANENTE VOTO
def create_voto_table(connection):
    # Borra todos los datos de la tabla 'Voto' si ya existe
    if engine.has_table('Voto'):
        connection.execute("DELETE FROM Voto")

    # Crea la tabla 'Voto' si no existe utilizando un script de instrucciones SQL
    if not engine.has_table('Voto'):
        create_table_sql = """
            CREATE TABLE Voto (
                id_voto INT PRIMARY KEY,
                dpi VARCHAR(13),
                id_mesa INT,
                fecha_hora DATETIME,
                FOREIGN KEY (dpi) REFERENCES Ciudadano(DPI),
                FOREIGN KEY (id_mesa) REFERENCES Mesa(id_mesa)
            )
        """
        connection.execute(create_table_sql)

# CREAR LA TABLA PERMANENTE DETALLE_VOTO
def create_detalle_voto_table(connection):
    # Borra todos los datos de la tabla 'Detalle_Voto' si ya existe
    if engine.has_table('Detalle_Voto'):
        connection.execute("DELETE FROM Detalle_Voto")

    # Crea la tabla 'Detalle_Voto' si no existe utilizando un script de instrucciones SQL
    if not engine.has_table('Detalle_Voto'):
        create_table_sql = """
            CREATE TABLE Detalle_Voto (
                id_detalle INT AUTO_INCREMENT PRIMARY KEY,
                id_voto INT,
                id_candidato INT,
                FOREIGN KEY (id_voto) REFERENCES Voto(id_voto),
                FOREIGN KEY (id_candidato) REFERENCES Candidato(id_candidato)
            )
        """
        connection.execute(create_table_sql)

# TRANSFERIR LOS DATOS DE LA TABLA TEMPORAL A LA PERMANENTE VOTO
def move_data_from_temp_to_permanent_table_voto(connection):
    # Ejecutar la consulta en la conexión proporcionada
    stmt = text("""
        INSERT IGNORE INTO Voto (id_voto, dpi, id_mesa, fecha_hora)
        SELECT id_voto, dpi, id_mesa, fecha_hora
        FROM tmp_Voto
    """)
    connection.execute(stmt)

# TRANSFERIR LOS DATOS DE LA TABLA TEMPORAL A LA PERMANENTE DETALLE_VOTO
def move_data_from_temp_to_permanent_table_detalle_voto(connection):
    # Ejecutar la consulta en la conexión proporcionada
    stmt = text("""
        INSERT INTO Detalle_Voto (id_voto, id_candidato)
        SELECT id_voto, id_candidato
        FROM tmp_Voto
    """)
    connection.execute(stmt)









@user.get("/crearmodelo")
async def create_model():

    # Empezar la conexión con la base de datos y crear las tablas
    with engine.begin() as conn1:
        create_partido_table(conn1)  # Esto crea la tabla Partido si no existe
        create_ciudadano_table(conn1)
        create_departamento_table(conn1)
        create_mesa_table(conn1)
        create_cargo_table(conn1)
        create_candidato_table(conn1)
        create_voto_table(conn1)
        create_detalle_voto_table(conn1)

    return "Modelo Creado Exitosamente"


@user.get("/eliminarmodelo")
async def delete_model():

    # Empezar la conexión con la base de datos y crear las tablas
    with engine.begin() as conn1:
        #Eliminar la tabla Detalle_Voto si existe
        if engine.has_table('Detalle_Voto'):
            conn1.execute("DROP TABLE Detalle_Voto")
        #Eliminar la tabla Voto si existe
        if engine.has_table('Voto'):
            conn1.execute("DROP TABLE Voto")
        #Eliminar la tabla Candidato si existe
        if engine.has_table('Candidato'):
            conn1.execute("DROP TABLE Candidato")
        #Eliminar la tabla Partido si existe
        if engine.has_table('Partido'):
            conn1.execute("DROP TABLE Partido")
        #Eliminar la tabla Ciudadano si existe
        if engine.has_table('Ciudadano'):
            conn1.execute("DROP TABLE Ciudadano")
        #Eliminar la tabla Mesa si existe
        if engine.has_table('Mesa'):
            conn1.execute("DROP TABLE Mesa")
        #Eliminar la tabla Departamento si existe
        if engine.has_table('Departamento'):
            conn1.execute("DROP TABLE Departamento")
        #Eliminar la tabla Cargo si existe
        if engine.has_table('Cargo'):
            conn1.execute("DROP TABLE Cargo")

    return "Modelo Eliminado Exitosamente"
        
        
       

@user.get("/cargartabtemp")
async def create_partido():

    # Leer el archivo CSV y cargarlo en la tabla temporal en MySQL
    with engine.begin() as conn1:
        # Conn1 se usa para cargar datos en tmp_Partido
        create_temp_partido_table(conn1)
        # Lee el contenido del archivo CSV
        with open('Archivos de Entrada\partidos.csv', 'r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            # Itera sobre las filas del archivo CSV e inserta los datos en la tabla temporal
            for row in csv_reader:
                # Convierte la fecha al formato 'YYYY-MM-DD'
                fundacion = datetime.strptime(row['Fundacion'], '%d/%m/%Y').strftime('%Y-%m-%d')

                # Actualiza el valor en el diccionario
                row['Fundacion'] = fundacion

                # Incluir 'id_partido' en la inserción, ya que no es autoincremental
                stmt = text("INSERT INTO tmp_Partido (id_partido, nombrePartido, Siglas, Fundacion) VALUES (:id_partido, :nombrePartido, :Siglas, :Fundacion)")
                conn1.execute(stmt, **row)

        create_partido_table(conn1)  # Esto crea la tabla Partido si no existe

        # Ahora, obtén una conexión directa para transferir los datos a la tabla permanente
        move_data_from_temp_to_permanent_table(conn1)  # Esto mueve los datos a la tabla permanente usando conn2

        conn1.execute("DROP TABLE tmp_Partido")



        # Conn1 se usa para cargar datos en tmp_Ciudadano
        create_temp_ciudadano_table(conn1)
        # Lee el contenido del archivo CSV
        with open('Archivos de Entrada\ciudadanos.csv', 'r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            # Itera sobre las filas del archivo CSV e inserta los datos en la tabla temporal
            for row in csv_reader:

        
                stmt = text("INSERT INTO tmp_Ciudadano (DPI, Nombre, Apellido, Direccion, Telefono, Edad, Genero) VALUES (:DPI, :Nombre, :Apellido, :Direccion, :Telefono, :Edad, :Genero)")    
                conn1.execute(stmt, **row)

        create_ciudadano_table(conn1)  # Esto crea la tabla Partido si no existe

        # Ahora, obtén una conexión directa para transferir los datos a la tabla permanente
        move_data_from_temp_to_permanent_table_ciudadano(conn1)  # Esto mueve los datos a la tabla permanente usando conn2

        conn1.execute("DROP TABLE tmp_Ciudadano")



        # Conn1 se usa para cargar datos en tmp_Departamento
        create_temp_departamento_table(conn1)
        # Lee el contenido del archivo CSV
        with open('Archivos de Entrada\departamentos.csv', 'r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            # Itera sobre las filas del archivo CSV e inserta los datos en la tabla temporal
            for row in csv_reader:

        
                stmt = text("INSERT INTO tmp_Departamento (id_departamento, nombre) VALUES (:id_departamento, :nombre)")    
                conn1.execute(stmt, **row)

        create_departamento_table(conn1)  # Esto crea la tabla Partido si no existe

        # Ahora, obtén una conexión directa para transferir los datos a la tabla permanente
        move_data_from_temp_to_permanent_table_departamento(conn1)  # Esto mueve los datos a la tabla permanente usando conn2

        conn1.execute("DROP TABLE tmp_Departamento")



        # Conn1 se usa para cargar datos en tmp_Mesa
        create_temp_mesa_table(conn1)
        # Lee el contenido del archivo CSV
        with open('Archivos de Entrada\mesas.csv', 'r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            # Itera sobre las filas del archivo CSV e inserta los datos en la tabla temporal
            for row in csv_reader:

        
                stmt = text("INSERT INTO tmp_Mesa (id_mesa, id_departamento) VALUES (:id_mesa, :id_departamento)")    
                conn1.execute(stmt, **row)

        create_mesa_table(conn1)  # Esto crea la tabla Partido si no existe

        # Ahora, obtén una conexión directa para transferir los datos a la tabla permanente
        move_data_from_temp_to_permanent_table_mesa(conn1)  # Esto mueve los datos a la tabla permanente usando conn2

        conn1.execute("DROP TABLE tmp_Mesa")

        # Conn1 se usa para cargar datos en tmp_Cargo
        create_temp_cargo_table(conn1)
        # Lee el contenido del archivo CSV
        with open('Archivos de Entrada\cargos.csv', 'r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            # Itera sobre las filas del archivo CSV e inserta los datos en la tabla temporal
            for row in csv_reader:

        
                stmt = text("INSERT INTO tmp_Cargo (id_cargo, cargo) VALUES (:id_cargo, :cargo)")    
                conn1.execute(stmt, **row)
        
        create_cargo_table(conn1)  # Esto crea la tabla Partido si no existe

        # Ahora, obtén una conexión directa para transferir los datos a la tabla permanente
        move_data_from_temp_to_permanent_table_cargo(conn1)

        conn1.execute("DROP TABLE tmp_Cargo")

        # Conn1 se usa para cargar datos en tmp_Candidato
        create_temp_candidato_table(conn1)
        # Lee el contenido del archivo CSV
        with open('Archivos de Entrada\candidatos.csv', 'r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            # Itera sobre las filas del archivo CSV e inserta los datos en la tabla temporal
            for row in csv_reader:

                # Convierte la fecha al formato 'YYYY-MM-DD'
                fecha_nacimiento = datetime.strptime(row['fecha_nacimiento'], '%d/%m/%Y').strftime('%Y-%m-%d')

                # Actualiza el valor en el diccionario
                row['fecha_nacimiento'] = fecha_nacimiento

        
                stmt = text("INSERT INTO tmp_Candidato (id_candidato, nombre, fecha_nacimiento, id_partido, id_cargo) VALUES (:id_candidato, :nombre, :fecha_nacimiento, :id_partido, :id_cargo)")    
                conn1.execute(stmt, **row)

        create_candidato_table(conn1)  # Esto crea la tabla Partido si no existe

        # Ahora, obtén una conexión directa para transferir los datos a la tabla permanente
        move_data_from_temp_to_permanent_table_candidato(conn1)

        conn1.execute("DROP TABLE tmp_Candidato")

        # Conn1 se usa para cargar datos en tmp_Voto
        create_temp_voto_table(conn1)
        # Lee el contenido del archivo CSV
        with open('Archivos de Entrada/votaciones.csv', 'r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)

            # Itera sobre las filas del archivo CSV e inserta los datos en la tabla temporal
            for row in csv_reader:

                # Convierte la fecha al formato 'YYYY-MM-DD HH:MM:SS'
                fecha_hora = datetime.strptime(row['fecha_hora'], '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')

                # Actualiza el valor en el diccionario
                row['fecha_hora'] = fecha_hora

        
                stmt = text("INSERT INTO tmp_Voto (id_voto, id_candidato, dpi, id_mesa, fecha_hora) VALUES (:id_voto, :id_candidato, :dpi, :id_mesa, :fecha_hora)")    
                conn1.execute(stmt, **row)
        
        create_voto_table(conn1)  # Esto crea la tabla Partido si no existe
        create_detalle_voto_table(conn1)

        # Ahora, obtén una conexión directa para transferir los datos a la tabla permanente
        move_data_from_temp_to_permanent_table_voto(conn1)
        move_data_from_temp_to_permanent_table_detalle_voto(conn1)

        conn1.execute("DROP TABLE tmp_Voto")
    
   

    return "Carga Masiva Exitosa"


# Definir un modelo para la respuesta
class CandidatoResponse:
    def __init__(self, nombre_presidente: str, nombre_vicepresidente: str, partido: str):
        self.nombre_presidente = nombre_presidente
        self.nombre_vicepresidente = nombre_vicepresidente
        self.partido = partido


# Endpoint para consultar candidatos a presidentes y vicepresidentes por partido
@user.get("/consulta1")
async def get_candidatos():
    # Iniciar una conexión a la base de datos
    with engine.connect() as conn:
        # Consulta SQL para obtener los nombres de los candidatos
        consulta_sql = text("""
            SELECT c1.nombre AS nombre_presidente, c2.nombre AS nombre_vicepresidente, p.nombrePartido AS partido
            FROM Candidato c1
            JOIN Candidato c2 ON c1.id_partido = c2.id_partido
            JOIN Partido p ON c1.id_partido = p.id_partido
            WHERE c1.id_cargo = 1 AND c2.id_cargo = 2
        """)

        # Ejecutar la consulta SQL
        resultados = conn.execute(consulta_sql).fetchall()

        # Crear una lista de objetos CandidatoResponse a partir de los resultados
        candidatos_response = [CandidatoResponse(nombre_presidente=row.nombre_presidente,
                                                 nombre_vicepresidente=row.nombre_vicepresidente,
                                                 partido=row.partido)
                               for row in resultados]

        return candidatos_response
    


# Definir un modelo para la respuesta
class CandidatoResponse2:
    def __init__(self, partido: str, num_candidatos: int):
        self.partido = partido
        self.num_candidatos = num_candidatos

@user.get("/consulta2")
async def get_candidatos_diputados():
    
    # Consulta SQL para obtener el número de candidatos a diputados por partido
    consulta_sql = text("""
        SELECT p.nombrePartido AS partido, COUNT(*) AS num_candidatos
        FROM Candidato c
        JOIN Partido p ON c.id_partido = p.id_partido
        WHERE c.id_cargo IN (3, 4, 5)  # IDs de los cargos de diputados
        GROUP BY p.nombrePartido
    """)

    # Ejecuta la consulta SQL
    with engine.connect() as conn:
        resultados = conn.execute(consulta_sql).fetchall()

    # Formatea los resultados como una lista de objetos CandidatoResponse
    candidatos_por_partido = [CandidatoResponse2(partido=row["partido"], num_candidatos=row["num_candidatos"]) for row in resultados]

    return {"candidatos_por_partido": candidatos_por_partido}



# Definir un modelo para la respuesta
class CandidatoResponse3:
    def __init__(self, partido: str, nombre_candidato: str):
        self.partido = partido
        self.nombre_candidato = nombre_candidato

@user.get("/consulta3")
async def get_candidatos_alcalde():
   
    # Consulta SQL para obtener el nombre de los candidatos a alcalde por partido
    consulta_sql = text("""
        SELECT p.nombrePartido AS partido, c.nombre AS nombre_candidato
        FROM Candidato c
        JOIN Partido p ON c.id_partido = p.id_partido
        WHERE c.id_cargo = 6  # ID del cargo de alcalde
    """)

    # Ejecuta la consulta SQL
    with engine.connect() as conn:
        resultados = conn.execute(consulta_sql).fetchall()

    # Formatea los resultados como una lista de objetos CandidatoResponse
    candidatos_por_partido = [CandidatoResponse3(partido=row["partido"], nombre_candidato=row["nombre_candidato"]) for row in resultados]

    return {"candidatos_alcalde_por_partido": candidatos_por_partido}
    

# Definir un modelo para la respuesta
class CandidatoResponse4:
    def __init__(self, partido: str, total_candidatos: int):
        self.partido = partido
        self.total_candidatos = total_candidatos

@user.get("/consulta4")
async def get_candidatos_por_partido():
 
    # Consulta SQL para obtener el total de candidatos por partido
    consulta_sql = text("""
        SELECT p.nombrePartido AS partido, COUNT(*) AS total_candidatos
        FROM Candidato c
        JOIN Partido p ON c.id_partido = p.id_partido
        GROUP BY p.nombrePartido
    """)

    # Ejecuta la consulta SQL
    with engine.connect() as conn:
        resultados = conn.execute(consulta_sql).fetchall()

    # Formatea los resultados como una lista de objetos CandidatoResponse
    candidatos_por_partido = [
        CandidatoResponse4(
            partido=row["partido"],
            total_candidatos=row["total_candidatos"]
        )
        for row in resultados
    ]

    return {"candidatos_por_partido": candidatos_por_partido}
 


# Definir un modelo para la respuesta
class VotacionesPorDepartamentoResponse:
    def __init__(self, departamento: str, cantidad_votaciones: int):
        self.departamento = departamento
        self.cantidad_votaciones = cantidad_votaciones

@user.get("/consulta5")
def get_votaciones_por_departamento():
    
    with engine.connect() as conn:
        # Consulta SQL para obtener la cantidad de votaciones por departamento
        consulta_sql = text("""
            SELECT d.nombre AS departamento, COUNT(*) AS cantidad_votaciones
            FROM Voto v
            JOIN Mesa m ON v.id_mesa = m.id_mesa
            JOIN Departamento d ON m.id_departamento = d.id_departamento
            GROUP BY d.nombre
        """)

        # Ejecutar la consulta SQL
        resultados = conn.execute(consulta_sql).fetchall()

        # Formatear los resultados como una lista de objetos VotacionesPorDepartamentoResponse
        votaciones_por_departamento = [
            VotacionesPorDepartamentoResponse(
                departamento=row.departamento,
                cantidad_votaciones=row.cantidad_votaciones
            )
            for row in resultados
        ]

        return {"votaciones_por_departamento": votaciones_por_departamento}
    


@user.get("/consulta6")
def get_votos_nulos():
    try:
        with engine.connect() as conn:
            # Consulta SQL para obtener la cantidad de votos nulos
            consulta_sql = text("""
                SELECT COUNT(*) AS cantidad_votos_nulos
                FROM Detalle_Voto
                WHERE id_candidato = -1
            """)

            # Ejecutar la consulta SQL
            resultado = conn.execute(consulta_sql).fetchone()

            cantidad_votos_nulos = resultado["cantidad_votos_nulos"]

            return {"cantidad_votos_nulos": cantidad_votos_nulos/5, "Total_votos_nulos": cantidad_votos_nulos }
    except Exception as e:
        return {"error": str(e)}
    

@user.get("/consulta7")
def get_top_10_edades_votantes():
    try:
        with engine.connect() as conn:
            # Consulta SQL para obtener el Top 10 de edades de votantes
            consulta_sql = text("""
                SELECT Edad, COUNT(*) AS cantidad_votantes
                FROM Ciudadano c
                INNER JOIN Voto v ON c.DPI = v.dpi
                GROUP BY Edad
                ORDER BY cantidad_votantes DESC, Edad ASC
                LIMIT 10
            """)

            # Ejecutar la consulta SQL
            resultados = conn.execute(consulta_sql).fetchall()

            # Crear una lista de objetos JSON con las edades y la cantidad de votantes
            top_10_edades_votantes = [{"Edad": row.Edad, "Cantidad_Votantes": row.cantidad_votantes} for row in resultados]

            return top_10_edades_votantes
    except Exception as e:
        return {"error": str(e)}



@user.get("/consulta8")
def get_top_10_candidatos_presidente_vicepresidente():
  
    with engine.connect() as conn:
        # Consulta SQL para obtener el Top 10 de candidatos más votados para presidente y vicepresidente
        consulta_sql = text("""
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
        """)

        # Ejecutar la consulta SQL
        resultados = conn.execute(consulta_sql).fetchall()

        # Crear una lista de objetos JSON con los candidatos más votados
        top_10_candidatos_presidente_vicepresidente = [{
            "Nombre_Presidente": row.nombre_presidente,
            "Nombre_Vicepresidente": row.nombre_vicepresidente,
            "Partido": row.partido,
            "Total_Votos": row.total_votos
        } for row in resultados]

        return top_10_candidatos_presidente_vicepresidente


@user.get("/consulta9")
def get_top_5_mesas_frecuentadas():

    with engine.connect() as conn:
        # Consulta SQL para obtener el Top 5 de mesas más frecuentadas
        consulta_sql = text("""
            SELECT m.id_mesa AS numero_mesa, d.nombre AS departamento, COUNT(*) AS total_votos
            FROM Mesa m
            JOIN Departamento d ON m.id_departamento = d.id_departamento
            JOIN Voto v ON m.id_mesa = v.id_mesa
            GROUP BY m.id_mesa, d.nombre
            ORDER BY total_votos DESC
            LIMIT 5
        """)

        # Ejecutar la consulta SQL
        resultados = conn.execute(consulta_sql).fetchall()

        # Crear una lista de objetos JSON con las mesas más frecuentadas
        top_5_mesas_frecuentadas = [{
            "Numero_Mesa": row.numero_mesa,
            "Departamento": row.departamento,
            "Total_Votos": row.total_votos
        } for row in resultados]

        return top_5_mesas_frecuentadas
    

@user.get("/consulta10")
def get_top_5_horas_concurridas():
    try:
        with engine.connect() as conn:
            # Consulta SQL para obtener el Top 5 de las horas más concurridas para votar
            consulta_sql = text("""
                SELECT DATE_FORMAT(fecha_hora, '%H:%i:%s') AS hora, COUNT(*) AS total_votos
                FROM Voto
                GROUP BY hora
                ORDER BY total_votos DESC
                LIMIT 5
            """)

            # Ejecutar la consulta SQL
            resultados = conn.execute(consulta_sql).fetchall()

            # Crear una lista de objetos JSON con las horas más concurridas en formato HH:MM:SS
            top_5_horas_concurridas = [{
                "Hora": row.hora,
                "Total_Votos": row.total_votos
            } for row in resultados]

            return top_5_horas_concurridas
    except Exception as e:
        return {"error": str(e)}
  

@user.get("/consulta11")
def get_cantidad_votos_por_genero():
    
    with engine.connect() as conn:
        # Consulta SQL para obtener la cantidad de votos por género
        consulta_sql = text("""
            SELECT c.Genero, COUNT(v.id_voto) AS Cantidad_Votos
            FROM Ciudadano c
            LEFT JOIN Voto v ON c.DPI = v.dpi
            GROUP BY c.Genero
        """)

        # Ejecutar la consulta SQL
        resultados = conn.execute(consulta_sql).fetchall()

        # Crear un diccionario con la cantidad de votos por género
        cantidad_votos_genero = {row.Genero: row.Cantidad_Votos for row in resultados}

        return cantidad_votos_genero
