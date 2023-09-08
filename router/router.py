import csv
from fastapi import APIRouter, UploadFile, File
from sqlalchemy import create_engine, text
from config.db import engine
from config.db import meta_data
from io import StringIO
from datetime import datetime


user = APIRouter()

@user.get("/")
def root():
    return {"message": "Hello, I am fastapi with a router "}
     

# CREAR LA TABLA TEMPORAL PARTIDO
def create_temp_partido_table(connection):
    connection.execute(
        """
        CREATE TEMPORARY TABLE tmp_Partido (
            id_partido INT PRIMARY KEY,
            nombrePartido VARCHAR(255),
            Siglas VARCHAR(255),
            Fundacion DATE
        )
        """
    )


# CREAR LA TABLA PERMANETE PARTIDO
def create_partido_table(connection):
    # Borra todos los datos de la tabla 'Partido' si ya existe
    if engine.has_table('Partido'):
        connection.execute("DELETE FROM Partido")

    # Crea la tabla 'Partido' si no existe utilizando un script de instrucciones SQL
    if not engine.has_table('Partido'):
        create_table_sql = """
            CREATE TABLE Partido (
                id_partido INT PRIMARY KEY,
                nombrePartido VARCHAR(255),
                Siglas VARCHAR(255),
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


@user.post("/api/partido")
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
    
  

    return "Success"




@user.put("/api/user")
def update_user():
    pass