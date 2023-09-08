from sqlalchemy import Table, Column
from sqlalchemy.sql.sqltypes import Integer, String, Date
from config.db import engine, meta_data


partidos = Table("Partido", meta_data, 
              Column("id_partido", String(50), nullable=False),
              Column("nombrePartido", String(255), nullable=False),
              Column("Siglas", String(255), nullable=False),
              Column("Fundacion", Date, nullable=False))  # Usa

meta_data.create_all(engine)