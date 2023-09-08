from pydantic import BaseModel
from typing import Optional

class PartidoSchema(BaseModel):
    id_partido: str
    nombrePartido: str
    Siglas: str
    Fundacion: str
