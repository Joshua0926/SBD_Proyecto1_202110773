from fastapi import FastAPI
from router.router import user
# from pydantic import BaseModel
# from typing import Annotated
# import models
# from database import engine, SessionLocal
# from sqlalchemy.orm import Session

app = FastAPI()

app.include_router(user) 




