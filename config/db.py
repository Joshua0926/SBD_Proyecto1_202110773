from sqlalchemy import create_engine, MetaData

engine = create_engine("mysql+pymysql://Joshua:davidosorio26@localhost:3306/tse")

meta_data = MetaData()