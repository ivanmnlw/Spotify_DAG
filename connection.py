import psycopg2
from sqlalchemy import create_engine

connection = psycopg2.connect(
    user = "postgres",
    password = "admin",
    host = "127.0.0.1",
    port = 5432,
    database = "playlist"
)

conn_string = 'postgresql://postgres:admin@127.0.0.1:5432/playlist'
db = create_engine(conn_string)
conn = db.connect()

cur = connection.cursor()