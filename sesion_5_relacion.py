## Sesion 5
## Relación entre colecciones

# %%

import yaml
import pprint
from pymongo import MongoClient

from pathlib import Path
import pandas as pd

# Utilizamos pprint para tener una mejor visualización de
# los diccionarios y objetos.
pp = pprint.PrettyPrinter(indent=2)

# %% Leemos archivo yaml con credenciales de acceso

key_file = "credenciales_mongodb.yaml"

# Cargamos informacion en archivo yaml a un diccionario
with open(key_file, "r") as io_file:
    data_db = yaml.safe_load(io_file)

data_db

# %% Conectarnos al servidor de MongoDB

# Información necesaria para conectarnos al servidor:
# IP
# Puerto
# Usuario
# Password
# Base de datos de autenticación

servidor = MongoClient(
    data_db["ip_server"],
    data_db["port_server"],
    username=data_db["user"],
    password=data_db["password"],
    authSource=data_db["auth_db"],
)

servidor

# %%

dbs = list(servidor.list_databases())

print(dbs)

my_db = servidor[dbs[0]["name"]]
my_db

# %% Seleccionamos la colección con la que vamos a trabajar

my_db.list_collection_names()

# %% Collecciones de datos de prueba para practicar (Estudiantes)

students = [
    {
        "_id": 1,
        "name": "Alice",
        "age": 20,
        "grade": "A",
        "email": "alice@example.com",
        "courses": ["Math", "Physics"],
        "courses_ids": [1, 2],
    },
    {
        "_id": 2,
        "name": "Bob",
        "age": 17,
        "grade": "B",
        "email": "bob@example.com",
        "courses": ["Math", "Chemistry"],
        "courses_ids": [1, 3],
    },
    {
        "_id": 3,
        "name": "Charlie",
        "age": 22,
        "grade": "A",
        "email": "charlie@example.com",
        "courses": ["Physics", "Chemistry"],
        "courses_ids": [2, 3],
    },
    {
        "_id": 4,
        "name": "David",
        "age": 19,
        "grade": "B",
        "email": "david@example.com",
        "courses": ["Math", "Computer Science"],
        "courses_ids": [1, 4],
    },
    {
        "_id": 5,
        "name": "Eve",
        "age": 21,
        "grade": "A",
        "email": "eve@example.com",
        "courses": ["Computer Science", "Physics"],
        "courses_ids": [4, 2],
    },
    {
        "_id": 6,
        "name": "Frank",
        "age": 18,
        "grade": "C",
        "email": "frank@example.com",
        "courses": ["Chemistry", "Math"],
        "courses_ids": [3, 1],
    },
    {
        "_id": 7,
        "name": "Grace",
        "age": 23,
        "grade": "A",
        "email": "grace@example.com",
        "courses": ["Physics", "History"],
        "courses_ids": [2, 5],
    },
]

# %% Students con datos incrustados

students_incrustado = [
    {
        "_id": 1,
        "name": "Alice",
        "age": 20,
        "grade": "A",
        "email": "alice@example.com",
        "courses": [
            {"name": "Math", "id": 1},
            {"name": "Physics", "id": 2}
        ]
    },
    {
        "_id": 2,
        "name": "Bob",
        "age": 17,
        "grade": "B",
        "email": "bob@example.com",
        "courses": [
            {"name": "Math", "id": 1},
            {"name": "Chemistry", "id": 3}
        ]
    },
    {
        "_id": 3,
        "name": "Charlie",
        "age": 22,
        "grade": "A",
        "email": "charlie@example.com",
        "courses": [
            {"name": "Physics", "id": 2},
            {"name": "Chemistry", "id": 3}
        ]
    },
    {
        "_id": 4,
        "name": "David",
        "age": 19,
        "grade": "B",
        "email": "david@example.com",
        "courses": [
            {"name": "Math", "id": 1},
            {"name": "Computer Science", "id": 4}
        ]
    },
    {
        "_id": 5,
        "name": "Eve",
        "age": 21,
        "grade": "A",
        "email": "eve@example.com",
        "courses": [
            {"name": "Physics", "id": 2},
            {"name": "Computer Science", "id": 4}
        ]
    },
    {
        "_id": 6,
        "name": "Frank",
        "age": 18,
        "grade": "C",
        "email": "frank@example.com",
        "courses": [
            {"name": "Chemistry", "id": 3},
            {"name": "Math", "id": 1}
        ]
    },
    {
        "_id": 7,
        "name": "Grace",
        "age": 23,
        "grade": "A",
        "email": "grace@example.com",
        "courses": [
            {"name": "Physics", "id": 2},
            {"name": "History", "id": 5}
        ]
    },
]

# %%  Profesores

teachers = [
    {"_id": 1, "name": "Dr. Smith", "subject": "Math", "email": "smith@example.com"},
    {
        "_id": 2,
        "name": "Prof. Johnson",
        "subject": "Physics",
        "email": "johnson@example.com",
    },
    {
        "_id": 3,
        "name": "Dr. Brown",
        "subject": "Chemistry",
        "email": "brown@example.com",
    },
    {
        "_id": 4,
        "name": "Dr. Green",
        "subject": "Computer Science",
        "email": "green@example.com",
    },
    {
        "_id": 5,
        "name": "Prof. White",
        "subject": "History",
        "email": "white@example.com",
    },
]

# %% Cursos

courses = [
    {
        "_id": 1,
        "course_name": "Math",
        "teacher_id": 1,
        "schedule": "Monday 10 AM - 12 PM",
    },
    {
        "_id": 2,
        "course_name": "Physics",
        "teacher_id": 2,
        "schedule": "Tuesday 2 PM - 4 PM",
    },
    {
        "_id": 3,
        "course_name": "Chemistry",
        "teacher_id": 3,
        "schedule": "Wednesday 1 PM - 3 PM",
    },
    {
        "_id": 4,
        "course_name": "Computer Science",
        "teacher_id": 4,
        "schedule": "Thursday 3 PM - 5 PM",
    },
    {
        "_id": 5,
        "course_name": "History",
        "teacher_id": 5,
        "schedule": "Friday 9 AM - 11 AM",
    },
]

# %% Ejercicios
# %% Insertar datos de courses en la base de datos
# Crear (o referenciar) la colección courses
courses_collection = my_db['teachers']

# Insertar los documentos
result = courses_collection.insert_many(teachers)

# Verificar la inserción
print(f"Se insertaron {len(result.inserted_ids)} documentos")
#%%