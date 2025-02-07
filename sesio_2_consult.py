## Sesion 2
## Ejercicio: consultar datos en MongoDB

# %%

import yaml
from pathlib import Path
from pymongo import MongoClient

import pprint

# Utilizamos pprint para tener una mejor visualización de
# los diccionarios y objetos.
pp = pprint.PrettyPrinter(indent=4)

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
    data_db["ip_servidor"],
    data_db["puerto_servidor"],
    username=data_db["usuario"],
    password=data_db["password"],
    authSource=data_db["auth_db"]
)

servidor

# %%

dbs = list(servidor.list_databases())

dbs
# dbs[0]["name"]

# %%

