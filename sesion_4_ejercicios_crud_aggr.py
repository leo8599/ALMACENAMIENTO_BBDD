## Sesion 4
## Agregación de datos en MongoDB

# %%

import yaml
import pprint
from pymongo import MongoClient

# from pathlib import Path
# import pandas as pd

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
    data_db["ip_servidor"],
    data_db["puerto_servidor"],
    username=data_db["usuario"],
    password=data_db["password"],
    authSource=data_db["auth_db"]
)

servidor

# %%

dbs = list(servidor.list_databases())

print(dbs)

my_db = servidor[dbs[0]["name"]]
my_db

# %% Seleccionamos la colección con la que vamos a trabajar

my_db.list_collection_names()

# %% Seleccionamos la colección con la que vamos a trabajar

# my_coll = my_db["cars_dataset"]
my_coll = my_db["HotelReservations"]

my_coll

# %% Para probar, extraemos un documento de la colección

test_doc = my_coll.find_one()
pp.pprint(test_doc)

# %%
## 1: Identifique las reservaciones activas y modifique su estado a "Arrived"

# %%
## 2: Identifique las reservaciones activas entre junio y agosto
### y agrege el campo "season" con el valor "summer"

# %%
## 3: Identifique las reservaciones activas entre  diciembre y marzo
## y agregue el campo "season" con el valor "winter"


# %%
## 4: Identifique las reservaciones con más week_nights que weekend_nights
## y agregue el campo "guest_type" con el valor "work"


# %%
## 5: Identifique las reservaciones con más weekend_nights que week_nights
## y agregue el campo "guest_type" con el valor "leisure"


# %%
## 6.1: Calcule el total de ganancias por año

# %%
## 6.2: Calcule el total de ganancias por año por mes

# %%
## 7.1: Calcule el total de perdidas por año

# %%
## 7.2: Calcule el total de perdidas por año por mes

# %%
## 8: Identifique las marcas unicas

# %%
## 9: Calcule el precio promedio por marca

# %%
## 10: Calcule el precio promedio por marca por año

# %%
## 11: Identifique los campos únicos de "Owner"

# %%
## 12: Determine el efecto en el precio de los autos de los campos
## Owner, Year y Kilometer
