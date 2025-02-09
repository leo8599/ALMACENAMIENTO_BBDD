# %%
# ## Ejercicios de operaciones CRUD en MongoDB

# Comenzamos por importar algunos paquetes que utilizaremos
import yaml
from pathlib import Path

# pymongo es el driver de Python para ineractuar con el servidor
from pymongo import MongoClient
from pymongo import errors

# utilizamos `pandas` para manipular los datos
import pandas as pd

# Usamos `pprint` para imprimir mejor los diccionarios
import pprint

# %% 
# Instanciamos un `PrettyPrinter`
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

client = MongoClient(
    data_db["ip_servidor"],
    data_db["puerto_servidor"],
    username=data_db["usuario"],
    password=data_db["password"],
    authSource=data_db["auth_db"]
)

client

# %%
# Obtenemos la lista de las bases de datos almacenadas en el serivdor
# y disponibles para el usuario

dbs_in_server = client.list_database_names()
dbs_in_server

# %%
# Especificamos la base de datos y la coleccion con la que vamos a trabajar
# Hay que poner el nombre de la base de datos que hemos trabajado en clase

db_name = ""
data_db = client[db_name]

colls_in_db = data_db.list_collection_names()
colls_in_db

# Vamos a usar esta colección, que previamente ya subimos a la base de datos
coll_name = "HotelReservations"
data_coll = data_db[coll_name]

# %% 
# Obtenemos un documento de prueba para verificar la estructura

test_doc = data_coll.find_one()
pp.pprint(test_doc)

# %%
# ## Ejercicios:

# %% 
# ### 1: Obtenga todas las reservaciones canceladas

# ### 2: Calcule las cancelaciones por mes (sin considerar año)

# ### 3: Identifique las reservaciones activas

# ### 4: Calcule las reservaciones mensuales (sin considerar año)

# ### 5: Identifique los meses con reservaciones con niños

# ### 6: las reservaciones especiales están relacionadas con niños?

# ### 7: en que segmento de mercado se cancelan más reservaciones?

# ### 8: se cancelan más reservaciones al inicio o al fin del mes?

# ### 9: identifique las reservaciones con niños o con pedidos especiales

# ### 10: como identificaría a las reservaciones asociadas a clientes VIP?
