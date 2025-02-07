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
    data_db["ip_server"],
    data_db["port_server"],
    username=data_db["user"],
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

my_coll = my_db["cars_dataset"]
my_coll

# %% Para probar, extraemos un documento de la colección

test_doc = my_coll.find_one()

pp.pprint(test_doc)
# %% now we are filtering using queries

#%% we extract docs from a collection using emopty query
query={}

cursor = my_coll.find(query)
resultados = list(cursor)
resultados
print(len(resultados))
# %%
## Las condiciones para filtrar los documentos en la colección,
# las expresamos mediante operadores:

# %% Operador de comparación "$eq" (equal)
# Sirve para buscar elementos cuyo atributo en particular
# es igual a la condición.

# 

# Sintaxis: query = {"atributo": {"$eq": "condición"}}

query = {"Make": {"$eq": "Honda"}}
cursor = my_coll.find(query)

resultados = list(cursor)
resultados

print(len(resultados))

for elem in resultados:
    print(elem["Make"])

pp.pprint(resultados[0])

# %%
# Sintaxis: query = {"atributo": {"$eq": "condición"}}

query = {"Make": {"$eq": "Honda"}}

atributos = {"Make":1,"Price":1,"Year":1}

cursor = my_coll.find(query,atributos)

resultados = list(cursor)
resultados

# %% Operadores de comparación:

#  "$gt" (greater than | mayor que)
#  "$gte" (greater than equal | mayor igual que)

#  "$lt" (lesser than | menor que)
#  "$lte" (lesser than equal | menor igual que)
# $and

query = {"Price": {"$gt": 100000},"Year": {"$gt": 2019},"Make": {"$eq": "Honda"}}

atributos = {"Make":1,"Price":1,"Year":1,"Model":1}

cursor = my_coll.find(query,atributos)

resultados = list(cursor)
resultados

for r in resultados:
    print(f'{r["Make"]} | {r["Price"]} | {r["Year"]} | {r["Model"]}')
# %% using $or or $and
query = {"$or":[{"Price": {"$gt": 100000},"Year": {"$gt": 2019},"Make": {"$eq": "Honda"}},{"Price": {"$gt": 100000},"Year": {"$gt": 2019},"Make": {"$eq": "Jeep"}}]}

atributos = {"Make":1,"Price":1,"Year":1,"Model":1}

cursor = my_coll.find(query,atributos)

resultados = list(cursor)
resultados

for r in resultados:
    print(f'{r["Make"]} | {r["Price"]} | {r["Year"]} | {r["Model"]}')


# %%
