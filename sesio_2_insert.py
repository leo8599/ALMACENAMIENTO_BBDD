## Sesion 2
## Ejercicio: insertar datos en MongoDB

# %%

import yaml
from pathlib import Path
from pymongo import MongoClient
import pandas as pd
import pprint

# Utilizamos pprint para tener una mejor visualización de
# los diccionarios y objetos.
pp = pprint.PrettyPrinter(indent=4)

# %% Leemos archivo yaml con credenciales de acceso

key_file = "credenciales_mongodb.yaml"

# Cargamos informacion en archivo yaml a un diccionario
with open(key_file, "r") as io_file:
    data_db = yaml.safe_load(io_file)

# 

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

# %% Imprimimos la lista de bases de datos que tenemos disponibles

dbs = list(servidor.list_databases())
dbs

# %% Seleccionamos la base de datos con la que vamos a trabajar.
my_db = servidor[dbs[0]["name"]]
my_db
#%%
# my_coll = my_db["test"]
my_coll = servidor["estDB217273256"]["test"]
my_coll

# %% Imprimimos la lista de colecciones dentro de la base de datos

collections_in_db = my_db.list_collection_names()
collections_in_db
#%% 
cursor = my_coll.find()

documentos = list(cursor)

doc_2 = documentos[2]

print(doc_2["y"])

# %% Definimos rutas a carpetas con conjuntos de datos:
Path.home()


#%%  absolute route

data_path = Path("d://ALMACENAMIENTO_BBDD/datasets")
data_path
#%% Relative path
data_path = Path("datasets")
data_path

#%%

files_in_path = list(data_path.iterdir())
files_in_path
# %%
data_file = files_in_path[3]
data_file

#%% We select the csv file that we are gonna workaaaaaaaaa
df = pd.read_csv(data_file)
df
# %% Transform df into a list of dicks 
df_dict = df.to_dict(orient='records')
df_dict[2]

# %% Instert file to DB
cars_coll = my_db["books_lends"]
cars_coll

doc = df_dict[0]
doc

# %% insert multiple docs from the df dick in one line/function
cars_coll.insert_many(df_dict)


# %%
cars_coll.insert_one(doc)




# %% eliminate all elements from cars_coll
cars_coll.delete_many({})
# %% delete cars_coll collection
cars_coll.drop()

# %% for cycle to insert one by one all the elelemnts  from the dataframe dick
for doc in df_dict:
    print(doc)
    cars_coll.insert_one(doc)

# %%
