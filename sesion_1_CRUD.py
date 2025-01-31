## Sesion 1
# %%
import pymongo
import yaml
from pathlib import Path
from pymongo import MongoClient

# %%
key_file = "credenciales_mongodb.yaml"
with open(key_file,"r") as io_file:
    data_db = yaml.safe_load(io_file)
data_db
# %% Conectarnos al servidor de MongoDB

# Información necesaria para conectarnos al servidor:
# IP
# Puerto
# Usuario
# Password
# Base de datos de autenticación

server = MongoClient(
    data_db["ip_server"],
    data_db["port_server"],
    username=data_db["user"],
    password=data_db["password"],
    authSource=data_db["auth_db"],
)

# %%
server
# %%
dbs = list(server.list_databases())
dbs[0]
# %%
data_base = server["estDB217273256"]
data_base
# %%
colls = list(data_base.list_collections()) 
colls
# %% coleccion
colection = data_base[colls[0]["name"]]
colection
# %% CRUD un doc
single_docs = colection.find_one()
single_docs
# %% CRUD various docs
mult_docs =  list(colection.find())
mult_docs


# %% insert new doc

new_doc = {"x":22,"y":13}

# %%
colection.insert_one(new_doc)
# %%
list(colection.find())
# %%
new_docs = [{"x":69,"y":12},
            {"x":72,"y":9}]

# %%
colection.insert_many(new_docs)
#%%
list(colection.find())
# %%
#data_base.colection.update_one({"y":13},{$set:{"x":69}})
# %%
