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


client = MongoClient(
    data_db["ip_server"],
    data_db["port_server"],
    data_db["user"],
    data_db["password"],
    data_db["auth_db"],
)

# %%




