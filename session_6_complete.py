## Sesion 6
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
    data_db["ip_servidor"],
    data_db["puerto_servidor"],
    username=data_db["usuario"],
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

# %%

data_path = Path("..", "datasets")
data_path

# %%

df_artists = pd.read_csv(Path(data_path, "musical_collabs", "artists.csv"))
df_collabs = pd.read_csv(Path(data_path, "musical_collabs", "collaborations.csv"))

# %%

artists_collection = my_db["artists"]
artists_collection.insert_many(df_artists.to_dict(orient="records"))

# %%

collaborations_collection = my_db["collaborations"]
collaborations_collection.insert_many(df_collabs.to_dict(orient="records"))

# %%

artists_collection.find_one()

# %%

collaborations_collection.find_one()

# %% Vincular colecciones artists y collaborations
# 

# Buscar las invitaciones que cada artista HACE a otros artistas
relacion = artists_collection.aggregate([
  {
      "$lookup": {
          "from": "collaborations",
          "localField": "artist",
          "foreignField": "artist1",
          "as": "resultado_lookup"
      }
  },
   {
      "$project": {
        #   "artist": 1,
          "artista_inicial": "$artist",
        #   "collab_songs": 1,
          "total_canciones": "$collab_songs",
          "total_colaboraciones": "$collab_individuals",
          "otros_artistas": "$resultado_lookup.artist2",
      }
  }
])

result = list(relacion)
pp.pprint(result[3])    

print(len(result[3]["otros_artistas"]))

# %%

query = {"artist": {"$eq": "Arcángel"}}

datos = {"artist": 1, "collab_songs": 1}
# datos = {}

cursor = artists_collection.find(query, datos)
resultado = list(cursor)
resultado

# %%


# Buscar las invitaciones que cada artista RECIBE de otros artistas
relacion = artists_collection.aggregate([
  {
      "$lookup": {
          "from": "collaborations",
          "localField": "artist",
          "foreignField": "artist2",
          "as": "resultado_lookup"
      }
  },
#    {
#       "$project": {
#           "artista_inicial": "$artist",
#           "otros_artistas": "$resultado_lookup.artist2",
#       }
#   }
])

result = list(relacion)
pp.pprint(result[0])  

# %%

df_artists = pd.read_csv(Path(data_path, "musical_collabs", "artists.csv"))
df_collabs = pd.read_csv(Path(data_path, "musical_collabs", "collaborations.csv"))