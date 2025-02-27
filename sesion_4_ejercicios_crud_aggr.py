## Sesion 4
## Agregación de datos en MongoDB

# %%

import yaml
import pprint
from pymongo import MongoClient

from pathlib import Path
import pandas as pd
import statsmodels.formula.api as sm

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
my_coll = my_db["hotels_dataset"]

my_coll

# %% Para probar, extraemos un documento de la colección

test_doc = my_coll.find_one()
pp.pprint(test_doc)

# %%
## 1: Identifique las reservaciones activas y modifique su estado a "Arrived"
query = {"booking_status": {"$eq": "Not_Canceled"}}

cursor = my_coll.find(query)

resultado = list(cursor)
df = pd.DataFrame(resultado)

df

# replace

# %% Identificar las reservaciones "Not_Canceled" de curtos "Room_Type 4" y cambiar
# su estatus a "Arrived"

query = {"$and": [
        {"booking_status": {"$eq": "Not_Canceled"}},
        {"room_type_reserved": {"$eq": "Room_Type 4"}}
    ]
}

cursor = my_coll.find(query)

resultado = list(cursor)
df = pd.DataFrame(resultado)

df
# %%

# query = {"$and": [
#         {"booking_status": {"$eq": "Not_Canceled"}},
#         {"room_type_reserved": {"$eq": "Room_Type 4"}}
#     ]
# }

query = {"Booking_ID": {"$eq": "INN00001"}}

# cursor = my_coll.find_one(query)

my_coll.replace_one(query, {"booking_status": "Arrived"})

cursor = my_coll.find_one(query)
cursor

# %%

my_coll.update_many(
     query,
    #  {"$set": {"booking_status": "Arrived"}}
     {"$set": {"booking_status": "Not_Canceled"}}
)


# %% update many

# Sintaxis: my_coll.update_many({query}, {actualización})

#my_coll.update_many(
#     {"booking_status": {"$eq": "Not_Canceled"}},
#      {"$set": {"booking_status": "Arrived"}}
# )

my_coll.update_many(
    {"booking_status": {"$eq": "Arrived"}},
     {"$set": {"booking_status": "Not_Canceled"}}
)


# %%

#query = {"booking_status": {"$eq": "Arrived"}}
query = {"booking_status": {"$eq": "Not_Canceled"}}

cursor = my_coll.find(query)

resultado = list(cursor)
df = pd.DataFrame(resultado)

df

# %%
## 2: Identifique las reservaciones activas entre junio y agosto
### y agrege el campo "season" con el valor "summer"

# respuesta 2
query = {'$and':
            [{'arrival_month': {'$gte': 6}},
            {'arrival_month': {'$lte': 8}},
            {'booking_status': {'$eq': 'Not_Canceled'}}
            ]
        }

query = {
    "$and": [
        {
            "$and": [
                {"arrival_month": {"$gte": 6}},
                {"arrival_month": {"$lte": 8}},
            ]
        },
        {"booking_status": {"$eq": "Not_Canceled"}},
    ]
}


update = {
     '$set' : {'season' : 'summer'}
}
my_coll.update_many(
     query, update
)

cursor = my_coll.find(query)
resultado = list(cursor)
df = pd.DataFrame(resultado)
df


# %%
## 3: Identifique las reservaciones activas entre  diciembre y marzo
## y agregue el campo "season" con el valor "winter"
#1ra respuesta
pipeline = [{"$match":{"booking_status":"Not_Canceled","arrival_month": {"$in":[12,1,2,3]}}},
            {"$addFields":{"season":"winter"}},
]

#%% 2da respuesta
query = {'$and':[
    {'$or':
         [{'arrival_month':{'$eq':12}},
          {'arrival_month':{'$lte':3}}
         ]},
          {'booking_status':{'$eq':"Not_Canceled"}}
]}

update = {
     '$set' : {'season' : 'winter'}
}
my_coll.update_many(
     query, update
)


cursor = my_coll.find(query)
resultado = list(cursor)
df = pd.DataFrame(resultado)
df

# %%
## 4: Identifique las reservaciones con más week_nights que weekend_nights
## y agregue el campo "guest_type" con el valor "work"

# Query to identify reservations with more week_nights than weekend_nights
query = {
    "$expr": {
        "$gt": ["$no_of_week_nights", "$no_of_weekend_nights"]
    }
}

# Update to add the "guest_type" field
update = {
    "$set": {"guest_type": "work"}
}

# Update matching documents
my_coll.update_many(query, update)

# Optional: Fetch the updated documents and create a DataFrame for display
#updated_query = {"guest_type": "work"}
cursor = my_coll.find(query)
resultado = list(cursor)
df = pd.DataFrame(resultado)
df

# %%
## 5: Identifique las reservaciones con más weekend_nights que week_nights
## y agregue el campo "guest_type" con el valor "leisure"

query = {
    "$expr": {
        "$gt": ["$no_of_weekend_nights", "$no_of_week_nights"]
    }
}

# Update to add the "guest_type" field
update = {
    "$set": {"guest_type": "leisure"}
}

# Update matching documents
my_coll.update_many(query, update)

# Optional: Fetch the updated documents and create a DataFrame for display
#updated_query = {"guest_type": "work"}
cursor = my_coll.find(query)
resultado = list(cursor)
df = pd.DataFrame(resultado)
df

# %%
## 6.1: Calcule el total de ganancias por año

pipeline = [
    {
        "$match": {
            "booking_status": "Not_Canceled"
        }
    },
    {
        "$group": {
            "_id": "$arrival_year",
            "total_earnings": {"$sum": "$avg_price_per_room"},
        }
    },
    {
        "$project": {
            "_id": 0,
            "year": "$_id",
            "total_earnings": 1,
        }
    },
    {
        "$sort": {"year": 1},
    },
]

resultado = list(my_coll.aggregate(pipeline))
df = pd.DataFrame(resultado)
df

# %%
## 6.2: Calcule el total de ganancias por año por mes

pipeline = [    
    {
        "$match": {
            "booking_status": "Not_Canceled"
        }
    },    
    {
        "$group": {
            "_id": {"year": "$arrival_year", "month": "$arrival_month"},
            "total_earnings": {"$sum": "$avg_price_per_room"},
        }
    },
    {
        "$project": {
            "_id": 0,
            "year": "$_id.year",
            "month": "$_id.month",
            "total_earnings": 1,
        }
    },
        {
        "$sort": {"year": 1, "month":1},
    },

]

resultado = list(my_coll.aggregate(pipeline))
df = pd.DataFrame(resultado)
df

# %%
## 7.1: Calcule el total de perdidas por año

pipeline = [
    {
        "$match": {
            "booking_status": "Canceled"
        }
    },
    {
        "$group": {
            "_id": "$arrival_year",
            "total_earnings": {"$sum": "$avg_price_per_room"},
        }
    },
    {
        "$project": {
            "_id": 0,
            "year": "$_id",
            "total_earnings": 1,
        }
    },
    {
        "$sort": {"year": 1},
    },
]

resultado = list(my_coll.aggregate(pipeline))
df = pd.DataFrame(resultado)
df

# %%
## 7.2: Calcule el total de perdidas por año por mes

pipeline = [    
    {
        "$match": {
            "booking_status": "Canceled"
        }
    },    
    {
        "$group": {
            "_id": {"year": "$arrival_year", "month": "$arrival_month"},
            "total_earnings": {"$sum": "$avg_price_per_room"},
        }
    },
    {
        "$project": {
            "_id": 0,
            "year": "$_id.year",
            "month": "$_id.month",
            "total_earnings": 1,
        }
    },
        {
        "$sort": {"year": 1, "month":1},
    },

]

resultado = list(my_coll.aggregate(pipeline))
df = pd.DataFrame(resultado)
df

#%% change to cars dataset


dbs = list(servidor.list_databases())

print(dbs)

my_db = servidor[dbs[0]["name"]]
my_db

# %% Seleccionamos la colección con la que vamos a trabajar

my_db.list_collection_names()

# %% Seleccionamos la colección con la que vamos a trabajar

my_coll = my_db["cars_dataset"]
#my_coll = my_db["hotels_dataset"]

my_coll

# %% Para probar, extraemos un documento de la colección

test_doc = my_coll.find_one()
pp.pprint(test_doc)


# %% Datos de coches
## 8: Identifique las marcas unicas

unique_makes = my_coll.distinct("Make")

print(unique_makes)


# %%
## 9: Calcule el precio promedio por marca

pipeline = [
    {
        "$group": {
            "_id": "$Make",
            "average_price": {"$avg": "$Price"},
        }
    },
    {
        "$project": {
            "_id": 0,
            "Make": "$_id",
            "average_price": 1,
        }
    },
]

resultado = list(my_coll.aggregate(pipeline))
df = pd.DataFrame(resultado)
df

# %%
## 10: Calcule el precio promedio por marca por año

pipeline = [
    {
        "$group": {
            "_id": {"Make": "$Make", "Year": "$Year"},
            "average_price": {"$avg": "$Price"},
        },
    },
    {
        "$project": {
            "_id": 0,
            "Make": "$_id.Make",
            "Year": "$_id.Year",
            "average_price": 1,
        },
    },
    {
        "$sort": {"Make": 1, "Year": 1},
    },
]

resultado = list(my_coll.aggregate(pipeline))
df = pd.DataFrame(resultado)
df

# %%
## 11: Identifique los campos únicos de "Owner"

unique_owner = my_coll.distinct("Owner")

print(unique_owner)

# %%
## 12: Determine el efecto en el precio de los autos de los campos
## Owner, Year y Kilometer


# Fetch the data into a Pandas DataFrame
# Fetch the data into a Pandas DataFrame
data = pd.DataFrame(list(my_coll.find({}, {"Owner": 1, "Year": 1, "Kilometer": 1, "Price": 1})))

# Map 'Owner' values to numerical values
owner_mapping = {
    'First': 1,
    'Second': 2,
    'Third': 3,
    'Fourth': 4,
    '4 or More': 5,
    'UnRegistered Car': 0,
}

data['Owner_Numerical'] = data['Owner'].map(owner_mapping)
data = data[data['Owner_Numerical'] != 0]

# Drop original owner column
#data.drop('Owner', axis=1, inplace=True)

# Define the regression model
model = sm.ols('Price ~ Owner_Numerical + Year + Kilometer', data=data)

# Fit the model
results = model.fit()

# Print the regression results
print(results.summary())
# %%
data
# %%
