# %%
# ## Ejercicios de operaciones CRUD en MongoDB

# Comenzamos por importar algunos paquetes que utilizaremos
import yaml
from pathlib import Path

# pymongo es el driver de Python para ineractuar con el servidor
from pymongo import MongoClient
from pymongo import errors
import calendar  # Import the calendar modu
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
    data_db["ip_server"],
    data_db["port_server"],
    username=data_db["user"],
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

db_name = "estDB217273256"
data_db = client[db_name]

colls_in_db = data_db.list_collection_names()
print(colls_in_db)

# Vamos a usar esta colección, que previamente ya subimos a la base de datos
coll_name = "hotels_dataset"
data_coll = data_db[coll_name]

# %% 
# Obtenemos un documento de prueba para verificar la estructura

test_doc = data_coll.find_one()
pp.pprint(test_doc)

# %%
# ## Ejercicios:

# %% 
# ### 1: Obtenga todas las reservaciones canceladas
query = {"booking_status": {"$eq": "Canceled"}}

atributos = {"Booking_ID":1,"booking_status":1}

cursor = data_coll.find(query,atributos)

resultados = list(cursor)
resultados

count = len(resultados)

print(f'En total existen {count} cancelaciones en esta base de datos')
print("")

for r in resultados:
    print(f'{r["Booking_ID"]} | {r["booking_status"]}')

# ### 2: Calcule las cancelaciones por mes (sin considerar año)
#%%
query = {"booking_status": {"$eq": "Canceled"}}
atributos = {"Booking_ID": 1, "booking_status": 1, "arrival_month": 1}

# Assuming data_coll is a pymongo collection object
cursor = data_coll.find(query, atributos)
df = pd.DataFrame(list(cursor))

# Handle potential missing or invalid 'arrival_month' values more efficiently
df = df[df['arrival_month'].between(1, 12, inclusive='both')].copy() #Filter out invalid month numbers and create a copy to avoid SettingWithCopyWarning

# Convert 'arrival_month' to month names
df['arrival_month_name'] = df['arrival_month'].apply(lambda x: calendar.month_name[x])

# Calculate monthly cancellation counts
monthly_counts = df['arrival_month_name'].value_counts().sort_index().to_dict() #Sorts the months in order

total_count = len(df)


print(f'En total existen {total_count} cancelaciones en esta base de datos')
print("")

print("\nMonthly Cancellation Counts:")
for month_name, count in monthly_counts.items():
    print(f"{month_name}: {count}")
#%%
# ### 3: Identifique las reservaciones activas

query = {"booking_status": {"$eq": "Not_Canceled"}}

atributos = {"Booking_ID":1,"booking_status":1}

cursor = data_coll.find(query,atributos)

resultados = list(cursor)
resultados

count = len(resultados)

print(f'En total existen {count} activas en esta base de datos')
print("")

for r in resultados:
    print(f'{r["Booking_ID"]} | {r["booking_status"]}')


#%%
# ### 4: Calcule las reservaciones mensuales (sin considerar año)
query = {"booking_status": {"$eq": "Not_Canceled"}}
atributos = {"Booking_ID": 1, "booking_status": 1, "arrival_month": 1}

# Assuming data_coll is a pymongo collection object
cursor = data_coll.find(query, atributos)
df = pd.DataFrame(list(cursor))

# Handle potential missing or invalid 'arrival_month' values more efficiently
df = df[df['arrival_month'].between(1, 12, inclusive='both')].copy() #Filter out invalid month numbers and create a copy to avoid SettingWithCopyWarning

# Convert 'arrival_month' to month names
df['arrival_month_name'] = df['arrival_month'].apply(lambda x: calendar.month_name[x])

# Calculate monthly cancellation counts
monthly_counts = df['arrival_month_name'].value_counts().sort_index().to_dict() #Sorts the months in order

total_count = len(df)

print(f'En total existen {total_count} reservaciones activas en esta base de datos')
print("")

print("\nMonthly Active Counts:")
for month_name, count in monthly_counts.items():
    print(f"{month_name}: {count}")
#%%
# ### 5: Identifique los meses con reservaciones con niños

query = {"booking_status": {"$eq": "Not_Canceled"},"no_of_children": {"$gt": 0}}
atributos = {"Booking_ID": 1, "booking_status": 1, "arrival_month": 1}

# Assuming data_coll is a pymongo collection object
cursor = data_coll.find(query, atributos)
df = pd.DataFrame(list(cursor))

# Handle potential missing or invalid 'arrival_month' values more efficiently
df = df[df['arrival_month'].between(1, 12, inclusive='both')].copy() #Filter out invalid month numbers and create a copy to avoid SettingWithCopyWarning

# Convert 'arrival_month' to month names
df['arrival_month_name'] = df['arrival_month'].apply(lambda x: calendar.month_name[x])

# Calculate monthly cancellation counts
monthly_counts = df['arrival_month_name'].value_counts().sort_index().to_dict() #Sorts the months in order

total_count = len(df)

print(f'En total existen {total_count} reservaciones activas con ninnos en esta base de datos')
print("")

print("\nMonthly Active Counts with children:")
for month_name, count in monthly_counts.items():
    print(f"{month_name}: {count}")

#%%
# ### 6: las reservaciones especiales están relacionadas con niños?

query = {"booking_status": {"$eq": "Not_Canceled"}, "no_of_children": {"$gt": 0}}
atributos = {"Booking_ID": 1, "booking_status": 1, "arrival_month": 1, "no_of_special_requests": 1, "no_of_children": 1}
cursor = data_coll.find(query, atributos)
df = pd.DataFrame(list(cursor))

# Handle potential missing or invalid 'arrival_month' values
df = df[df['arrival_month'].between(1, 12, inclusive='both')].copy()

# Month calculation (This was missing and caused the NameError)
df['arrival_month_name'] = df['arrival_month'].apply(lambda x: calendar.month_name[x])
monthly_counts = df['arrival_month_name'].value_counts().sort_index().to_dict()

total_count = len(df)

print(f'En total existen {total_count} reservaciones activas con ninnos en esta base de datos')
print("")

print("\nMonthly Active Counts with children:")
for month_name, count in monthly_counts.items():
    print(f"{month_name}: {count}")


# Analyze the relationship between special requests and children
if 'no_of_special_requests' in df.columns:
    if pd.api.types.is_numeric_dtype(df['no_of_special_requests']):
        correlation = df['no_of_special_requests'].corr(df['no_of_children'])
        print(f"\nCorrelation between special requests and children: {correlation}")

        print("\nDescriptive statistics for special requests:")
        print(df['no_of_special_requests'].describe())

        print("\nAverage number of children by number of special requests:")
        print(df.groupby('no_of_special_requests')['no_of_children'].mean())

    else:
        print("\n'no_of_special_requests' column is not numeric. Cannot calculate correlation.")
        print("Consider converting it to numeric if possible for correlation analysis.")
else:
    print("\n'no_of_special_requests' column not found in the data.")

#%%
# ### 7: en que segmento de mercado se cancelan más reservaciones?



#%%
# ### 8: se cancelan más reservaciones al inicio o al fin del mes?
#%%
# ### 9: identifique las reservaciones con niños o con pedidos especiales
#%%
# ### 10: como identificaría a las reservaciones asociadas a clientes VIP?
#%%
# %%
