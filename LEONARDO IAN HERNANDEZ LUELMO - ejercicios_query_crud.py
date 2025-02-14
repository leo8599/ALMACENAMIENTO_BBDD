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
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

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
#%%
# ### 2: Calcule las cancelaciones por mes (sin considerar año)

atributo = "arrival_month"

pipeline = [
    {"$match": {"booking_status": "Canceled"}},
    {"$group": {"_id": f"${atributo}", "Count": {"$sum": 1}}},
    {"$sort" : {"Count":-1}}
]

cursor = data_coll.aggregate(pipeline)
res = list(cursor)
res_df = pd.DataFrame(res)
res_df

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

atributo = "arrival_month"

pipeline = [
    {"$match": {}},
    {"$group": {"_id": f"${atributo}", "Total reservaciones mensuales": {"$sum": 1}}},
    {"$sort" : {"Total reservaciones mensuales":-1}}
]

cursor = data_coll.aggregate(pipeline)
res = list(cursor)
res_df = pd.DataFrame(res)
res_df

#%%
# ### 5: Identifique los meses con reservaciones con niños

atributo = "arrival_month"

pipeline = [
    {"$match": {"no_of_children": {"$gt":0}}},
    {"$group": {"_id": f"${atributo}", "Total Niños": {"$sum": 1}}},
    {"$sort" : {"Total Niños":-1}}
]

cursor = data_coll.aggregate(pipeline)
res = list(cursor)
res_df = pd.DataFrame(res)
res_df

#%%
# ### 6: las reservaciones especiales están relacionadas con niños?

#query = {"booking_status": {"$eq": "Not_Canceled"}}
query = {}
atributos = {"required_car_parking_space": 1, "repeated_guest": 1, "no_of_weekend_nights": 1, "no_of_special_requests": 1, "no_of_children": 1, "no_of_previous_bookings_not_canceled": 1, "no_of_previous_cancellations": 1, "no_of_week_nights": 1, "no_of_adults": 1, "lead_time": 1, "avg_price_per_room": 1}
cursor = data_coll.find(query, atributos)
df = pd.DataFrame(list(cursor))

def plot_correlation_matrix(df, method='pearson', title_suffix='', show_values=True, figsize=(10, 8)):
    """
    Calculates and plots the correlation matrix of a Pandas DataFrame.

    Args:
        df: The Pandas DataFrame.
        method: The correlation method ('pearson', 'kendall', 'spearman').
                Defaults to 'pearson'.
        title_suffix: Optional suffix to add to the plot title.
        show_values: Whether to display correlation coefficients on the heatmap.
        figsize: Figure size as a tuple (width, height).

    Returns:
        None (displays the plot).  Returns the matplotlib Axes object.
    """

    if method not in ['pearson', 'kendall', 'spearman']:
        raise ValueError("Invalid correlation method. Choose 'pearson', 'kendall', or 'spearman'.")

    correlation_matrix = df.corr(method=method, numeric_only=True)

    plt.figure(figsize=figsize)  # Set the figure size
    sns.set(style="white")  # Set a clean style for the plot

    # Generate a mask for the upper triangle (optional, but makes the plot cleaner)
    mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))

    # Create the heatmap using seaborn
    heatmap = sns.heatmap(
        correlation_matrix,
        mask=mask,            # Hide the upper triangle
        cmap="RdBu",      # Choose a colormap (coolwarm, RdBu, etc.)
        vmin=-1, vmax=1,      # Set the color scale limits
        annot=show_values,     # Show the correlation values on the plot (optional)
        fmt=".2f",           # Format the annotation values (2 decimal places)
        linewidths=.5,         # Add lines between cells
        cbar_kws={"shrink": .75},  # Shrink the colorbar
        square=True           # Make cells square
    )

    # Add a title
    plt.title(f'Correlation Matrix ({method}){title_suffix}', fontsize=16)

    # Rotate x-axis labels for better readability (optional, but often necessary)
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)

    # Improve layout (avoid labels overlapping)
    plt.tight_layout()

    plt.show()

    return heatmap.axes  # Return the axes for further customization if needed

plot_correlation_matrix(df)  # Pearson by default
#plot_correlation_matrix(df_clean, method='spearman', title_suffix=' (Spearman)', show_values=False, figsize=(8, 6))
#plot_correlation_matrix(df_clean, method='kendall', title_suffix= ' using Kendall')

correlation = df['no_of_special_requests'].corr(df['no_of_children'])

print(f"\nCorrelation between special requests and children: {correlation}")
print(f"\nTherefore, there is a slight correlation (not near 1 but higher than any other corr) between special reservations and reservations with children, in addition to avg room price and but also number of adults, which is a stronger correlation, what can suggests that special reservations could include children but also adults.")


#%%
# ### 7: en que segmento de mercado se cancelan más reservaciones?

atributo = "market_segment_type"

pipeline = [
    {"$match": {"booking_status": "Canceled"}},
    {"$group": {"_id": f"${atributo}", "Count": {"$sum": 1}}},
    {"$sort" : {"Count":-1}}
]

cursor = data_coll.aggregate(pipeline)
res = list(cursor)
res_df = pd.DataFrame(res)
res_df

#%%
# ### 8: se cancelan más reservaciones al inicio o al fin del mes?

atributo = "arrival_date"

pipeline = [
    {"$match": {"booking_status": "Canceled"}},
    {"$group": {"_id": f"${atributo}", "Count": {"$sum": 1}}},
    {"$sort" : {"_id":1}}
]

cursor = data_coll.aggregate(pipeline)
res = list(cursor)
res_df = pd.DataFrame(res)
res_df


#%%
# ### 9: identifique las reservaciones con niños o con pedidos especiales
#%%
# ### 10: como identificaría a las reservaciones asociadas a clientes VIP?
#%% Extra clase

agregador = "room_type_reserved"
value = "avg_price_per_room"

pipeline = [
    {"$match": {"arrival_year": {"$eq":2018}},
     "$match": {"booking_status": {"$eq":"Not_Canceled"}}},
    {"$group": {"_id": f"${atributo}", "total_income": {"$sum": f"${value}"}}},
    {"$sort" : {"total_income":-1}}
]

cursor = data_coll.aggregate(pipeline)
res = list(cursor)
res_df = pd.DataFrame(res)
res_df

# %%


