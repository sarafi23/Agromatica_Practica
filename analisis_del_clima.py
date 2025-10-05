import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

db_user = 'postgres'
db_password = '123450'
db_host = 'localhost'
db_port = '5432'
db_name = 'clima_agro'

db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)


sql_query = "SELECT * FROM lecturas ORDER BY year, doy;"
df = pd.read_sql(sql_query, engine)

print("¡Datos cargados exitosamente en un DataFrame de Pandas!")
print("\n--- Primeros 5 Registros (df.head()) ---")
print(df.head())

print("\n--- Estadísticas Descriptivas (df.describe()) ---")
print(df[['t2m', 'rh2m']].describe())


fig, ax1 = plt.subplots(figsize=(12, 6))


fig.suptitle('Tendencias Diarias de Temperatura y Humedad', fontsize=16)

color_temp = 'tab:red'
ax1.set_xlabel('Día del Año (DOY)')
ax1.set_ylabel('Temperatura a 2m (°C)', color=color_temp)
ax1.plot(df['doy'], df['t2m'], color=color_temp, label='T2M (°C)')
ax1.tick_params(axis='y', labelcolor=color_temp)
ax1.grid(axis='y', linestyle='--', alpha=0.7) # Añadir rejilla para T2M


ax2 = ax1.twinx()
color_hum = 'tab:blue'
ax2.set_ylabel('Humedad Relativa a 2m (%)', color=color_hum)
ax2.plot(df['doy'], df['rh2m'], color=color_hum, linestyle='--', label='RH2M (%)')
ax2.tick_params(axis='y', labelcolor=color_hum)

fig.tight_layout()
plt.show()

max_humedad = df['rh2m'].max()
dias_mayor_humedad = df[df['rh2m'] == max_humedad]

print(f"\n--- Días con Mayor Humedad ({max_humedad:.2f}%) ---")
print(dias_mayor_humedad[['doy', 't2m', 'rh2m']])

correlacion = df['t2m'].corr(df['rh2m'])
print(f"\nCoeficiente de Correlación (T2M vs. RH2M): {correlacion:.4f}")












