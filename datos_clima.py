import pandas as pd
from sqlalchemy import create_engine, text
import os

DB_USER = "postgres"
DB_PASS = "123450"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "clima_agro"


CSV_FILE_PATH = "datos_guayas.csv"
TABLE_NAME = "lecturas"

SKIP_ROWS = 10

MISSING_DATA_VALUE = -999



# 2. DEFINICIÓN DE LA ESTRUCTURA DE LA TABLA (SQL)

SQL_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    YEAR INT,
    DOY INT,
    T2M NUMERIC,     -- Temperatura a 2 Metros (C)
    RH2M NUMERIC     -- Humedad Relativa a 2 Metros (%)
);
"""

# 3. LÓGICA DE IMPORTACIÓN Y TRANSFORMACIÓN

def importar_csv_a_postgres():
    """Lee el CSV, limpia los datos y los guarda en PostgreSQL."""
    print(f"Iniciando la importación a la tabla: {TABLE_NAME}")

    try:

        db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)
        print("Conexión con PostgreSQL establecida.")
    except Exception as e:
        print(f"ERROR al conectar con PostgreSQL. ¿Están tus credenciales correctas? Detalle: {e}")
        return


    try:
        with engine.connect() as connection:
            connection.execute(text(SQL_CREATE_TABLE))
            connection.commit()
        print(f"Tabla '{TABLE_NAME}' verificada/creada en '{DB_NAME}'.")
    except Exception as e:
        print(f"ERROR al crear la tabla: {e}")
        return


    try:
        df = pd.read_csv(
            CSV_FILE_PATH,
            skiprows=SKIP_ROWS,
            na_values=[MISSING_DATA_VALUE]
        )

        df.columns = [col.strip() for col in df.columns]


        df.columns = [col.lower() for col in df.columns]



        print(f"CSV leído y limpiado. Filas para importar: {len(df)}")

    except FileNotFoundError:
        print(f"ERROR: El archivo CSV no se encontró. Asegúrate de que '{CSV_FILE_PATH}' esté en la misma carpeta que el script.")
        return
    except Exception as e:
        print(f"ERROR al leer o procesar el CSV: {e}")
        return


    try:

        df.to_sql(
            TABLE_NAME,
            engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )
        print("\n=======================================================")
        print(f" ¡ÉXITO! {len(df)} filas importadas a la tabla '{TABLE_NAME}'.")
        print("=======================================================")

    except Exception as e:
        print(f"\n ERROR al insertar datos en la base de datos: {e}")


if __name__ == "__main__":
    importar_csv_a_postgres()