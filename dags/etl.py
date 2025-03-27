import yaml
import psycopg2 
from psycopg2 import sql
from sqlalchemy import create_engine, text
import pandas as pd
import json
import numpy as np
from decimal import Decimal
import os

def load_config():
    file_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)  #Convierte el YAML en un diccionario
    return config

def get_db_engine():
    config = load_config()
    print("DB CONFIG:", config)

    db_config = config.get("database", {})
    print("Extracted DB CONFIG:", db_config)

    return create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}")

def exportar_df(df, filename="predios_tranformed.csv"):
    path = "/opt/airflow/data"  # Ruta dentro del contenedor Docker
    os.makedirs(path, exist_ok=True)  # Asegura que la carpeta exista

    filepath = os.path.join(path, filename)
    df.to_csv(filepath, index=False)  # Guarda el CSV sin índice
    print(f"DataFrame exportado en: {filepath}")

def extract():
    """Proceso de extraccion de la data csv proyecto_predios"""
    engine = get_db_engine()
    df = pd.read_csv('./data/predios_dataset.csv', delimiter=";")

    config = load_config()
    db_config = config["database"]

    # Carga de credenciales
    db_user = db_config["user"]
    db_password = db_config["password"]
    db_host = db_config["host"]
    db_port = db_config["port"]
    db_name = db_config["name"]

    # DB connection
    conn = psycopg2.connect(
        dbname="postgres",
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    conn.autocommit = True

    #Crea un cursor para ejecutar sql query
    db_name = "proye_etl_db"
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            print(f"Base de datos '{db_name}' creada exitosamente.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"La base de datos '{db_name}' ya existe.")
    finally:
        conn.close()

    #Ejecuta sql query create table
    with engine.begin() as conn: 
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS base_proye (
                id SERIAL PRIMARY KEY,
                OBJETO_NUMERICO VARCHAR(100),
                TIPOPRED VARCHAR(50),
                AVALPRED_VIGANT BIGINT,
                USU_VIGANT VARCHAR(10),
                ACTIVIDAD_VIGANT VARCHAR(10),
                ESTRATO_VIGANT VARCHAR(10),
                AREA_VIGANT VARCHAR(10),
                TERRENO_VIGANT VARCHAR(10),
                PREDIAL_VIGANT BIGINT,
                COMUNA VARCHAR(10),
                BARRIO VARCHAR(10),
                MANZANA VARCHAR(10),
                TIPO_PREDIO VARCHAR(50),
                ACTUALIZACION VARCHAR(50),
                AVALPRED_VIGACT BIGINT,
                USU_VIGACT VARCHAR(10),
                ACTIVIDAD_VIGACT VARCHAR(10),
                ESTRATO_VIGACT VARCHAR(10),
                AREA_VIGACT VARCHAR(10),
                TERRENO_VIGACT VARCHAR(10),
                CARTERA_VIGACT CHAR
            );
        """))
        
    print("Tabla 'base_proye' creada exitosamente en PostgreSQL.")
    
    #Staging stage

    #Pre-transform
    df.columns= df.columns.str.lower()

    df['objeto_numerico'] = df['objeto_numerico'].astype(str)
    df['avalpred_vigant'] = df['avalpred_vigant'].astype('int64')
    df['avalpred_vigact'] = df['avalpred_vigact'].astype('int64')
    df['predial_vigant'] = df['predial_vigant'].astype('int64')

    #Insertar data
    with engine.connect() as conn:
        df.to_sql("base_proye", con=engine, if_exists="append", index=False)

    print("Data guardada en la tabla 'base_proye' exitosamente.")

def transform():
    """Proceso de tranformacion de la data proyecto_predios"""
    engine = get_db_engine()

    config = load_config()
    db_config = config["database"]

    # Carga de credenciales
    db_user = db_config["user"]
    db_password = db_config["password"]
    db_host = db_config["host"]
    db_port = db_config["port"]
    db_name = db_config["name"]

    # DB connection
    conn = psycopg2.connect(
        dbname="postgres",
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    conn.autocommit = True

    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    with engine.connect() as conn:
        proye_db_df = pd.read_sql("SELECT * FROM base_proye", conn)

    #Limpieza de datos

    #Borrar todos los registros nulos en la columnas indicadas
    proye_db_df = proye_db_df.dropna(subset=["objeto_numerico", "usu_vigant", "actividad_vigant", "avalpred_vigant", "predial_vigant", "avalpred_vigact"])

    #Borrar todos los registros repetidos en la columna indicada
    proye_db_df = proye_db_df.drop_duplicates(subset=['objeto_numerico'], keep='first')

    #Estandarización y manejo inicial de la data

    df_transformed = proye_db_df.copy() #Copia del dataframe proye_db_df para comienzo a la siguiente etapa

    df_transformed.rename(columns={'id': 'id_predio'}, inplace=True) #Renombramiento de la columna 'id' a 'id_predio' para reutilzarla

    #Eliminar la columna 'tipo_predio' porque no tiene relevancia en los calculos y puede genera confusion con la columna 'tipopred'
    df_transformed = df_transformed.drop(columns=['tipo_predio'])

    #Eliminar las columnas porque no tiene relevancia en los calculos.
    df_transformed = df_transformed.drop(columns=['area_vigant', 'terreno_vigant','area_vigact', 'terreno_vigact', 'manzana'])

    #Se agrega un 0 al inicio de cada objeto_numerico por el estandar que lo define (20 digitos).
    df_transformed['objeto_numerico'] = df_transformed['objeto_numerico'].apply(lambda x: '0' + x if len(x) == 19 else x)

    #Se agrega un 0 al inicio de los usu y actividad definiendo un estandar.
    df_transformed[['usu_vigant', 'actividad_vigant', 'usu_vigact', 'actividad_vigact']] = df_transformed[['usu_vigant', 'actividad_vigant', 'usu_vigact', 'actividad_vigact']].map(lambda x: '0' + x if len(x) == 1 else x)

    #Se concatenan el uso y la actividad de la vigencia actual.
    df_transformed['uso_actividad_vigact'] = df_transformed['usu_vigact'].str.cat(df_transformed['actividad_vigact'])

    #Redondear las columnas de los avaluos al multiplo 1000.
    df_transformed['avalpred_vigant'] = df_transformed['avalpred_vigant'].round(-3)
    df_transformed['avalpred_vigact'] = df_transformed['avalpred_vigact'].round(-3)

    #Convertir las columnas 'estrato_vigact' y 'estrato_vigant' a Int64 y luego remplazar los vacios por la letra 'N' convirtiendo nuevamente la columna a string
    df_transformed['estrato_vigact'] = pd.to_numeric(df_transformed['estrato_vigact'], errors='coerce').astype('Int64')
    df_transformed['estrato_vigact'] = df_transformed['estrato_vigact'].astype(str).replace("<NA>", "N")
    df_transformed['estrato_vigant'] = pd.to_numeric(df_transformed['estrato_vigant'], errors='coerce').astype('Int64')
    df_transformed['estrato_vigant'] = df_transformed['estrato_vigant'].astype(str).replace("<NA>", "N")

    #Agregar columna residencial o no residencial
    df_transformed['tipo_res'] = df_transformed.apply(lambda row: 'Residencial' if row['uso_actividad_vigact'] == "0101" else "No Residencial", axis=1)

    return df_transformed.to_json(orient="records")

def merge(**kwargs):
    """Proceso de merge de la data proyecto_predios y tarifa_predios"""
    ti = kwargs["ti"]
    df_json = ti.xcom_pull(task_ids="transform_task")
    df_transformed = pd.read_json(df_json, orient="records")
    print("Load: DataFrame cargado de transform_task")
    
    #Carga del archivo json tarifas
    with open("./data/tarifas_predios.json", "r") as file:
        tarifas_data = json.load(file, parse_float=Decimal)["tarifas_residencial_urbano_rural"] # Mantiene precisión en los valores con decimales

    # Convertir a DataFrame sin modificar num_tarifa para visualizar antes
    df_tarifas = pd.DataFrame(tarifas_data)

    # Convertir listas a strings para asegurar comparación correcta
    df_tarifas["uso_actividad_vigact"] = df_tarifas["uso_actividad_vigact"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else str(x)
    )
    df_tarifas["estrato_vigact"] = df_tarifas["estrato_vigact"].apply(
        lambda x: [str(e) for e in x] if isinstance(x, list) else [str(x)] if x else []
    )

    def asignar_tarifa(row):
        for _, tarifa in df_tarifas.iterrows():
                if str(row["uso_actividad_vigact"]) in tarifa["uso_actividad_vigact"]:
                    if str(row["estrato_vigact"]) in tarifa["estrato_vigact"]:
                        return pd.Series([tarifa["id_tarifa"], tarifa["num_tarifa"]]) 
        return pd.Series([None, None])  # Retorna None si no hay coincidencias

    # Asegurar que df_transformed tenga las columnas necesarias
    if "uso_actividad_vigact" not in df_transformed.columns or "estrato_vigact" not in df_transformed.columns:
        raise ValueError("df_transformed no tiene las columnas necesarias")
    
    # Aplicar la función al DataFrame
    df_transformed[["id_tarifa", "num_tarifa"]] = df_transformed.apply(asignar_tarifa, axis=1)
    print("Tarifa aplicada a df_transformed")

    df_merged = df_transformed.copy()

    #Renombramiento de la columna 'num_tarifa' a 'tarifa_vigact'
    df_merged.rename(columns={'num_tarifa': 'tarifa_vigact'}, inplace=True)

    # Quitar la "T" y convertir a int
    df_merged.loc[:, "id_tarifa"] = df_merged["id_tarifa"].str.replace("T", "", regex=False).astype(int)

    # Convertir a Decimal para mantener la diferencia en el formato
    df_merged["tarifa_vigact"] = df_merged["tarifa_vigact"].apply(lambda x: Decimal(x))

    SSMLV=192172500 #Salario Mínimo Mensual Legal Vigente actual

    #Funcion para asignar los indicadores segun las condiciones establecidas el Limite_3
    def calcular_indicador(row):
        if ((row['estrato_vigact'] == 1) or (row['estrato_vigact'] == 2)) and (row['avalpred_vigact'] <= SSMLV):
            return 5.20
        elif ((row['estrato_vigact'] == 1) or (row['estrato_vigact'] == 2)) and (row['avalpred_vigact'] > SSMLV) and (row['cartera_vigact'] == 'N'):
            return 13.20
        elif (row['estrato_vigact'] != 1 and row['estrato_vigact'] != 2) and (row['avalpred_vigact'] > 0) and (row['cartera_vigact'] == 'N'):
            return 13.20
        elif row['cartera_vigact'] == 'Y':
            return 0.0
        elif row['cartera_vigact'] == 'N':
            return 13.20
        else:
            return 0.0

    #Aplica la funcion calcular_indicador a las filas del df_transformed
    df_merged['indicador_precio'] = df_merged.apply(calcular_indicador, axis=1)

    #Calcular el Limite_1
    df_merged["limite_1"] = df_merged.apply(
        lambda row: (Decimal(row["avalpred_vigact"]) * row["tarifa_vigact"]).quantize(Decimal('1')),
        axis=1
    ).astype("Int64")

    #Calcular el Limite_2
    df_merged["limite_2"] = (df_merged["predial_vigant"] * 2).round(10)
    df_merged['limite_2'] = pd.to_numeric(df_merged['limite_2'], errors='coerce').astype('Int64')

    #Calcular el Limite_3
    df_merged['limite_3'] = df_merged.apply(lambda row: round(row['predial_vigant'] * (1 + row['indicador_precio'] / 100), -3) if row['indicador_precio'] > 0 else 0, axis=1)
    df_merged['limite_3'] = pd.to_numeric(df_merged['limite_3'], errors='coerce').astype('Int64')

    #Se toma el minimo entre los limites para escoger el impuesto_a_facturar
    df_merged.loc[(df_merged['limite_1'] > 0) & (df_merged['indicador_precio'] > 0), 'impuesto_a_facturar'] = df_merged[['limite_1', 'limite_2', 'limite_3']].min(axis=1)
    df_merged.loc[(df_merged['limite_1'] > 0) & (df_merged['indicador_precio'] == 0), 'impuesto_a_facturar'] = df_merged[['limite_1', 'limite_2']].min(axis=1)
    df_merged.loc[df_merged['limite_1'] == 0, 'impuesto_a_facturar'] = 0
    print("Impuesto_a_facturar calculado")

    df_merged['favo_lim_vigact'] = np.select(
        [
            (df_merged['impuesto_a_facturar'] == df_merged['limite_1']).to_numpy(dtype=bool),
            (df_merged['impuesto_a_facturar'] == df_merged['limite_2']).to_numpy(dtype=bool),
            (df_merged['impuesto_a_facturar'] == df_merged['limite_3']).to_numpy(dtype=bool)
        ],
        ['limite_1', 'limite_2', 'limite_3'],
        default='ninguno'
    )

    #Aplicar el descuento establecido a aplicar al impuesto_a_facturar
    df_merged['descuento'] = df_merged['impuesto_a_facturar'] * 0.15
    df_merged['descuento'] = pd.to_numeric(df_merged['descuento'], errors='coerce').astype('Int64')

    #Calcular valor_a_pagar
    df_merged['valor_a_pagar'] = df_merged['impuesto_a_facturar'] - df_merged['descuento']
    df_merged['valor_a_pagar'] = pd.to_numeric(df_merged['valor_a_pagar'], errors='coerce').astype('Int64')

    return df_merged.to_json(orient="records")


def load(**kwargs):
    """Proceso de load de la data final a la base de datos dimensional"""
    ti = kwargs["ti"]
    df_json = ti.xcom_pull(task_ids="merge_task")
    df_merged = pd.read_json(df_json, orient="records")
    print("Load: DataFrame cargado de merge_task")

    engine = get_db_engine()

    with engine.begin() as conn: 
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_predio (
            ID_PREDIO SERIAL PRIMARY KEY,
            OBJETO_NUMERICO VARCHAR(50),
            TIPOPRED VARCHAR(50),
            USU_VIGANT VARCHAR(50),
            ACTIVIDAD_VIGANT VARCHAR(50),
            ESTRATO_VIGANT VARCHAR(50),
            COMUNA VARCHAR(50),
            BARRIO VARCHAR(50),
            ACTUALIZACION VARCHAR(50),
            USU_VIGACT VARCHAR(50),
            ACTIVIDAD_VIGACT VARCHAR(50),
            ESTRATO_VIGACT VARCHAR(50),
            CARTERA_VIGACT VARCHAR(50),
            TIPO_RES VARCHAR(50)
            );
        """))
        
    print("Tabla 'dim_predio' creada exitosamente en PostgreSQL.")

    with engine.begin() as conn: 
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_tarifa (
            ID_TARIFA SERIAL PRIMARY KEY,
            TARIFA_VIGACT float
            );
        """))
        
    print("Tabla 'dim_tarifa' creada exitosamente en PostgreSQL.")

    with engine.begin() as conn: 
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_impuesto (
            ID_IMPUESTO SERIAL PRIMARY KEY,
            AVALPRED_VIGANT BIGINT,
            PREDIAL_VIGANT BIGINT,
            AVALPRED_VIGACT BIGINT,
            INDICADOR_PRECIO FLOAT,
            LIMITE_1 BIGINT,
            LIMITE_2 BIGINT,
            LIMITE_3 BIGINT,
            FAVO_LIM_VIGACT VARCHAR(50),
            IMPUESTO_A_FACTURAR BIGINT,
            DESCUENTO BIGINT,          
            VALOR_A_PAGAR BIGINT,
            ID_PREDIO INT REFERENCES dim_predio(ID_PREDIO) ON DELETE CASCADE,
            ID_TARIFA INT REFERENCES dim_tarifa(ID_TARIFA) ON DELETE SET NULL
            );
        """))
        
    print("Tabla 'fact_impuesto' creada exitosamente en PostgreSQL.")

    #Split dataframe df_merged a distintos dataframes para insertar en la bd_dimensional
    df_dim_tarifa = df_merged[['id_tarifa','tarifa_vigact']]
    df_dim_predio = df_merged[['id_predio',
    'objeto_numerico',
    'tipopred',
    'usu_vigant',
    'actividad_vigant',
    'estrato_vigant',
    'comuna',
    'barrio',
    'actualizacion',
    'usu_vigact',
    'actividad_vigact',
    'estrato_vigact',
    'cartera_vigact',
    'tipo_res']]
    df_fact_impuesto = df_merged[['avalpred_vigant',
    'predial_vigant',
    'avalpred_vigact',
    'indicador_precio',
    'limite_1',
    'limite_2',
    'limite_3',
    'impuesto_a_facturar',
    'favo_lim_vigact',
    'descuento',
    'valor_a_pagar',
    'id_predio',
    'id_tarifa']]

    # Ordenar de forma ascendente
    df_dim_tarifa = df_dim_tarifa.sort_values(by="id_tarifa", ascending=True).reset_index(drop=True)

    # Eliminar duplicados manteniendo el primer valor encontrado
    df_dim_tarifa = df_dim_tarifa.drop_duplicates(subset=['id_tarifa'], keep='first')

    #Insertar datos dim_predio
    with engine.connect() as conn:
        df_dim_predio.to_sql("dim_predio", con=engine, if_exists="append", index=False)

    print("Data guardada en la tabla 'dim_predio' exitosamente.")

    #Insertar datos dim_tarifa
    with engine.connect() as conn:
        df_dim_tarifa.to_sql("dim_tarifa", con=engine, if_exists="append", index=False)

    print("Data guardada en la tabla 'dim_tarifa' exitosamente.")

    #Insertar datos fact_impuesto
    with engine.connect() as conn:
        df_fact_impuesto.to_sql("fact_impuesto", con=engine, if_exists="append", index=False)

    print("Data guardada en la tabla 'fact_impuesto' exitosamente.")