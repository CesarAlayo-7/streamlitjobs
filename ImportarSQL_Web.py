import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text, event
import urllib
import io
import traceback

# ======================================================
# 🔧 UTILIDADES
# ======================================================
def normalizar_col(col):
    return str(col).strip().lower().replace(" ", "_").replace("#", ".")


def mapear_columnas(df_cols, columnas_sql):
    sql_map = {normalizar_col(c): c for c in columnas_sql}
    return {
        c: sql_map[normalizar_col(c)]
        for c in df_cols
        if normalizar_col(c) in sql_map
    }


def bulk_insert_fast(df, table, schema, engine):
    columnas = ",".join(f"[{c}]" for c in df.columns)
    params = ",".join("?" for _ in df.columns)

    sql = f"INSERT INTO [{schema}].[{table}] ({columnas}) VALUES ({params})"
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    with engine.begin() as conn:
        cursor = conn.connection.cursor()
        cursor.fast_executemany = True
        cursor.executemany(sql, data)


# ======================================================
# 🔗 CONEXIÓN SQL SERVER (DINÁMICA POR USUARIO)
# ======================================================
def crear_engine(servidor, database, username, password):
    driver = "ODBC Driver 17 for SQL Server"

    params = urllib.parse.quote_plus(
        f"DRIVER={driver};"
        f"SERVER={servidor};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    @event.listens_for(engine, "before_cursor_execute")
    def enable_fast_executemany(conn, cursor, statement, parameters, context, executemany):
        if executemany and hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True

    return engine


# ======================================================
# 🚀 UI CONFIG
# ======================================================
st.set_page_config("Importador Excel → SQL", "📊", layout="wide")
st.title("📊 Importador Excel → SQL Server")

# ======================================================
# 🔐 0️⃣ CONEXIÓN A SQL SERVER
# ======================================================
st.subheader("🔐 Conexión a SQL Server")

with st.form("conexion_sql"):
    servidor = st.text_input("Servidor", placeholder="CSPLCDB02\\QADEV")
    database = st.text_input("Base de datos", placeholder="ReportesCdg_Temporales")
    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")

    conectar = st.form_submit_button("Conectar")

if conectar:
    try:
        engine = crear_engine(servidor, database, username, password)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        st.session_state["engine"] = engine
        st.success("✅ Conexión exitosa")

    except Exception as e:
        st.error("❌ Error de conexión")
        st.text(e)
        st.stop()

if "engine" not in st.session_state:
    st.info("🔐 Ingresa las credenciales para continuar")
    st.stop()

engine = st.session_state["engine"]
inspector = inspect(engine)

# ======================================================
# 1️⃣ SELECCIÓN DE TABLA DESTINO
# ======================================================
st.subheader("1️⃣ Tabla destino")

schemas = inspector.get_schema_names()
schema_sel = st.selectbox("Esquema", schemas)

tablas = inspector.get_table_names(schema=schema_sel)
tabla_sel = st.selectbox("Tabla", tablas)

columnas_sql = [c["name"] for c in inspector.get_columns(tabla_sel, schema=schema_sel)]
columnas_sql_norm = [normalizar_col(c) for c in columnas_sql]

# ======================================================
# 2️⃣ SUBIDA DE EXCEL
# ======================================================
st.subheader("2️⃣ Subir archivos Excel")

uploaded_files = st.file_uploader(
    "Selecciona uno o más archivos",
    type=["xlsx", "xls"],
    accept_multiple_files=True
)

if not uploaded_files:
    st.stop()

if "files" not in st.session_state:
    st.session_state["files"] = {}

for f in uploaded_files:
    if f.name not in st.session_state["files"]:
        st.session_state["files"][f.name] = {
            "bytes": f.read(),
            "sheet": None
        }

# ======================================================
# 3️⃣ SELECCIÓN DE HOJAS
# ======================================================
st.subheader("3️⃣ Seleccionar hoja")

for name, info in st.session_state["files"].items():
    xls = pd.ExcelFile(io.BytesIO(info["bytes"]))
    hoja = st.selectbox(
        f"{name}",
        xls.sheet_names,
        key=f"sheet_{name}"
    )
    info["sheet"] = hoja

# ======================================================
# OPCIONES GLOBALES
# ======================================================
st.markdown("---")
ignorar_extras = st.checkbox("Ignorar columnas extra en Excel", value=True)

# ======================================================
# 🚀 VALIDAR + CARGAR
# ======================================================
st.markdown("---")
if st.button("🚀 Validar y cargar a SQL Server"):
    progreso = st.progress(0)
    total = len(st.session_state["files"])

    for i, (name, info) in enumerate(st.session_state["files"].items(), start=1):
        try:
            df = pd.read_excel(io.BytesIO(info["bytes"]), sheet_name=info["sheet"])
            df = df.replace({np.nan: None})

            cols_excel_norm = [normalizar_col(c) for c in df.columns]
            faltantes = set(columnas_sql_norm) - set(cols_excel_norm)
            extras = set(cols_excel_norm) - set(columnas_sql_norm)

            if faltantes:
                st.error(f"❌ {name}: Faltan columnas obligatorias")
                continue

            if extras and not ignorar_extras:
                st.error(f"❌ {name}: Tiene columnas extra")
                continue

            df = df[[c for c in df.columns if normalizar_col(c) in columnas_sql_norm]]
            df.rename(columns=mapear_columnas(df.columns, columnas_sql), inplace=True)
            df = df[[c for c in columnas_sql if c in df.columns]]

            bulk_insert_fast(df, tabla_sel, schema_sel, engine)

            st.success(f"✅ {name} cargado correctamente")

        except Exception as e:
            st.error(f"❌ Error en {name}: {e}")
            st.text(traceback.format_exc())

        progreso.progress(i / total)

    st.balloons()
    st.success("🎉 Proceso finalizado correctamente")
