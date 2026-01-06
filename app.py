import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text
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


def bulk_insert(df, table, schema, engine):
    columnas = ",".join(f"[{c}]" for c in df.columns)
    params = ",".join(":" + c for c in df.columns)

    sql = text(
        f"INSERT INTO [{schema}].[{table}] ({columnas}) VALUES ({params})"
    )

    data = df.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(sql, data)


# ======================================================
# 🔗 CONEXIÓN SQL SERVER (INTERNET / PYTDS)
# ======================================================
def crear_engine(servidor, database, username, password, puerto):
    engine = create_engine(
        f"mssql+pytds://{username}:{password}@{servidor}:{puerto}/{database}"
    )
    return engine


# ======================================================
# 🚀 UI CONFIG
# ======================================================
st.set_page_config(
    page_title="Importador Excel → SQL Server",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Importador Excel → SQL Server")
st.caption("Aplicación web – Streamlit Cloud")

# ======================================================
# 🔐 0️⃣ CONEXIÓN A SQL SERVER
# ======================================================
st.subheader("🔐 Conexión a SQL Server")

with st.form("conexion_sql"):
    servidor = st.text_input(
        "Servidor (IP o DNS)",
        placeholder="sql.midominio.com o 200.50.xxx.xxx"
    )
    puerto = st.number_input("Puerto", value=1433, step=1)
    database = st.text_input("Base de datos")
    username = st.text_input("Usuario SQL")
    password = st.text_input("Contraseña", type="password")

    conectar = st.form_submit_button("🔌 Conectar")

if conectar:
    try:
        engine = crear_engine(
            servidor.strip(),
            database.strip(),
            username.strip(),
            password,
            puerto
        )

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        st.session_state["engine"] = engine
        st.success("✅ Conexión exitosa")

    except Exception as e:
        st.error("❌ Error de conexión")
        st.code(str(e))
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
    "Selecciona uno o más archivos Excel",
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
        f"Archivo: {name}",
        xls.sheet_names,
        key=f"sheet_{name}"
    )
    info["sheet"] = hoja

# ======================================================
# OPCIONES
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
            df = pd.read_excel(
                io.BytesIO(info["bytes"]),
                sheet_name=info["sheet"]
            )
            df = df.replace({np.nan: None})

            cols_excel_norm = [normalizar_col(c) for c in df.columns]
            faltantes = set(columnas_sql_norm) - set(cols_excel_norm)
            extras = set(cols_excel_norm) - set(columnas_sql_norm)

            if faltantes:
                st.error(f"❌ {name}: faltan columnas obligatorias")
                continue

            if extras and not ignorar_extras:
                st.error(f"❌ {name}: contiene columnas extra")
                continue

            # Filtrar y mapear columnas
            df = df[[c for c in df.columns if normalizar_col(c) in columnas_sql_norm]]
            df.rename(columns=mapear_columnas(df.columns, columnas_sql), inplace=True)
            df = df[[c for c in columnas_sql if c in df.columns]]

            # Insertar
            bulk_insert(df, tabla_sel, schema_sel, engine)

            st.success(f"✅ {name} cargado correctamente")

        except Exception as e:
            st.error(f"❌ Error en {name}")
            st.code(traceback.format_exc())

        progreso.progress(i / total)

    st.balloons()
    st.success("🎉 Proceso finalizado correctamente")
