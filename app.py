import time
import datetime
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from streamlit import rerun

from lib import *

# --------------- CONFIGURACIÓN DE LA PÁGINA -----------------#
st.markdown("""
    <style>
        /* Quitar márgenes y padding del contenedor principal */
        .appview-container .main, .block-container {
            padding: 10 !important;
            margin: 5 !important;
            max-width: 90% !important;
        }
    </style>
""", unsafe_allow_html=True)

# --------------- CONEXIÓN A LA BASE DE DATOS -----------------#
engine = create_engine(st.secrets["connections"]["neon"]["url"])

# --------------- SECCIÓN PRINCIPAL -----------------#
# Obtener lugares únicos
places = get_unique_places()
st.title('Analizador de crimen sobre vehículos')

# Filtros y segmentación
col1, col2, _, col3 = st.columns((2, 1, 3, 1))
with col1:
    predict = st.checkbox("Predicción de crimen a futuro", value=False)
with col2:
    pond = st.checkbox("Ponderar crímenes", value=False)
with col3:
    with st.popover("Segmentación"):
        chosen_crime = st.multiselect('Segmentado por crimen',
                                      sorted(category_map.keys()),
                                      default=None)
        chosen_place = st.multiselect('Segmentado por lugar',
                                      sorted(places),
                                      default=None)

freq_choice = st.radio("Predicción de crimen a futuro",
                       ["Por trimestre", "Por mes", "Por semana"],
                       label_visibility="collapsed",
                       horizontal=True)
freq = freqmap[freq_choice]

# Construir condiciones de filtrado
crime_conditions, place_conditions = build_conditions(chosen_crime, chosen_place)

grouped = fetch_grouped_data(crime_conditions, place_conditions, freq)
apply_ponderation_to_data(grouped, pond)
# --------------- GRÁFICOS -----------------#
if predict:
    st.header("Predicción de Crimen a Futuro")
    n_steps = st.slider("Número de etapas a predecir",
                        min_value=freq[1],
                        max_value=freq[2],
                        value=(freq[1] + freq[2]) // 2,
                        label_visibility="collapsed")
    forecast = forecast_data(grouped, freq, n_steps)
    chart, combined = create_combined_chart(grouped, forecast)
else:
    chart, grouped = create_historical_chart(grouped)

col_chart, col_kpi = st.columns((12, 2))
with col_chart:
    st.altair_chart(chart, use_container_width=True)
with col_kpi:
    display_kpis(combined if predict else grouped, freq)
    st.markdown(arrow, unsafe_allow_html=True)

st.container(height=200, border=False)

# --------------- INPUT DE DATOS -----------------#
if "new_data" not in st.session_state:
    st.session_state.new_data = pd.DataFrame()

col_input1, col_empty, col_input2 = st.columns((2, 1, 2))
st.markdown('<div id="target-section"></div>', unsafe_allow_html=True)

with col_input1:
    st.header("Inserte un nuevo crimen")
    date = st.date_input("Fecha del crimen", value=datetime.date.today())
    place = st.selectbox("Lugar del crimen", sorted(places))
    crime = st.selectbox("Tipo de crimen", sorted(category_map.keys()))
    if st.button("Agregar"):
        st.session_state.new_data = st.session_state.new_data._append(
            {'date': date, 'crimecodedesc': category_map[crime], 'areaname': place},
            ignore_index=True)
        st.session_state.new_data = apply_pond(st.session_state.new_data)

with col_input2:
    st.header("O arrastre aquí un archivo con los datos")
    uploaded_data = st.file_uploader("Archivo .csv", type=["csv"], label_visibility="collapsed")
    if uploaded_data is not None:
        uploaded_data = pd.read_csv(uploaded_data)
        st.session_state.new_data = st.session_state.new_data._append(
            uploaded_data[['date', 'crimecodedesc', 'areaname']],
            ignore_index=True)
        st.session_state.new_data = apply_pond(st.session_state.new_data)

st.dataframe(data=st.session_state.new_data, height=200)

# --------------- BOTONES DE ACCIÓN -----------------#
col_btn1, col_btn2, col_empty2 = st.columns((1, 1, 7))
ret = None
with col_btn1:
    if st.button("Guardar datos"):
        with st.spinner("Guardando datos..."):

            with engine.begin() as connection:
                for _, row in st.session_state.new_data.iterrows():
                    insert_data_query = text("""
                        INSERT INTO main (date, crimecodedesc, areaname, rawpond, pond)
                        VALUES (:date, :crimecodedesc, :areaname, :rawpond, :pond)
                    """)
                    ret = connection.execute(insert_data_query, row.to_dict())

with col_btn2:
    if st.button("Borrar datos"):
        st.session_state.new_data = pd.DataFrame()

if ret is not None:
    if ret.rowcount > 0:
        st.success("Datos guardados correctamente!")
        st.session_state.new_data = pd.DataFrame()
        time.sleep(3)
        st.rerun()
    else:
        st.error("Error al guardar los datos")