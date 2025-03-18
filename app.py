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

# --------------- SECCIÓN PRINCIPAL -----------------#

# Obtener lugares únicos
places = get_unique_places()

st.title('Analizador de crimen sobre vehículos')
# Filtros

col1, col2,_,col3= st.columns((1,1,2,1))
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

# Consulta general
sql_query = f"""
    SELECT date, crimecodedesc, areaname, pond
    FROM main
    WHERE ({crime_conditions})
      AND ({place_conditions})
"""
data = conn.query(sql_query, ttl="10m")

# Consulta agrupada para el gráfico
grouped = fetch_grouped_data(crime_conditions, place_conditions, freq)
grouped = apply_ponderation_to_data(grouped, pond)
# --------------- GRÁFICOS -----------------#
if predict:
    st.header("Predicción de Crimen a Futuro")
    n_steps = st.slider("Número de etapas a predecir",
                        min_value=freq[1], max_value=freq[2],
                        value=(freq[1] + freq[2]) // 2,
                        label_visibility="collapsed")

    forecast = forecast_data(grouped, freq, n_steps)
    chart = create_combined_chart(grouped, forecast)
else:
    chart = create_historical_chart(grouped)

st.altair_chart(chart, use_container_width=True)

# --------------- KPIs -----------------#
if not predict:
    display_kpis(grouped, freq)
