import streamlit as st
from prophet import Prophet
import altair as alt
import pandas as pd

conn = st.connection("neon", type="sql")

# ---------------   CONFIG    ------------------#

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

category_map = {
    'STOLEN VEHICLE': 'VEHICLE - STOLEN',
    'VEHICLE STEALING ATTEMPT': 'VEHICLE - ATTEMPT STOLEN',
    'OTHER VEHICLES STOLEN': 'VEHICLE, STOLEN - OTHER (MOTORIZED SCOOTERS, BIKES, ETC)',
    'BURGLARY': 'BURGLARY FROM VEHICLE',
    'BURGLARY (ATTEMPT)': 'BURGLARY FROM VEHICLE, ATTEMPTED',
    'THROWING OBJECT AT VEHICLE': 'THROWING OBJECT AT MOVING VEHICLE',
    'THEFT FROM MOTOR VEHICLE-ATTEMPT': 'THEFT FROM MOTOR VEHICLE - ATTEMPT',
    'THEFT FROM MOTOR VEHICLE (PETTY)': 'THEFT FROM MOTOR VEHICLE - PETTY ($950 & UNDER)',
    'THEFT FROM MOTOR VEHICLE (GRAND)': 'THEFT FROM MOTOR VEHICLE - GRAND ($950.01 AND OVER)',
    'SHOTS FIRED AT VEHICLE': 'SHOTS FIRED AT MOVING VEHICLE, TRAIN OR AIRCRAFT'
}

freqmap = {
    "Por mes": ["month", 1, 12, "MS", "%b %Y", [True, False]],
    "Por semana": ["week", 1, 52, "W", "%d %b %Y", [True, True]],
    "Por trimestre": ["quarter", 1, 8, "QS", None, [True, False]]
}


def format_quarter(date):
    quarter = (date.month - 1) // 3 + 1
    return f"T{quarter} {date.year}"


# ---------------   PAGE    ------------------#

places_q = f"""
    SELECT DISTINCT areaname
    FROM main
"""

places = conn.query(places_q,ttl = "10m")['areaname']

st.title('Analizador de crimen sobre vehículos')

with st.popover("Filtros"):
    col = st.columns(2)
    with col[0]:
        predict = st.checkbox("Predicción de crimen a futuro", value=False)
    with col[1]:
        pond = st.checkbox("Ponderar crímenes", value=False)

    freq = freqmap[st.radio("Predicción de crimen a futuro", ["Por trimestre", "Por mes", "Por semana"],
                            label_visibility="collapsed", horizontal=True)]

    chosen_crime = st.multiselect('Segmentado por crimen', sorted(category_map.keys(), key=lambda x:
    x), label_visibility="visible", default=None)
    chosen_place = st.multiselect('Segmentado por lugar', sorted(places),
                                  label_visibility="visible", default=None)

# Filtrar con base en los filtros elegidos
crime_conditions = " OR ".join(
    [f"crimecodedesc = '{category_map[crime]}'" for crime in chosen_crime]) if chosen_crime else "1=1"
place_conditions = " OR ".join([f"areaname = '{place}'" for place in chosen_place]) if chosen_place else "1=1"

# Construir la consulta SQL
sql_query = f"""
    SELECT date, crimecodedesc, areaname, pond
    FROM main
    WHERE ({crime_conditions})
    AND ({place_conditions})
"""

data = conn.query(sql_query,ttl = "10m")

if predict:
    st.header("Predicción de Crimen a Futuro")
    n_steps = st.slider("Número de etapas a predecir", min_value=freq[1], max_value=freq[2],
                        value=(freq[1] + freq[2]) // 2, label_visibility="collapsed")
else:
    n_steps = 0

# ---------------   DATA    ------------------#

grouped_query = f"""
    SELECT
        DATE_TRUNC('{freq[0]}', date) AS period,
        COUNT(*) AS count,
        AVG(pond) AS pond
    FROM main
    WHERE ({crime_conditions})
    AND ({place_conditions})
    GROUP BY period
    ORDER BY period
"""

grouped = conn.query(grouped_query, ttl = "10m")

print("Fecha mínima en los datos:", grouped['period'].min())
print("Fecha máxima en los datos:", grouped['period'].max())

# Aplicar ponderación si se ha seleccionado
if pond:
    total_original = grouped['count'].sum()
    total_ponderado = (grouped['count'] * grouped['pond']).sum()
    normalization_factor = total_original / total_ponderado
    grouped['count'] = grouped['count'] * grouped['pond'] * normalization_factor

# ---------------   CHART    ------------------#

if predict:
    # Preparar los datos para Prophet
    prophet_data = grouped[['period', 'count']].rename(columns={'period': 'ds', 'count': 'y'})
    prophet_data['ds'] = pd.to_datetime(prophet_data['ds'], utc = True).dt.tz_localize(None)

    # Ajustar el modelo Prophet
    model_prophet = Prophet(yearly_seasonality=freq[5][0], weekly_seasonality=freq[5][1], daily_seasonality=False)
    model_prophet.fit(prophet_data)

    # Crear fechas futuras
    future = model_prophet.make_future_dataframe(periods=n_steps, freq=freq[3])
    forecast = model_prophet.predict(future)

    # Preparar los datos para el gráfico
    last_date = prophet_data['ds'].max()
    forecast['tipo'] = forecast['ds'].apply(lambda x: 'Histórico' if x <= last_date else 'Predicción')
    data_to_plot = forecast[['ds', 'yhat', 'yhat_upper', 'yhat_lower', 'tipo']]



    y_min = forecast['yhat'].min()
    y_max = forecast['yhat'].max()
    if y_min < 0:
        y_min = 0
    rango = y_max - y_min
    if rango < 1e-3:
        domain = [y_min - 1, y_max + 1]
    else:
        padding = rango * 0.1
        domain = [y_min - padding, y_max + padding]

    scale_y = alt.Scale(domain=domain, nice=True)

    line_chart = alt.Chart(data_to_plot).mark_line(point=True).encode(
        x=alt.X('ds:T', title='Fecha'),
        y=alt.Y('yhat:Q', title='Valor', scale=scale_y),
        color=alt.Color(
            'tipo:N',
            scale=alt.Scale(domain=['Histórico', 'Predicción'], range=['lightblue', 'red']),
            legend=None
        )
    ).properties(
        width=700,
        height=600,
    )

    band_chart = alt.Chart(data_to_plot[data_to_plot["tipo"] == "Predicción"]).mark_area(opacity=0.3).encode(
        x=alt.X('ds:T', title='Fecha'),
        y=alt.Y('yhat_lower:Q', title='Valor', scale=scale_y),
        y2="yhat_upper:Q"
    ).properties(
        width=700,
        height=600,
    )
    chart = band_chart + line_chart
else:
    chart = alt.Chart(grouped).mark_line(point=True).encode(
        x=alt.X('period:T', title='Fecha'),
        y=alt.Y('count:Q', title='Valor',
                scale=alt.Scale(domain=[grouped['count'].min(), grouped['count'].max()])),
        color=alt.value('lightblue')
    ).properties(
        width=700,
        height=600,
    )

st.altair_chart(chart, use_container_width=True)

# ---------------   KPI    ------------------#
max_date = grouped.loc[grouped['count'].idxmax(), 'period']
min_date = grouped.loc[grouped['count'].idxmin(), 'period']

if not predict:
    with st.container():
        columns = st.columns(4)
        with columns[0]:
            st.metric('Total', int(grouped['count'].sum()))
        with columns[1]:
            st.metric('Media', int(grouped['count'].mean()))
        with columns[2]:
            max_period = format_quarter(max_date) if freq[0] == 'quarter' else max_date.strftime(freq[4])
            st.metric('Periodo con más crímenes', max_period)
        with columns[3]:
            min_period = format_quarter(min_date) if freq[0] == 'quarter' else min_date.strftime(freq[4])
            st.metric("Periodo con menos crímenes", min_period)
