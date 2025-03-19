import streamlit as st
from prophet import Prophet
import altair as alt
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import datetime
# Mapas de categorías y configuraciones de frecuencia
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

# Conexión a la base de datos
conn = st.connection("neon", type="sql")
engine = create_engine(st.secrets["connections"]["neon"]["url"])

arrow = """
<style>
    /* Estilos para la flecha minimalista */
    .scroll-arrow {
        display: block;
        width: 50px;
        height: 50px;
        margin: 20px auto; /* Centrado */
        font-size: 2em;
        line-height: 50px;
        text-align: center;
        color: #FFFFFF;
        text-decoration: none; /* Quita subrayado */
        cursor: pointer;
    }
    .scroll-arrow:hover {
        color: #FFFFFF;
    }
</style>

<!-- Enlace con la flecha -->
<a href="#target-section" class="scroll-arrow">&#8595;</a>

<script>
    document.addEventListener("DOMContentLoaded", function() {
        document.querySelector(".scroll-arrow").addEventListener("click", function(event) {
            event.preventDefault();
            document.querySelector("#target-section").scrollIntoView({ behavior: 'smooth' });
        });
    });
</script>
"""

# --------------- FUNCIONES AUXILIARES -----------------#


def format_quarter(date):
    quarter = (date.month - 1) // 3 + 1
    return f"T{quarter} {date.year}"

def get_unique_places():
    query = "SELECT DISTINCT areaname FROM main"

    result = conn.query(query, ttl="10m")
    return result['areaname']


def build_conditions(chosen_crime, chosen_place):
    crime_conditions = " OR ".join(
        [f"crimecodedesc = '{category_map[crime]}'" for crime in chosen_crime]
    ) if chosen_crime else "1=1"

    place_conditions = " OR ".join(
        [f"areaname = '{place}'" for place in chosen_place]
    ) if chosen_place else "1=1"

    return crime_conditions, place_conditions


def fetch_grouped_data(crime_conditions, place_conditions, freq):
    # Generar la query SQL
    query = f"""
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
    df = conn.query(query, ttl=0)

    return df

def apply_ponderation_to_data(grouped, apply_ponder):
    if apply_ponder:
        total_original = grouped['count'].sum()
        total_ponderado = (grouped['count'] * grouped['pond']).sum()
        normalization_factor = total_original / total_ponderado
        grouped['count'] = grouped['count'] * grouped['pond'] * normalization_factor
    return grouped


def forecast_data(grouped, freq, n_steps):
    prophet_data = grouped[['period', 'count']].rename(columns={'period': 'ds', 'count': 'y'})
    prophet_data['ds'] = pd.to_datetime(prophet_data['ds'], utc=True).dt.tz_localize(None)

    model = Prophet(yearly_seasonality=freq[5][0],
                    weekly_seasonality=freq[5][1],
                    daily_seasonality=False)
    model.fit(prophet_data)

    future = model.make_future_dataframe(periods=n_steps, freq=freq[3])
    forecast = model.predict(future)
    last_date = prophet_data['ds'].max()
    forecast['tipo'] = forecast['ds'].apply(lambda x: 'Histórico' if x <= last_date else 'Predicción')

    return forecast


def create_combined_chart(grouped, forecast):
    # Concatenar los datos históricos y las predicciones
    historical_data = grouped[['period', 'count']].rename(columns={'period': 'ds', 'count': 'yhat'})
    historical_data['ds'] = pd.to_datetime(historical_data['ds'], utc=True).dt.tz_localize(None)
    historical_data['tipo'] = 'Histórico'
    # Datos de predicción

    forecast_data = forecast[['ds', 'yhat', 'yhat_upper', 'yhat_lower', 'tipo']]
    last_historical_date = historical_data['ds'].max()
    forecast_data_filtered = forecast_data[forecast_data['ds'] > last_historical_date]

    combined_data = pd.concat([historical_data, forecast_data_filtered], ignore_index=True)

    # Definir el rango de valores en el eje Y
    y_min = combined_data['yhat'].min()
    y_max = combined_data['yhat'].max()
    if y_min < 0:
        y_min = 0
    range_diff = y_max - y_min
    padding = range_diff * 0.1 if range_diff > 1e-3 else 1
    domain = [y_min - padding, y_max + padding]

    scale_y = alt.Scale(domain=domain, nice=True)

    line_chart = alt.Chart(combined_data).mark_line(point=True).encode(
        x=alt.X('ds:T', title='Fecha'),
        y=alt.Y('yhat:Q', title='Valor', scale=scale_y),
        color=alt.Color('tipo:N', scale=alt.Scale(domain=['Histórico', 'Predicción'],
                                                  range=['lightblue', 'red']),
                        legend=None)
    ).properties(width=700, height=500)

    band_chart = alt.Chart(combined_data[combined_data["tipo"] == "Predicción"]).mark_area(opacity=0.3).encode(
        x=alt.X('ds:T', title='Fecha'),
        y=alt.Y('yhat_lower:Q', title='Valor', scale=scale_y),
        y2="yhat_upper:Q"
    ).properties(width=700, height=500)

    return band_chart + line_chart, combined_data


def create_historical_chart(grouped):
    # Definir dominio para el eje Y
    grouped = grouped[['period', 'count']].rename(columns={'period': 'ds', 'count': 'yhat'})
    return alt.Chart(grouped).mark_line(point=True).encode(
        x=alt.X('ds:T', title='Fecha'),
        y=alt.Y('yhat:Q', title='Valor',
                scale=alt.Scale(domain=[grouped['yhat'].min(), grouped['yhat'].max()])),
        color=alt.value('lightblue')
    ).properties(width=700, height=500), grouped


def display_kpis(grouped, freq):
    max_date = grouped.loc[grouped['yhat'].idxmax(), 'ds']
    min_date = grouped.loc[grouped['yhat'].idxmin(), 'ds']
    max_period = format_quarter(max_date) if freq[0] == 'quarter' else max_date.strftime(freq[4])
    min_period = format_quarter(min_date) if freq[0] == 'quarter' else min_date.strftime(freq[4])

    with st.container():
        st.metric('Total', int(grouped['yhat'].sum()))
        st.metric('Media', int(grouped['yhat'].mean()))
        st.metric('Periodo con más crímenes', max_period)
        st.metric('Periodo con menos crímenes', min_period)

def apply_pond(df):
    df['rawpond'] = df.apply(
            lambda row: 0.035 if 'ATTEMPT' in row['crimecodedesc'] or 'PETTY' in row['crimecodedesc'] or 'THROWING' in row[
                'crimecodedesc'] else
            0.1 if 'BURGLARY' in row['crimecodedesc'] else
            0.125 if 'SHOTS' in row['crimecodedesc'] else
            0.2,
            axis=1
        )
    df['pond'] = df.apply(
        lambda row: 0.2396657425039096 if 'ATTEMPT' in row['crimecodedesc'] or 'PETTY' in row['crimecodedesc'] or 'THROWING' in row[
            'crimecodedesc'] else
        0.6847592642968847 if 'BURGLARY' in row['crimecodedesc'] else
        0.8559490803711058 if 'SHOTS' in row['crimecodedesc'] else
        1.3695185285937694,
        axis=1
    )
    return df