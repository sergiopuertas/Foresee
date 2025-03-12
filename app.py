import pandas as pd
import streamlit as st
from prophet import Prophet
import altair as alt

# ---------------   CONFIG    ------------------#

st.set_page_config(
    initial_sidebar_state="collapsed",
)
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
    'OTHER VEHICLES STOLEN':'VEHICLE, STOLEN - OTHER (MOTORIZED SCOOTERS, BIKES, ETC)',
    'BURGLARY': 'BURGLARY FROM VEHICLE',
    'BURGLARY (ATTEMPT)': 'BURGLARY FROM VEHICLE, ATTEMPTED',
    'THROWING OBJECT AT VEHICLE':'THROWING OBJECT AT MOVING VEHICLE',
    'THEFT FROM MOTOR VEHICLE-ATTEMPT':'THEFT FROM MOTOR VEHICLE - ATTEMPT',
    'THEFT FROM MOTOR VEHICLE (PETTY)':'THEFT FROM MOTOR VEHICLE - PETTY ($950 & UNDER)',
    'THEFT FROM MOTOR VEHICLE (GRAND)':'THEFT FROM MOTOR VEHICLE - GRAND ($950.01 AND OVER)',
    'SHOTS FIRED AT VEHICLE':'SHOTS FIRED AT MOVING VEHICLE, TRAIN OR AIRCRAFT'

}
freqmap = {
    "Por mes": ["M", 1, 12, "MS", "%b %Y",[True,False]],
    "Por semana": ["W", 1, 52, "W", "%d %b %Y",[True,True]],
    "Por trimestre": ["Q", 1, 8, "QE", None,[True,False]]
}

def format_quarter(date):
    quarter = (date.month - 1) // 3 + 1
    return f"T{quarter} {date.year}"

@st.cache_data
def load_data():
    return pd.read_csv('data_cleaned.csv')

data = load_data()

# ---------------   PAGE    ------------------#

st.title('Analizador de crimen sobre vehículos')
col = st.columns((1,1,2))
with col[0]:
    predict = st.checkbox("Predicción de crimen a futuro", value=False)
with col[1]:
    pond = st.checkbox("Ponderar crímenes", value=False)
with st.sidebar:
    st.header("Opciones")
    chosen_crime = st.multiselect('Segmentado por crimen', category_map.keys(),label_visibility="visible",default=None)
    chosen_place = st.multiselect('Segmentado por lugar', data['AREA NAME'].unique(),label_visibility="visible",default=None)
if chosen_crime:
    data = data[data['Crm Cd Desc'].isin([category_map[x] for x in chosen_crime])]
if chosen_place:
    data = data[data['AREA NAME'].isin(chosen_place)]

freq = freqmap[st.radio("Predicción de crimen a futuro",["Por trimestre","Por mes", "Por semana"],label_visibility="collapsed",horizontal=True)]

if predict:
    st.header("Predicción de Crimen a Futuro")
    n_steps = st.slider("Número de etapas a predecir", min_value=freq[1], max_value=freq[2], value=(freq[1]+freq[2])//2,label_visibility="collapsed")
else:
    n_steps = 0

# ---------------   DATA    ------------------#

grouped = data.groupby(pd.to_datetime(data['DATE OCC'], format='mixed').dt.to_period(freq[0])).agg(
    count=('DATE OCC', 'size'),
    pond=('pond', 'mean')
).reset_index()

if pond:
    grouped['count'] = grouped['count'] * grouped['pond']
grouped['DATE OCC'] = pd.to_datetime(grouped['DATE OCC'].dt.strftime(freq[4]), format=freq[4])

monthly_data = grouped

# ---------------   CHART    ------------------#

if predict:

    prophet_data = monthly_data[['DATE OCC', 'count']].rename(columns={'DATE OCC': 'ds', 'count': 'y'})
    model_prophet = Prophet(yearly_seasonality=freq[5][0], weekly_seasonality=freq[5][1], daily_seasonality=False)
    model_prophet.fit(prophet_data)
    future = model_prophet.make_future_dataframe(periods=n_steps, freq=freq[3])
    forecast = model_prophet.predict(future)

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


    band_chart = alt.Chart(data_to_plot[data_to_plot["tipo"]=="Predicción"]).mark_area(opacity=0.3).encode(
        x=alt.X('ds:T', title='Fecha'),
        y=alt.Y('yhat_lower:Q', title='Valor', scale=scale_y),
        y2="yhat_upper:Q"
    ).properties(
        width=700,
        height=600,
    )
    chart = band_chart + line_chart

else:
    chart = alt.Chart(monthly_data).mark_line(point=True).encode(
        x=alt.X('DATE OCC:T', title='Fecha'),
        y=alt.Y('count:Q', title='Valor',
                scale=alt.Scale(domain=[monthly_data['count'].min(), monthly_data['count'].max()])),
        color=alt.value('lightblue')
    ).properties(
        width=700,
        height=600,
    )

st.altair_chart(chart, use_container_width=True)

# ---------------   KPI    ------------------#

if not predict:
    with st.container():
        columns = st.columns(4)
        with columns[0]:
            st.metric('Total', int(monthly_data['count'].sum()))

        with columns[1]:
            st.metric('Media', int(monthly_data['count'].mean()))

        with columns[2]:
            if freq[0] == "Q":
                max_period = format_quarter(monthly_data.loc[monthly_data['count'].idxmax(), 'DATE OCC'])
            else:
                max_period = monthly_data.loc[monthly_data['count'].idxmax(), 'DATE OCC'].strftime(freq[4])
            st.metric('Periodo con más crímenes', max_period)

        with columns[3]:
            if freq[0] == "Q":
                min_period = format_quarter(monthly_data.loc[monthly_data['count'].idxmin(), 'DATE OCC'])
            else:
                min_period = monthly_data.loc[monthly_data['count'].idxmin(), 'DATE OCC'].strftime(freq[4])

            st.metric("Periodo con menos crímenes", min_period)


