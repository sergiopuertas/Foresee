import bcrypt
import datetime
import time
import streamlit as st
import altair as alt
import pandas as pd
from prophet import Prophet
from sqlalchemy import text
import uuid
import sqlalchemy as sa
import toml

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
    "Por mes": ["month", 1, 24, "MS", "%b %Y", [True, False]],
    "Por semana": ["week", 1, 104, "W", "%d %b %Y", [True, True]],
    "Por trimestre": ["quarter", 1, 16, "QS", None, [True, False]]
}

class DataComponents:
    def __init__(self, connection):
        self.connection = connection

    @st.cache_data(ttl=600)
    def get_secure_unique_places(_self, user_role, user_area):
        area_condition = "" if user_role == 'admin' else f" AND areaname = {user_area}"

        query = f"SELECT DISTINCT areaname FROM main WHERE 1=1 {area_condition}"
        result = _self.connection.execute(text(query))
        rows = result.fetchall()
        return [row[0] for row in rows]

    @st.cache_data(ttl=600)
    def secure_fetch_grouped_data(_self, crime_conditions, place_conditions, freq):
        query = f"""
            SELECT
                DATE_TRUNC(:freq, date) AS period,
                COUNT(*) AS count,
                AVG(pond) AS pond
            FROM main
            WHERE ({crime_conditions}) AND ({place_conditions})
            GROUP BY period
            ORDER BY period
        """
        result = _self.connection.execute(text(query), {'freq': freq[0]})
        rows = result.fetchall()
        columns = result.keys()
        return pd.DataFrame(rows, columns=columns) if rows else None

    @st.cache_data(ttl=600)
    def get_user(_self, email):
        query = f"SELECT * FROM usuarios WHERE email = \'{email}\' "
        result = _self.connection.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()
        return pd.DataFrame(rows, columns=columns) if rows else None

    def create_user(self, email, full_name, role, area, password):
        user_id = uuid.uuid5(uuid.NAMESPACE_DNS, email)
        query = """
            INSERT INTO usuarios (id, email, full_name, role, area, password)
            VALUES (:id, :email, :full_name, :role, :area, :password)
        """
        params = {
            'id': str(user_id),
            'email': email,
            'full_name': full_name,
            'role': role,
            'area': area,
            'password': password
        }
        try:
            self.connection.execute(text(query), params)
            self.connection.commit()
            return True
        except Exception as e:
            st.error(f"Error al crear usuario: {str(e)}")
            return False

    @st.cache_data(ttl=600)
    def verify_login(_self, email, plain_password):
        query = f"SELECT password FROM usuarios WHERE email = \'{email}\' "
        try:
            result = _self.connection.execute(text(query))
            row = result.fetchone()
            if row is None:
                return False

            stored_hash = row[0].encode('utf-8')
            return bcrypt.checkpw(plain_password.encode('utf-8'), stored_hash)
        except Exception as e:
            st.error(f"Error en verificación: {str(e)}")
            return False


class InteractionComponents:
    @staticmethod
    def create_filters(places):
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
        return predict, pond, chosen_crime, chosen_place

    @staticmethod
    def create_data_input(user_role, get_places_func, conn):
        if user_role == 'admin':
            with st.expander("📝 Ingreso de Datos", expanded=False):
                if "new_data" not in st.session_state:
                    st.session_state.new_data = pd.DataFrame()

                col_input1, _, col_input2 = st.columns((2, 1, 2))
                with col_input1:
                    st.header("Inserte un nuevo crimen")
                    date = st.date_input("Fecha del crimen", value=datetime.date.today())
                    place = st.selectbox("Lugar del crimen", sorted(get_places_func()))
                    crime = st.selectbox("Tipo de crimen", sorted(category_map.keys()))
                    if st.button("Agregar"):
                        new_entry = {
                            'date': date,
                            'crimecodedesc': category_map[crime],
                            'areaname': place
                        }
                        st.session_state.new_data = pd.concat([
                            st.session_state.new_data,
                            pd.DataFrame([new_entry])
                        ], ignore_index=True)
                        st.session_state.new_data = apply_pond(st.session_state.new_data)

                with col_input2:
                    st.header("Cargar archivo CSV")
                    uploaded_data = st.file_uploader("Archivo .csv", type=["csv"], label_visibility="collapsed", on_change=st.cache_data.clear)
                    if uploaded_data is not None:
                        uploaded_df = pd.read_csv(uploaded_data)
                        st.session_state.new_data = pd.concat([
                            st.session_state.new_data,
                            uploaded_df[['date', 'crimecodedesc', 'areaname']]
                        ], ignore_index=True)
                        st.session_state.new_data = apply_pond(st.session_state.new_data)

                st.dataframe(st.session_state.new_data, height=200)
                InteractionComponents.save_delete_data(conn)

    @staticmethod
    def save_delete_data(conn):
        col_btn1, col_btn2, _ = st.columns((1, 1, 6))
        with col_btn1:
            if st.button("Guardar datos",on_click=st.cache_data.clear):
                if not st.session_state.new_data.empty:
                    with st.spinner("Guardando datos..."):
                        try:
                            st.session_state.new_data.to_sql(
                                    'main',
                                    conn,
                                    if_exists='append',
                                    index=False
                            )
                            st.session_state.new_data = pd.DataFrame()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al guardar datos: {str(e)}")
        with col_btn2:
            if st.button("Borrar datos"):
                st.session_state.new_data = pd.DataFrame()

    @staticmethod
    def user_create_form(data_components, get_places_func):
        with st.expander("🔒 Administración de usuarios - Registrar nuevo", expanded=False):
            with st.form("register_form", clear_on_submit=True ):
                col1, col2 = st.columns(2)
                with col1:
                    new_name = st.text_input("Nombre completo*")
                    new_email = st.text_input("Email*")
                    new_role = st.selectbox("Rol*", ["admin", "user"])
                with col2:
                    new_password = st.text_input("Contraseña*", type="password")
                    confirm_password = st.text_input("Confirmar contraseña*", type="password")
                    new_area = st.selectbox("Área", get_places_func())

                if st.form_submit_button("🎯 Registrar usuario", on_click=st.cache_data.clear):
                    if data_components.get_user(new_email) is not None:
                        st.error("❌ El usuario ya existe")
                    elif not all([ new_name, new_email, new_password]):
                        st.error("❌ Todos los campos son obligatorios")
                    elif "@" not in new_email or "." not in new_email:
                        st.error("❌ El correo electrónico no es válido")
                    elif len(new_password) < 8:
                        st.error("❌ La contraseña debe tener al menos 8 caracteres")
                    elif confirm_password != new_password:
                        st.error("❌ Las contraseñas deben coincidir")
                    else:
                        try:
                            hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt(12)).decode('utf-8')
                            data_components.create_user(
                                new_email,
                                new_name,
                                new_role,
                                new_area if new_role == "user" else None,
                                hashed_password,
                            )

                            st.success("✅ Usuario registrado exitosamente")
                        except Exception as e:
                            st.error(f"❌ Error al registrar: {str(e)}")


def handle_authentication(data_components):
    if "authentication_status" not in st.session_state:
        st.session_state["authentication_status"] = None
        st.session_state["username"] = None

    empty = st.empty()
    with empty:
        login_container = st.container()

        with login_container:
            st.title("Bienvenido a Foresee")
            st.subheader("Por favor, inicie sesión")
            st.container(height=40, border=False)
            mail = st.text_input("Email")
            password = st.text_input("Password", type="password")
            login_button = st.button("Login")
    if login_button:
        user = data_components.get_user(mail)
        if user is not None and data_components.verify_login(mail, password):
            st.session_state["authentication_status"] = True
            st.session_state["mail"] = mail
            st.session_state["user_info"] = {
                'role': user['role'].iloc[0],
                'area': user['area'].iloc[0]
            }
            login_container.empty()
            st.rerun()
        else:
            st.error("Credenciales inválidas")

# --------------- FUNCIONES AUXILIARES -----------------#

def format_quarter(date):
    quarter = (date.month - 1) // 3 + 1
    return f"T{quarter} {date.year}"


def build_conditions(chosen_crime, chosen_place):
    crime_conditions = " OR ".join(
        [f"crimecodedesc = '{category_map[crime]}'" for crime in chosen_crime]
    ) if chosen_crime else "1=1"

    place_conditions = " OR ".join(
        [f"areaname = '{place}'" for place in chosen_place]
    ) if chosen_place else "1=1"

    return crime_conditions, place_conditions

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
    columns = st.columns((1,3,3,3,3))
    with columns[1]:
        st.metric('Total', int(grouped['yhat'].sum()))
    with columns[2]:
        st.metric('Media', int(grouped['yhat'].mean()))
    with columns[3]:
        st.metric('Periodo con más crímenes', max_period)
    with columns[4]:
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