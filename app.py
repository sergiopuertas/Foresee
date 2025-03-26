# app.py
import time
import datetime
import yaml
import streamlit as st
import streamlit_authenticator as stauth
import pandas as pd
from lib import *


# Componentes de datos
class DataComponents:
    def __init__(self, conn):
        self.conn = conn

    def get_secure_unique_places(self, user_role, user_area):
        area_condition = ""
        if user_role != 'admin' and user_area:
            area_condition = f" AND areaname = '{user_area}'"

        query = f"SELECT DISTINCT areaname FROM main WHERE 1=1 {area_condition}"
        result = self.conn.query(query, ttl=0)
        return [row[0] for row in result.values]

    def secure_fetch_grouped_data(_self, crime_conditions, place_conditions, freq):
        query = f"""
            SELECT
                DATE_TRUNC('{freq[0]}', date) AS period,
                COUNT(*) AS count,
                AVG(pond) AS pond
            FROM main
            WHERE ({crime_conditions}) AND ({place_conditions})
            GROUP BY period
            ORDER BY period
        """
        return _self.conn.query(query, ttl = 0)


# Componentes de UI
class UIComponents:

    @staticmethod
    def create_main_controls(places):
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
                    uploaded_data = st.file_uploader("Archivo .csv", type=["csv"], label_visibility="collapsed")
                    if uploaded_data is not None:
                        uploaded_df = pd.read_csv(uploaded_data)
                        st.session_state.new_data = pd.concat([
                            st.session_state.new_data,
                            uploaded_df[['date', 'crimecodedesc', 'areaname']]
                        ], ignore_index=True)
                        st.session_state.new_data = apply_pond(st.session_state.new_data)

                st.dataframe(st.session_state.new_data, height=200)
                UIComponents.create_data_buttons(conn)

    @staticmethod
    def create_data_buttons(conn):
        col_btn1, col_btn2, _ = st.columns((1, 1, 7))
        with col_btn1:
            if st.button("Guardar datos"):
                if not st.session_state.new_data.empty:
                    with st.spinner("Guardando datos..."):
                        try:
                            with conn._instance.begin() as connection:
                                st.session_state.new_data.to_sql(
                                    'main',
                                    connection,
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
    def create_user_management(authenticator, config, get_places_func):
        with st.expander("🔒 Administración de usuarios - Registrar nuevo", expanded=False):
            with st.form("register_form", clear_on_submit=True):
                col1, col2 = st.columns(2)
                with col1:
                    new_username = st.text_input("Nombre de usuario*")
                    new_name = st.text_input("Nombre completo*")
                    new_email = st.text_input("Email*")
                with col2:
                    new_password = st.text_input("Contraseña*", type="password")
                    new_role = st.selectbox("Rol*", ["admin", "user"])
                    new_area = st.selectbox("Área", get_places_func())

                if st.form_submit_button("🎯 Registrar usuario"):
                    if new_username in config['credentials']['usernames']:
                        st.error("❌ El usuario ya existe")
                    elif not all([new_username, new_name, new_email, new_password]):
                        st.error("❌ Todos los campos son obligatorios")
                    elif "@" not in new_email or "." not in new_email:
                        st.error("❌ El correo electrónico no es válido")
                    elif len(new_password) < 8:
                        st.error("❌ La contraseña debe tener al menos 8 caracteres")
                    else:
                        try:
                            hashed_password = stauth.Hasher([new_password]).generate()[0]
                            config['credentials']['usernames'][new_username] = {
                                'email': new_email,
                                'name': new_name,
                                'password': hashed_password,
                                'role': new_role,
                                'area': new_area if new_role == "user" else None
                            }

                            with open('config.yaml', 'w') as file:
                                yaml.dump(config, file, default_flow_style=False)

                            authenticator.credentials = config['credentials']
                            st.success("✅ Usuario registrado exitosamente")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al registrar: {str(e)}")

# Configuración inicial de la UI
def configure_ui():
    st.set_page_config(layout="wide")
    st.markdown("""
        <style>
            .appview-container .main, .block-container {
                padding: 10 !important;
                margin: 5 !important;
                max-width: 90% !important;
            }
        </style>
    """, unsafe_allow_html=True)


# Manejo de autenticación
def handle_authentication():
    with open("config.yaml") as file:
        config = yaml.load(file, Loader=yaml.SafeLoader)

    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )

    if "authentication_status" not in st.session_state:
        st.session_state["authentication_status"] = None
        st.session_state["username"] = None

    login_container = st.empty()

    with login_container:
        name, auth_status, username = authenticator.login("Iniciar Sesión", "main")

    if auth_status is False:
        st.error("Usuario/contraseña incorrectos")
    elif auth_status is None:
        st.warning("Por favor ingresa tus credenciales")
    else:
        st.session_state["authentication_status"] = True
        st.session_state["username"] = username
        login_container.empty()

    return authenticator, config


def main():
    configure_ui()
    authenticator, config = handle_authentication()

    if st.session_state["authentication_status"]:
        # Configuración inicial
        conn = st.connection("neon", type="sql")
        data_components = DataComponents(conn)
        ui_components = UIComponents()

        # Obtener información del usuario
        user_info = config['credentials']['usernames'].get(st.session_state["username"], {})
        user_role = user_info.get('role', 'user')
        user_area = user_info.get('area', None)

        # Componentes principales
        places = data_components.get_secure_unique_places(user_role, user_area)
        predict, pond, chosen_crime, chosen_place = ui_components.create_main_controls(places)

        # Configuración de frecuencia
        freq_choice = st.radio("Predicción de crimen a futuro",
                               ["Por trimestre", "Por mes", "Por semana"],
                               label_visibility="collapsed",
                               horizontal=True)
        freq = freqmap[freq_choice]

        # Obtener y procesar datos
        crime_cond, place_cond = build_conditions(chosen_crime, chosen_place)
        grouped = data_components.secure_fetch_grouped_data(crime_cond, place_cond, freq)
        apply_ponderation_to_data(grouped, pond)

        # Visualización de datos
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

        # Mostrar gráficos y KPIs
        col_chart, col_kpi = st.columns((12, 2))
        with col_chart:
            st.altair_chart(chart, use_container_width=True)
        with col_kpi:
            display_kpis(combined if predict else grouped, freq)

        st.container(height=20, border=False)

        # Componentes de administración
        ui_components.create_data_input(user_role,
                                        lambda: data_components.get_secure_unique_places(user_role, user_area),conn)
        if user_role == 'admin':
            ui_components.create_user_management(authenticator, config,
                                                 lambda: data_components.get_secure_unique_places(user_role, user_area))

        # Botón de logout
        col = st.columns((4, 1, 4))
        with col[1]:
            if st.button("Cerrar sesión"):
                st.session_state["authentication_status"] = None
                st.rerun()


if __name__ == "__main__":
    main()