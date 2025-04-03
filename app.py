# app.py
from lib import *

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
@st.cache_resource
def get_engine():
    DB = toml.load(".streamlit/secrets.toml")["DB"]["url"]
    return sa.engine.create_engine(DB)

def get_connection():
    if "connection" not in st.session_state or st.session_state.connection.closed:
        print("nueva conexión")
        st.session_state.connection = get_engine().connect()
    return st.session_state.connection

def main():
    conn = get_connection()
    data_components = DataComponents(conn)

    if "authentication_status" not in st.session_state:
            st.session_state["authentication_status"] = None
    st.markdown("<h1 style='font-size: 6em; text-align: center;'>Foresee</h1>", unsafe_allow_html=True)
    st.container(height=20, border=False)
    if not st.session_state.get("authentication_status"):
            handle_authentication(data_components)
    else:
        user_email = st.session_state["mail"]
        perms = data_components.get_user_permissions(user_email)

        create_data = "Nuevos datos SI" in perms
        create_users = "Nuevos usuarios SI" in perms
        KPI = "KPI SI" in perms
        Predict_perms = 'PREDICT SI' in perms
        see_perms = [perm for perm in perms if "SEE" in perm][0]

        places = data_components.get_secure_unique_places(user_email, see_perms)
        predict, pond, chosen_crime, chosen_place = InteractionComponents.create_filters(places, Predict_perms)


        # Configuración de frecuencia
        freq_choice = st.radio("Predicción de crimen a futuro",
                               ["Por trimestre", "Por mes", "Por semana"],
                               label_visibility="collapsed",
                               horizontal=True,
                               key="freq_choice")

        # Obtener y procesar datos
        crime_cond, place_cond = build_conditions(chosen_crime, chosen_place)
        grouped = data_components.secure_fetch_grouped_data(crime_cond, place_cond, freqmap[freq_choice])
        apply_ponderation_to_data(grouped, pond)

        if predict:
            st.header("Predicción de Crimen a Futuro")
            n_steps = st.slider("Número de etapas a predecir",
                                min_value=freqmap[freq_choice][1],
                                max_value=freqmap[freq_choice][2],
                                value=(freqmap[freq_choice][1] + freqmap[freq_choice][2]) // 2,
                                label_visibility="collapsed")
            forecast = forecast_data(grouped, freqmap[freq_choice], n_steps)
            chart, combined = create_combined_chart(grouped, forecast)
        else:
            chart, grouped = create_historical_chart(grouped)

        # Mostrar gráficos y KPIs
        st.altair_chart(chart, use_container_width=True)
        if KPI:
            display_kpis(combined if predict else grouped, freqmap[freq_choice])

        st.container(height=20, border=False)

        if create_data:
            if "new_data" not in st.session_state:
                st.session_state["new_data"] = pd.DataFrame()
            # Componentes de administración
            InteractionComponents.create_data_input(
                lambda: data_components.get_secure_unique_places(user_email, "SEE_ALL"), conn)
        if create_users:
            InteractionComponents.user_create_form(data_components,
                                                   lambda: data_components.get_secure_unique_places(user_email, "SEE_ALL"))

        # Botón de logout
        _,col,_ = st.columns((4, 1, 4))
        with col:
            if st.button("Cerrar sesión"):
                st.session_state["authentication_status"] = None
                st.rerun()


if __name__ == "__main__":
    main()