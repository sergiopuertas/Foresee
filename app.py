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

def main():
    DB= toml.load(".streamlit/secrets.toml")["DB"]["url"]
    engine = sa.engine.create_engine(DB)

    with engine.connect() as conn:
        data_components = DataComponents(conn)

        if "authentication_status" not in st.session_state:
            st.session_state["authentication_status"] = None
        st.markdown("<h1 style='font-size: 6em; text-align: center;'>Foresee</h1>", unsafe_allow_html=True)
        if not st.session_state.get("authentication_status"):
            handle_authentication(data_components)
        else:
            user_email = st.session_state["mail"]
            perms = data_components.get_user_permissions(user_email)

            create_data = True if "Nuevos datos SI" in perms else False
            create_users = True if "Nuevos usuarios SI" in perms else False
            KPI = True if "KPI SI" in perms else False
            Predict_perms = True if 'PREDICT SI' in perms else False
            see_perms = [perm for perm in perms if "SEE" in perm][0]

            places = data_components.get_secure_unique_places(user_email,see_perms)
            predict, pond, chosen_crime, chosen_place = InteractionComponents.create_filters(places, Predict_perms)

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
            st.altair_chart(chart, use_container_width=True)
            if KPI:
                display_kpis(combined if predict else grouped, freq)

            st.container(height=20, border=False)

            if create_data:
                # Componentes de administración
                InteractionComponents.create_data_input(
                                                        lambda: data_components.get_secure_unique_places(user_email,"SEE_ALL"),conn)
            if create_users:
                InteractionComponents.user_create_form(data_components,
                                                       lambda: data_components.get_secure_unique_places(user_email,"SEE_ALL"))

            # Botón de logout
            col = st.columns((4, 1, 4))
            with col[1]:
                if st.button("Cerrar sesión"):
                    st.session_state["authentication_status"] = None
                    st.rerun()

if __name__ == "__main__":
    main()