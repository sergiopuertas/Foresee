import pandas as pd
from prophet import Prophet
import sqlalchemy as sa
from sqlalchemy import text, Engine
import uuid
from argon2 import PasswordHasher
import uvicorn
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Depends, Header, Response
from pydantic import BaseModel, EmailStr
import uuid
from datetime import datetime
import dotenv
import os


def get_engine():
    # Si no está definida la variable, cargamos el archivo .env, útil solo en desarrollo
    if not os.getenv("DB"):
        dotenv.load_dotenv()  # Esto busca un archivo .env en la raíz
    db_url = os.getenv("DB")
    if not db_url:
        raise RuntimeError("La variable de entorno DB no está definida.")
    return sa.engine.create_engine(db_url, pool_pre_ping=True)


ph = PasswordHasher()

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
    "Por trimestre": ["quarter", 1, 16, "QS", None, [True, False]],
    "Por día":["day", 1, 365, "D", "%d %b %Y", [True, True]],
}

class DataComponents:
    def __init__(self, engine):
        self.engine = engine

    def get_user_permissions(_self, email):
        """ Obtiene los permisos de un usuario en función de sus roles """
        query = """
            SELECT p.resource
            FROM usuarios u
            JOIN user_roles ur ON u.id = ur.user_id
            JOIN roles r ON ur.role_id = r.id
            JOIN role_permissions rp ON r.id = rp.role_id
            JOIN permissions p ON rp.permission_id = p.id
            WHERE u.email = :email
            GROUP BY p.resource
        """
        with _self.engine.connect() as conn:
            result = conn.execute(text(query), {'email': email})

        rows = result.fetchall()
        permissions = [row[0] for row in rows]
        return permissions

    def get_user_area(_self, email):
        """ Obtiene los permisos de un usuario en función de sus roles """
        query = """
                SELECT area FROM usuarios WHERE email = :email
            """
        with _self.engine.connect() as conn:
            result = conn.execute(text(query), {'email': email})
        rows = result.fetchall()
        area = [row[0] for row in rows]
        return area[0]

    def get_secure_unique_places(_self, email, see_permissions):
        """ Obtiene las áreas disponibles según los permisos del usuario. """
        if see_permissions == 'SEE_LOCAL':
            area_conditions = f"areaname = '{_self.get_user_area(email)}'"
        elif see_permissions == 'SEE_ALL':
            area_conditions = "1=1"
        else:
            area_conditions = "1=0"

        query = f"SELECT DISTINCT areaname FROM main WHERE {area_conditions}"
        with _self.engine.connect() as conn:
            result = conn.execute(text(query))
        rows = result.fetchall()
        return [row[0] for row in rows]

    def secure_fetch_grouped_data(_self, crime_conditions, place_conditions, freq, init_time=None, end_time=None):
        """ Obtiene datos agrupados según los permisos del usuario. """

        if not init_time or not end_time:
            query_min_max = "SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM main"
            with _self.engine.connect() as conn:
                result = conn.execute(text(query_min_max)).fetchone()
            init_time = init_time or result[0]
            end_time = end_time or result[1]

        date_filter = f"AND date BETWEEN '{init_time}' AND '{end_time}'"
        print(freq)
        if freq in['month', 'week', 'quarter', 'day']:
            query = f"""
                SELECT DATE_TRUNC('{freq}', date) AS period, COUNT(*) AS count
                FROM main
                WHERE ({crime_conditions}) AND ({place_conditions}) {date_filter}
                GROUP BY period
                ORDER BY period
            """
        elif freq == "Custom" and init_time is not None and end_time is not None:
            query = f"""
                SELECT COUNT(*) AS count
                FROM main
                WHERE ({crime_conditions}) AND ({place_conditions}) {date_filter}
            """
        elif freq is None and init_time is not None and end_time is not None:
            query = f"""
                SELECT date AS period, crimecodedesc, areaname
                FROM main
                WHERE ({crime_conditions}) AND ({place_conditions}) {date_filter}
                ORDER BY date"""
        else:
            raise HTTPException(status_code=400, detail="Frecuencia no válida o no soportada")

        with _self.engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()

        columns = result.keys()
        return pd.DataFrame(rows, columns=columns) if rows else None

    def create_user(self, email, full_name, area, password, role):
        """ Crea un nuevo usuario con un ID único y sin roles asignados. """
        user_id = uuid.uuid5(uuid.NAMESPACE_DNS, email)
        password_hash = ph.hash(password)

        query = """
            INSERT INTO usuarios (id, email, full_name,area, password)
            VALUES (:id, :email, :full_name,:area, :password)
        """
        params = {
            'id': str(user_id),
            'email': email,
            'full_name': full_name,
            'area': area,
            'password': password_hash
        }
        with self.engine.connect() as conn:
                conn.execute(text(query), params)
                conn.commit()

        query = """
            SELECT id FROM roles WHERE name = :role
        """

        with self.engine.connect() as conn:
            role_result = conn.execute(text(query), {'role': role})
        role_id = role_result.fetchone()[0]

        query = """
            INSERT INTO user_roles (user_id, role_id)
            VALUES (:user_id, :role_id)
        """
        params = {
            'user_id': str(user_id),
            'role_id': role_id
        }

        with self.engine.connect() as conn:
            conn.execute(text(query), params)
            conn.commit()
        return True


    def get_user(_self, email):
        """ Obtiene la información de un usuario por email. """
        query = "SELECT * FROM usuarios WHERE email = :email"
        with _self.engine.connect() as conn:
            result = conn.execute(text(query), {'email': email})
        rows = result.fetchall()
        columns = result.keys()
        return pd.DataFrame(rows, columns=columns) if rows else None


    def verify_login(_self, email, plain_password):
        """ Verifica la contraseña de un usuario. """
        query = "SELECT password FROM usuarios WHERE email = :email"
        with _self.engine.connect() as conn:
            result = conn.execute(text(query), {'email': email})
        row = result.fetchone()
        if row is None:
                print("Usuario no encontrado")
                return False

        stored_hash = row[0]
        return ph.verify(stored_hash, plain_password,)

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