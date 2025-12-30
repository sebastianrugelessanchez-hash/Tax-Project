"""
ETL_COMMAND.py - Módulo de extracción y transformación para datos COMMAND

Este módulo realiza:
- Normalización simple de datos
- Extracción de City/State desde la descripción
- Construcción de city_state_key para el merge
"""

import pandas as pd
import re
from typing import Tuple


def parse_city_state(description: str) -> Tuple[str, str]:
    """
    Extrae ciudad y estado de un string como 'PLEASANTON, TX'

    Args:
        description: String con formato 'CITY, ST'

    Returns:
        Tupla (city, state) normalizada
    """
    if pd.isna(description) or not isinstance(description, str):
        return None, None

    # Patrón: CITY NAME, ST
    match = re.match(r'^(.+?),\s*([A-Z]{2})$', description.strip())
    if match:
        city = match.group(1).strip().upper()
        state = match.group(2).strip().upper()
        return city, state

    return None, None


def build_city_state_key(city: str, state: str) -> str:
    """
    Construye una clave única para city/state

    Args:
        city: Nombre de la ciudad
        state: Código del estado (2 letras)

    Returns:
        Clave normalizada 'CITY_STATE'
    """
    if city is None or state is None:
        return None

    # Normalizar: remover espacios extra, convertir a mayúsculas
    city_normalized = re.sub(r'\s+', ' ', city.strip().upper())
    state_normalized = state.strip().upper()

    return f"{city_normalized}_{state_normalized}"


def extract_command_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrae y normaliza datos del archivo COMMAND

    El archivo COMMAND tiene un formato simple:
    - Tax code: Código del impuesto (ej: 'PLE')
    - Description: Ciudad y estado (ej: 'PLEASANTON, TX')
    - Short description: Descripción corta

    Args:
        df: DataFrame crudo del archivo COMMAND

    Returns:
        DataFrame con columnas: tax_code, city, state, description, city_state_key
    """
    records = []

    for idx, row in df.iterrows():
        tax_code = row['Tax code']
        description = row['Description']
        short_desc = row['Short description']

        city, state = parse_city_state(description)

        if city and state:
            records.append({
                'tax_code': tax_code,
                'city': city,
                'state': state,
                'description': description,
                'short_description': short_desc,
                'city_state_key': build_city_state_key(city, state)
            })

    return pd.DataFrame(records)


def transform_command(file_path: str) -> pd.DataFrame:
    """
    Pipeline completo de ETL para datos COMMAND

    Args:
        file_path: Ruta al archivo Excel de COMMAND

    Returns:
        DataFrame limpio y transformado con city_state_key
    """
    # Leer archivo Excel
    df_raw = pd.read_excel(file_path)

    # Extraer y normalizar datos
    df_clean = extract_command_data(df_raw)

    # Agregar columna de origen
    df_clean['source'] = 'COMMAND'

    return df_clean


if __name__ == "__main__":
    # Test del módulo
    import os

    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(base_path, "Base de datos", "Tax Code Report-COMMNAD.xlsx")

    df = transform_command(file_path)
    print(f"Registros extraídos: {len(df)}")
    print(f"\nColumnas: {list(df.columns)}")
    print(f"\nPrimeros 10 registros:")
    print(df.head(10).to_string())
