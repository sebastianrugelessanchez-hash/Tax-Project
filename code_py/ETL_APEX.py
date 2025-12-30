"""
ETL_APEX.py - Módulo de extracción y transformación para datos APEX

Este módulo realiza:
- Parseo basado en bloques del archivo Tax Code Report-APEX.xlsx
- Extracción de City/State
- Construcción de city_state_key para el merge
"""

import pandas as pd
import re
from typing import Tuple


def parse_city_state(location: str) -> Tuple[str, str]:
    """
    Extrae ciudad y estado de un string como 'ADDISON, TX'

    Args:
        location: String con formato 'CITY, ST'

    Returns:
        Tupla (city, state) normalizada
    """
    if pd.isna(location) or not isinstance(location, str):
        return None, None

    # Patrón: CITY NAME, ST
    match = re.match(r'^(.+?),\s*([A-Z]{2})$', location.strip())
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


def extract_apex_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrae datos del formato de bloques de APEX

    El archivo APEX tiene un formato de bloques donde:
    - Fila con 'TaxCode' en primera columna marca inicio de bloque
    - La columna ACT contiene el código del tax
    - La tercera columna contiene 'CITY, STATE'
    - Fila 'Total Rate' contiene la tasa total

    Args:
        df: DataFrame crudo del archivo APEX

    Returns:
        DataFrame con columnas: tax_code, city, state, total_rate, city_state_key
    """
    records = []

    current_tax_code = None
    current_location = None
    current_total_rate = None

    # Obtener nombres de columnas
    cols = df.columns.tolist()

    for idx, row in df.iterrows():
        tax_code_col = row.iloc[0]  # Primera columna (TaxCode, State, Total Rate, etc.)

        if tax_code_col == 'TaxCode':
            # Inicio de nuevo bloque
            # Si hay un bloque anterior pendiente, guardarlo
            if current_tax_code is not None and current_total_rate is not None:
                city, state = parse_city_state(current_location)
                if city and state:
                    records.append({
                        'tax_code': current_tax_code,
                        'city': city,
                        'state': state,
                        'total_rate': current_total_rate,
                        'city_state_key': build_city_state_key(city, state)
                    })

            # Nuevo bloque
            current_tax_code = row.iloc[1]  # Columna ACT
            current_location = row.iloc[2]  # Columna con CITY, STATE
            current_total_rate = None

        elif tax_code_col == 'Total Rate':
            # Fin de bloque - capturar la tasa total
            try:
                current_total_rate = float(row.iloc[1])
            except (ValueError, TypeError):
                current_total_rate = None

    # Guardar el último bloque
    if current_tax_code is not None and current_total_rate is not None:
        city, state = parse_city_state(current_location)
        if city and state:
            records.append({
                'tax_code': current_tax_code,
                'city': city,
                'state': state,
                'total_rate': current_total_rate,
                'city_state_key': build_city_state_key(city, state)
            })

    return pd.DataFrame(records)


def transform_apex(file_path: str) -> pd.DataFrame:
    """
    Pipeline completo de ETL para datos APEX

    Args:
        file_path: Ruta al archivo Excel de APEX

    Returns:
        DataFrame limpio y transformado con city_state_key
    """
    # Leer archivo Excel
    df_raw = pd.read_excel(file_path)

    # Extraer datos de los bloques
    df_clean = extract_apex_data(df_raw)

    # Agregar columna de origen
    df_clean['source'] = 'APEX'

    return df_clean


if __name__ == "__main__":
    # Test del módulo
    import os

    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(base_path, "Base de datos", "Tax Code Report-APEX.xlsx")

    df = transform_apex(file_path)
    print(f"Registros extraídos: {len(df)}")
    print(f"\nColumnas: {list(df.columns)}")
    print(f"\nPrimeros 10 registros:")
    print(df.head(10).to_string())
