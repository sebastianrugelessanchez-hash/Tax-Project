"""
ETL_EDITS.py - Módulo de extracción y transformación para Tax Rate Edits

Este módulo realiza:
- Extracción de city/state desde Jurisdiction Name
- Extracción de old_rate y new_rate
- Construcción de city_state_key para el merge
"""

import pandas as pd
import re
from typing import Tuple

from config import STATE_CODES, EDITS_COLUMNS


def parse_jurisdiction_name(jurisdiction: str) -> Tuple[str, str]:
    """
    Extrae el nombre de la ciudad desde Jurisdiction Name

    El formato es típicamente: 'City Name (City)' o 'City Name (County)'

    Args:
        jurisdiction: String con el nombre de la jurisdicción

    Returns:
        Tupla (city_name, jurisdiction_type)
    """
    if pd.isna(jurisdiction) or not isinstance(jurisdiction, str):
        return None, None

    # Patrón: Name (Type) - ej: 'Gilbert (City)', 'Hamilton (County)'
    match = re.match(r'^(.+?)\s*\((\w+)\)$', jurisdiction.strip())
    if match:
        name = match.group(1).strip().upper()
        jtype = match.group(2).strip()
        return name, jtype

    # Si no tiene paréntesis, intentar limpiar el nombre
    # Algunos tienen formato: 'City Name Something (District)'
    match2 = re.match(r'^(.+?)(?:\s+\(.+\))?$', jurisdiction.strip())
    if match2:
        name = match2.group(1).strip().upper()
        # Limpiar nombres que terminan con palabras como 'Transactions', 'Tax', etc.
        name = re.sub(r'\s+(Transactions|Tax|Regional|Metropolitan|District).*$', '', name, flags=re.IGNORECASE)
        return name.upper(), None

    return jurisdiction.strip().upper(), None


def build_city_state_key(city: str, state: str) -> str:
    """
    Construye una clave única para city/state

    Args:
        city: Nombre de la ciudad/jurisdicción
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


def extract_edits_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrae datos relevantes del archivo Tax Rate Edits

    Columnas importantes:
    - State: Estado (ej: 'Texas', 'Colorado')
    - Jurisdiction Name: Nombre de la jurisdicción (ej: 'Gilbert (City)')
    - Old Rate: Tasa anterior
    - New Rate: Tasa nueva
    - Effective Date: Fecha efectiva
    - Change Type: Tipo de cambio (Added, Expired, etc.)

    Args:
        df: DataFrame crudo del archivo EDITS

    Returns:
        DataFrame con columnas: jurisdiction, state, old_rate, new_rate, city_state_key
    """
    records = []

    for idx, row in df.iterrows():
        state_name = row.get(EDITS_COLUMNS['state'], '')
        jurisdiction = row.get(EDITS_COLUMNS['jurisdiction_name'], '')
        old_rate = row.get(EDITS_COLUMNS['old_rate'], None)
        new_rate = row.get(EDITS_COLUMNS['new_rate'], None)
        effective_date = row.get(EDITS_COLUMNS['effective_date'], None)
        change_type = row.get(EDITS_COLUMNS['change_type'], '')
        jurisdiction_type = row.get(EDITS_COLUMNS['jurisdiction_type'], '')

        # Convertir nombre de estado a código
        if pd.isna(state_name) or not isinstance(state_name, str):
            continue

        state_code = STATE_CODES.get(state_name.strip().upper(), None)
        if not state_code:
            continue

        # Extraer nombre de la jurisdicción
        city_name, jtype = parse_jurisdiction_name(jurisdiction)
        if not city_name:
            continue

        # Convertir tasas a float
        try:
            old_rate_val = float(old_rate) if pd.notna(old_rate) else 0.0
            new_rate_val = float(new_rate) if pd.notna(new_rate) else 0.0
        except (ValueError, TypeError):
            old_rate_val = 0.0
            new_rate_val = 0.0

        records.append({
            'jurisdiction': city_name,
            'state': state_code,
            'state_name': state_name,
            'old_rate': old_rate_val,
            'new_rate': new_rate_val,
            'rate_change': new_rate_val - old_rate_val,
            'effective_date': effective_date,
            'change_type': change_type,
            'jurisdiction_type': jurisdiction_type,
            'city_state_key': build_city_state_key(city_name, state_code)
        })

    return pd.DataFrame(records)


def transform_edits(file_path: str) -> pd.DataFrame:
    """
    Pipeline completo de ETL para Tax Rate Edits

    Args:
        file_path: Ruta al archivo Excel de EDITS

    Returns:
        DataFrame limpio y transformado con city_state_key
    """
    # Leer archivo Excel
    df_raw = pd.read_excel(file_path)

    # Extraer datos
    df_clean = extract_edits_data(df_raw)

    # Agregar columna de origen
    df_clean['source'] = 'EDITS'

    return df_clean


if __name__ == "__main__":
    # Test del módulo
    from config import FILES

    df = transform_edits(FILES['EDITS'])
    print(f"Registros extraídos: {len(df)}")
    print(f"\nColumnas: {list(df.columns)}")
    print(f"\nPrimeros 10 registros:")
    print(df.head(10).to_string())
    print(f"\nEstados únicos: {df['state'].unique()}")
