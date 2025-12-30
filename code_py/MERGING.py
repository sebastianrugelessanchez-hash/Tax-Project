"""
MERGING.py - Capa de Lógica de Negocio

Este módulo realiza:
1) OUTER JOIN APEX vs COMMAND → determina update_platform
2) INNER JOIN con EDITS → trae old_rate / new_rate
3) Mantiene SOLO registros de actualización (basado en regla de negocio)
"""

import pandas as pd
from typing import Tuple


def outer_join_apex_command(df_apex: pd.DataFrame, df_command: pd.DataFrame) -> pd.DataFrame:
    """
    Paso 1: OUTER JOIN entre APEX y COMMAND

    Determina qué plataforma necesita actualización:
    - Si está en APEX pero no en COMMAND: update_platform = 'COMMAND'
    - Si está en COMMAND pero no en APEX: update_platform = 'APEX'
    - Si está en ambos: update_platform = 'BOTH' o None según la lógica

    Args:
        df_apex: DataFrame de APEX con city_state_key
        df_command: DataFrame de COMMAND con city_state_key

    Returns:
        DataFrame con columna update_platform indicando qué plataforma actualizar
    """
    # Realizar OUTER JOIN usando city_state_key
    df_merged = pd.merge(
        df_apex,
        df_command,
        on='city_state_key',
        how='outer',
        suffixes=('_apex', '_command'),
        indicator=True
    )

    # Determinar qué plataforma necesita actualización
    def determine_update_platform(row):
        if row['_merge'] == 'left_only':
            # Solo en APEX, necesita agregarse a COMMAND
            return 'ADD_TO_COMMAND'
        elif row['_merge'] == 'right_only':
            # Solo en COMMAND, necesita agregarse a APEX
            return 'ADD_TO_APEX'
        else:
            # En ambos, puede necesitar actualización de tasas
            return 'BOTH'

    df_merged['update_platform'] = df_merged.apply(determine_update_platform, axis=1)

    # Consolidar columnas city y state
    df_merged['city'] = df_merged['city_apex'].fillna(df_merged['city_command'])
    df_merged['state'] = df_merged['state_apex'].fillna(df_merged['state_command'])

    # Limpiar columnas duplicadas
    cols_to_keep = [
        'city_state_key', 'city', 'state',
        'tax_code_apex', 'total_rate',
        'tax_code_command', 'description', 'short_description',
        'update_platform', '_merge'
    ]

    # Filtrar solo columnas que existen
    cols_to_keep = [col for col in cols_to_keep if col in df_merged.columns]

    return df_merged[cols_to_keep]


def inner_join_with_edits(df_platforms: pd.DataFrame, df_edits: pd.DataFrame) -> pd.DataFrame:
    """
    Paso 2: INNER JOIN con EDITS

    Trae old_rate y new_rate del archivo de ediciones

    Args:
        df_platforms: DataFrame resultado del outer join APEX/COMMAND
        df_edits: DataFrame de EDITS con tasas old/new

    Returns:
        DataFrame con información de tasas agregada
    """
    # Realizar INNER JOIN usando city_state_key
    df_merged = pd.merge(
        df_platforms,
        df_edits[['city_state_key', 'jurisdiction', 'old_rate', 'new_rate',
                  'rate_change', 'effective_date', 'change_type', 'jurisdiction_type']],
        on='city_state_key',
        how='inner'
    )

    return df_merged


def filter_update_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Paso 3: Mantener SOLO registros que requieren actualización

    Reglas de negocio:
    - Registros donde hay un cambio de tasa (rate_change != 0)
    - Registros donde la plataforma necesita agregarse
    - Filtrar registros expirados (change_type == 'Expired')

    Args:
        df: DataFrame con toda la información merged

    Returns:
        DataFrame filtrado solo con registros que requieren acción
    """
    # Filtro 1: Cambios de tasa (no incluir expired que son removals)
    rate_changes = df[
        (df['rate_change'] != 0) &
        (df['change_type'] != 'Expired')
    ].copy()

    # Agregar columna de acción requerida
    def determine_action(row):
        if row['update_platform'] == 'ADD_TO_COMMAND':
            return 'Agregar a COMMAND'
        elif row['update_platform'] == 'ADD_TO_APEX':
            return 'Agregar a APEX'
        elif row['rate_change'] > 0:
            return 'Incremento de tasa'
        elif row['rate_change'] < 0:
            return 'Decremento de tasa'
        else:
            return 'Sin cambio'

    rate_changes['action_required'] = rate_changes.apply(determine_action, axis=1)

    # Filtrar solo donde hay acción requerida
    update_records = rate_changes[rate_changes['action_required'] != 'Sin cambio']

    return update_records


def merge_all(df_apex: pd.DataFrame, df_command: pd.DataFrame, df_edits: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Pipeline completo de merging

    Args:
        df_apex: DataFrame limpio de APEX
        df_command: DataFrame limpio de COMMAND
        df_edits: DataFrame limpio de EDITS

    Returns:
        Tupla (df_all_merged, df_updates_only)
        - df_all_merged: Todos los registros mergeados
        - df_updates_only: Solo registros que requieren actualización
    """
    # Paso 1: OUTER JOIN APEX vs COMMAND
    print("Paso 1: OUTER JOIN APEX vs COMMAND...")
    df_platforms = outer_join_apex_command(df_apex, df_command)
    print(f"  - Registros después de OUTER JOIN: {len(df_platforms)}")

    # Paso 2: INNER JOIN con EDITS
    print("\nPaso 2: INNER JOIN con EDITS...")
    df_all_merged = inner_join_with_edits(df_platforms, df_edits)
    print(f"  - Registros después de INNER JOIN: {len(df_all_merged)}")

    # Paso 3: Filtrar solo registros de actualización
    print("\nPaso 3: Filtrando registros que requieren actualización...")
    df_updates_only = filter_update_records(df_all_merged)
    print(f"  - Registros que requieren actualización: {len(df_updates_only)}")

    return df_all_merged, df_updates_only


if __name__ == "__main__":
    # Test del módulo
    import os
    import sys

    # Agregar el directorio src al path
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    from ETL_APEX import transform_apex
    from ETL_COMMAND import transform_command
    from ETL_EDITS import transform_edits

    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Cargar datos
    print("Cargando datos...")
    df_apex = transform_apex(os.path.join(base_path, "Base de datos", "Tax Code Report-APEX.xlsx"))
    df_command = transform_command(os.path.join(base_path, "Base de datos", "Tax Code Report-COMMNAD.xlsx"))
    df_edits = transform_edits(os.path.join(base_path, "Base de datos", "Tax Rate Edits.xlsx"))

    print(f"\nAPEX: {len(df_apex)} registros")
    print(f"COMMAND: {len(df_command)} registros")
    print(f"EDITS: {len(df_edits)} registros")

    # Ejecutar merge
    print("\n" + "="*50)
    print("EJECUTANDO MERGE")
    print("="*50)

    df_all, df_updates = merge_all(df_apex, df_command, df_edits)

    print("\n" + "="*50)
    print("RESULTADOS")
    print("="*50)

    if len(df_updates) > 0:
        print("\nRegistros que requieren actualización:")
        print(df_updates.to_string())
    else:
        print("\nNo se encontraron registros que requieran actualización.")
