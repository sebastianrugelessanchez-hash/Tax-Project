"""
config.py - Configuración centralizada del proyecto

Este módulo contiene todas las configuraciones, rutas y constantes
utilizadas en el sistema ETL de impuestos.
"""

import os
from pathlib import Path

# =============================================================================
# RUTAS DEL PROYECTO
# =============================================================================

# Directorio raíz del proyecto
ROOT_DIR = Path(__file__).parent.parent

# Directorios principales
CODE_DIR = ROOT_DIR / "code_py"
DATA_DIR = ROOT_DIR / "Base de datos"
OUTPUT_DIR = ROOT_DIR / "output"

# =============================================================================
# ARCHIVOS DE ENTRADA
# =============================================================================

# Archivos de la base de datos
FILES = {
    'APEX': DATA_DIR / "Tax Code Report_APEX.xlsx",
    'COMMAND': DATA_DIR / "Tax Code Report-COMMNAD.xlsx",
    'EDITS': DATA_DIR / "Tax Rate Edits.xlsx"
}

# =============================================================================
# CONFIGURACIÓN DE COLUMNAS
# =============================================================================

# Columnas esperadas en cada archivo
APEX_COLUMNS = {
    'tax_code_col': 0,      # Columna con TaxCode, State, Total Rate
    'act_col': 1,           # Columna ACT (código del tax)
    'location_col': 2,      # Columna con CITY, STATE
    'tax_id_col': 3,        # Columna Tax ID
    'percent_col': 4,       # Columna Percent
}

COMMAND_COLUMNS = {
    'tax_code': 'Tax code',
    'description': 'Description',
    'short_description': 'Short description'
}

EDITS_COLUMNS = {
    'state': 'State',
    'jurisdiction_name': 'Jurisdiction Name',
    'old_rate': 'Old Rate',
    'new_rate': 'New Rate',
    'effective_date': 'Effective Date',
    'change_type': 'Change Type',
    'jurisdiction_type': 'Jurisdiction Type'
}

# =============================================================================
# MAPEO DE ESTADOS (Nombre completo -> Código)
# =============================================================================

STATE_CODES = {
    'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
    'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
    'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
    'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
    'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
    'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
    'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
    'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
    'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
    'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
    'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
    'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
    'WISCONSIN': 'WI', 'WYOMING': 'WY', 'DISTRICT OF COLUMBIA': 'DC'
}

# =============================================================================
# CONFIGURACIÓN DE REPORTES
# =============================================================================

REPORT_CONFIG = {
    'export_excel': True,
    'export_csv': True,
    'print_console': True,
    'date_format': '%Y-%m-%d %H:%M:%S',
    'filename_prefix': 'tax_update_report'
}

# =============================================================================
# REGLAS DE NEGOCIO
# =============================================================================

BUSINESS_RULES = {
    # Tipos de cambio a excluir del reporte de actualizaciones
    'excluded_change_types': ['Expired'],

    # Plataformas válidas
    'platforms': ['APEX', 'COMMAND'],

    # Umbral mínimo de cambio de tasa para reportar (0 = reportar todos)
    'min_rate_change_threshold': 0
}

# =============================================================================
# FUNCIONES DE UTILIDAD
# =============================================================================

def ensure_output_dir():
    """Crea el directorio de salida si no existe"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR


def get_file_path(file_key: str) -> Path:
    """
    Obtiene la ruta de un archivo por su clave

    Args:
        file_key: Clave del archivo ('APEX', 'COMMAND', 'EDITS')

    Returns:
        Path al archivo
    """
    if file_key not in FILES:
        raise ValueError(f"Archivo no reconocido: {file_key}. Opciones: {list(FILES.keys())}")
    return FILES[file_key]


def validate_files():
    """
    Valida que todos los archivos de entrada existan

    Returns:
        dict con el estado de cada archivo
    """
    status = {}
    for name, path in FILES.items():
        status[name] = {
            'path': str(path),
            'exists': path.exists(),
            'size': path.stat().st_size if path.exists() else 0
        }
    return status


# =============================================================================
# VALIDACIÓN AL IMPORTAR
# =============================================================================

if __name__ == "__main__":
    print("Configuración del Proyecto Taxes")
    print("=" * 50)
    print(f"ROOT_DIR: {ROOT_DIR}")
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"OUTPUT_DIR: {OUTPUT_DIR}")
    print("\nArchivos:")
    for name, info in validate_files().items():
        status = "✓" if info['exists'] else "✗"
        print(f"  {status} {name}: {info['path']}")
