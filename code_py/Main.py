"""
Main.py - Punto de entrada principal del sistema ETL de Impuestos

Este módulo orquesta todo el pipeline:
1. Lee los documentos de la base de datos
2. Ejecuta los módulos ETL (APEX, COMMAND, EDITS)
3. Ejecuta la lógica de negocio (MERGING)
4. Genera los reportes finales (Reporting)
"""

from datetime import datetime

# Importar configuración centralizada
from config import (
    FILES, OUTPUT_DIR, REPORT_CONFIG,
    ensure_output_dir, validate_files
)

# Importar módulos del proyecto
from ETL_APEX import transform_apex
from ETL_COMMAND import transform_command
from ETL_EDITS import transform_edits
from MERGING import merge_all
from Reporting import generate_report


def load_documents():
    """
    Carga todos los documentos de la base de datos

    Returns:
        Tupla con las rutas de archivos (apex, command, edits)
    """
    print("\n" + "="*60)
    print("CARGANDO DOCUMENTOS")
    print("="*60)

    # Verificar que los archivos existen usando config
    file_status = validate_files()

    for name, info in file_status.items():
        if info['exists']:
            print(f"  ✓ {name}: {info['path'].split('/')[-1]}")
        else:
            print(f"  ✗ {name}: ARCHIVO NO ENCONTRADO - {info['path']}")
            raise FileNotFoundError(f"No se encontró el archivo: {info['path']}")

    return FILES['APEX'], FILES['COMMAND'], FILES['EDITS']


def run_etl(apex_path: str, command_path: str, edits_path: str):
    """
    Ejecuta el pipeline ETL para todos los archivos

    Args:
        apex_path: Ruta al archivo APEX
        command_path: Ruta al archivo COMMAND
        edits_path: Ruta al archivo EDITS

    Returns:
        Tupla con los DataFrames transformados (df_apex, df_command, df_edits)
    """
    print("\n" + "="*60)
    print("EJECUTANDO ETL")
    print("="*60)

    # ETL APEX
    print("\n[1/3] Procesando APEX...")
    df_apex = transform_apex(apex_path)
    print(f"      → {len(df_apex)} registros extraídos")

    # ETL COMMAND
    print("\n[2/3] Procesando COMMAND...")
    df_command = transform_command(command_path)
    print(f"      → {len(df_command)} registros extraídos")

    # ETL EDITS
    print("\n[3/3] Procesando EDITS...")
    df_edits = transform_edits(edits_path)
    print(f"      → {len(df_edits)} registros extraídos")

    return df_apex, df_command, df_edits


def run_merge(df_apex, df_command, df_edits):
    """
    Ejecuta la lógica de negocio (merging)

    Args:
        df_apex: DataFrame de APEX
        df_command: DataFrame de COMMAND
        df_edits: DataFrame de EDITS

    Returns:
        Tupla (df_all_merged, df_updates_only)
    """
    print("\n" + "="*60)
    print("EJECUTANDO MERGE (LÓGICA DE NEGOCIO)")
    print("="*60)

    df_all, df_updates = merge_all(df_apex, df_command, df_edits)

    return df_all, df_updates


def run_reporting(df_all, df_updates):
    """
    Genera los reportes finales

    Args:
        df_all: DataFrame con todos los registros
        df_updates: DataFrame con registros que requieren actualización

    Returns:
        Diccionario con el resumen del reporte
    """
    print("\n" + "="*60)
    print("GENERANDO REPORTES")
    print("="*60)

    # Crear directorio de salida si no existe
    output_dir = ensure_output_dir()

    summary = generate_report(
        df_all,
        df_updates,
        output_dir=str(output_dir),
        export_excel=REPORT_CONFIG['export_excel'],
        export_csv=REPORT_CONFIG['export_csv'],
        print_console=REPORT_CONFIG['print_console']
    )

    return summary


def main():
    """
    Función principal - Ejecuta todo el pipeline
    """
    start_time = datetime.now()

    print("\n" + "#"*60)
    print("#" + " "*58 + "#")
    print("#" + "   SISTEMA ETL DE IMPUESTOS - PROJECT TAXES".center(58) + "#")
    print("#" + " "*58 + "#")
    print("#"*60)
    print(f"\nInicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Paso 1: Cargar documentos
        apex_path, command_path, edits_path = load_documents()

        # Paso 2: Ejecutar ETL
        df_apex, df_command, df_edits = run_etl(apex_path, command_path, edits_path)

        # Paso 3: Ejecutar Merge
        df_all, df_updates = run_merge(df_apex, df_command, df_edits)

        # Paso 4: Generar Reportes
        summary = run_reporting(df_all, df_updates)

        # Resumen final
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("\n" + "#"*60)
        print("PROCESO COMPLETADO EXITOSAMENTE")
        print("#"*60)
        print(f"\nDuración total: {duration:.2f} segundos")
        print(f"Archivos de salida en: {OUTPUT_DIR}")

        return summary

    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        raise


if __name__ == "__main__":
    main()
