"""
Reporting.py - Módulo de Generación de Reportes

Este módulo crea el reporte final:
- Muestra el match entre APEX y COMMAND
- Indica si se debe actualizar o no un tax
- Genera reportes en diferentes formatos
"""

import pandas as pd
from datetime import datetime
import os


def generate_summary_report(df_all: pd.DataFrame, df_updates: pd.DataFrame) -> dict:
    """
    Genera un resumen estadístico del proceso

    Args:
        df_all: DataFrame con todos los registros mergeados
        df_updates: DataFrame con registros que requieren actualización

    Returns:
        Diccionario con estadísticas del reporte
    """
    summary = {
        'total_records_processed': len(df_all),
        'records_requiring_update': len(df_updates),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    if len(df_updates) > 0:
        # Estadísticas por plataforma
        platform_counts = df_updates['update_platform'].value_counts().to_dict()
        summary['by_platform'] = platform_counts

        # Estadísticas por tipo de acción
        if 'action_required' in df_updates.columns:
            action_counts = df_updates['action_required'].value_counts().to_dict()
            summary['by_action'] = action_counts

        # Estadísticas por estado
        if 'state' in df_updates.columns:
            state_counts = df_updates['state'].value_counts().to_dict()
            summary['by_state'] = state_counts

    return summary


def format_update_report(df_updates: pd.DataFrame) -> pd.DataFrame:
    """
    Formatea el reporte de actualizaciones para presentación

    Args:
        df_updates: DataFrame con registros que requieren actualización

    Returns:
        DataFrame formateado para el reporte final
    """
    if len(df_updates) == 0:
        return pd.DataFrame()

    # Seleccionar y ordenar columnas para el reporte
    report_columns = [
        'city_state_key',
        'city',
        'state',
        'tax_code_apex',
        'tax_code_command',
        'old_rate',
        'new_rate',
        'rate_change',
        'action_required',
        'effective_date',
        'update_platform'
    ]

    # Filtrar solo columnas que existen
    available_columns = [col for col in report_columns if col in df_updates.columns]

    df_report = df_updates[available_columns].copy()

    # Formatear tasas como porcentaje
    for col in ['old_rate', 'new_rate', 'rate_change']:
        if col in df_report.columns:
            df_report[col] = df_report[col].apply(lambda x: f"{x*100:.2f}%" if pd.notna(x) else "N/A")

    # Ordenar por estado y ciudad
    df_report = df_report.sort_values(['state', 'city'])

    return df_report


def export_to_excel(df_report: pd.DataFrame, summary: dict, output_path: str):
    """
    Exporta el reporte a un archivo Excel con múltiples hojas

    Args:
        df_report: DataFrame con el reporte formateado
        summary: Diccionario con el resumen estadístico
        output_path: Ruta del archivo de salida
    """
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Hoja 1: Reporte de actualizaciones
        if len(df_report) > 0:
            df_report.to_excel(writer, sheet_name='Updates Required', index=False)
        else:
            pd.DataFrame({'Message': ['No updates required']}).to_excel(
                writer, sheet_name='Updates Required', index=False
            )

        # Hoja 2: Resumen
        summary_df = pd.DataFrame([
            {'Metric': 'Total Records Processed', 'Value': summary['total_records_processed']},
            {'Metric': 'Records Requiring Update', 'Value': summary['records_requiring_update']},
            {'Metric': 'Report Generated', 'Value': summary['timestamp']}
        ])

        # Agregar desglose por plataforma
        if 'by_platform' in summary:
            for platform, count in summary['by_platform'].items():
                summary_df = pd.concat([summary_df, pd.DataFrame([
                    {'Metric': f'Platform: {platform}', 'Value': count}
                ])], ignore_index=True)

        # Agregar desglose por acción
        if 'by_action' in summary:
            for action, count in summary['by_action'].items():
                summary_df = pd.concat([summary_df, pd.DataFrame([
                    {'Metric': f'Action: {action}', 'Value': count}
                ])], ignore_index=True)

        summary_df.to_excel(writer, sheet_name='Summary', index=False)

    print(f"Reporte exportado a: {output_path}")


def export_to_csv(df_report: pd.DataFrame, output_path: str):
    """
    Exporta el reporte a un archivo CSV

    Args:
        df_report: DataFrame con el reporte formateado
        output_path: Ruta del archivo de salida
    """
    df_report.to_csv(output_path, index=False)
    print(f"Reporte CSV exportado a: {output_path}")


def print_report(df_report: pd.DataFrame, summary: dict):
    """
    Imprime el reporte en consola

    Args:
        df_report: DataFrame con el reporte formateado
        summary: Diccionario con el resumen estadístico
    """
    print("\n" + "="*80)
    print("REPORTE DE ACTUALIZACIONES DE IMPUESTOS")
    print("="*80)

    print(f"\nFecha de generación: {summary['timestamp']}")
    print(f"Total de registros procesados: {summary['total_records_processed']}")
    print(f"Registros que requieren actualización: {summary['records_requiring_update']}")

    if 'by_platform' in summary:
        print("\nDesglose por plataforma:")
        for platform, count in summary['by_platform'].items():
            print(f"  - {platform}: {count}")

    if 'by_action' in summary:
        print("\nDesglose por acción requerida:")
        for action, count in summary['by_action'].items():
            print(f"  - {action}: {count}")

    if 'by_state' in summary:
        print("\nDesglose por estado (top 10):")
        sorted_states = sorted(summary['by_state'].items(), key=lambda x: x[1], reverse=True)[:10]
        for state, count in sorted_states:
            print(f"  - {state}: {count}")

    if len(df_report) > 0:
        print("\n" + "-"*80)
        print("DETALLE DE ACTUALIZACIONES")
        print("-"*80)
        print(df_report.to_string(index=False))
    else:
        print("\n✓ No se requieren actualizaciones.")

    print("\n" + "="*80)


def generate_report(df_all: pd.DataFrame, df_updates: pd.DataFrame,
                    output_dir: str = None, export_excel: bool = True,
                    export_csv: bool = False, print_console: bool = True) -> dict:
    """
    Pipeline completo de generación de reportes

    Args:
        df_all: DataFrame con todos los registros mergeados
        df_updates: DataFrame con registros que requieren actualización
        output_dir: Directorio de salida para archivos (opcional)
        export_excel: Si True, exporta a Excel
        export_csv: Si True, exporta a CSV
        print_console: Si True, imprime en consola

    Returns:
        Diccionario con el resumen del reporte
    """
    # Generar resumen
    summary = generate_summary_report(df_all, df_updates)

    # Formatear reporte
    df_report = format_update_report(df_updates)

    # Imprimir en consola
    if print_console:
        print_report(df_report, summary)

    # Exportar archivos
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if export_excel:
            excel_path = os.path.join(output_dir, f'tax_update_report_{timestamp}.xlsx')
            export_to_excel(df_report, summary, excel_path)

        if export_csv:
            csv_path = os.path.join(output_dir, f'tax_update_report_{timestamp}.csv')
            export_to_csv(df_report, csv_path)

    return summary


if __name__ == "__main__":
    # Test del módulo con datos de ejemplo
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    from ETL_APEX import transform_apex
    from ETL_COMMAND import transform_command
    from ETL_EDITS import transform_edits
    from MERGING import merge_all

    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Cargar y procesar datos
    print("Cargando y procesando datos...")
    df_apex = transform_apex(os.path.join(base_path, "Base de datos", "Tax Code Report-APEX.xlsx"))
    df_command = transform_command(os.path.join(base_path, "Base de datos", "Tax Code Report-COMMNAD.xlsx"))
    df_edits = transform_edits(os.path.join(base_path, "Base de datos", "Tax Rate Edits.xlsx"))

    # Merge
    df_all, df_updates = merge_all(df_apex, df_command, df_edits)

    # Generar reporte
    output_dir = os.path.join(base_path, "output")
    summary = generate_report(
        df_all, df_updates,
        output_dir=output_dir,
        export_excel=True,
        export_csv=True,
        print_console=True
    )
