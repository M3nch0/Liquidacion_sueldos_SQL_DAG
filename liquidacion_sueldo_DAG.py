"""
DAG: Liquidacion de sueldos con SQL Server (contenedor del compose).

    wait_for_file -> crear_base -> load_empleados -> build_liquidacion -> build_reporte
                  -> validate -> [export_liquidacion, export_reporte]

Requisitos en Airflow:
  - Conexion  mssql_default  (tipo Microsoft SQL Server: host sqlserver, schema TestDB, login sa)
  - Conexion  fs_default     (la del FileSensor)
  - Variables fecha_liquidacion y pct_aportes (opcionales: si no existen se usa 2026-10-31 y 17)
"""
import os

import pandas as pd
import pendulum
from airflow.sdk import dag, task, Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

CONN_ID = 'mssql_default'
DATABASE = 'TestDB'
EMPLEADOS_CSV = '/opt/airflow/carpeta1/empleados.csv'
CARPETA_SALIDA = '/opt/airflow/carpeta2'

# Parametros: se leen de las Variables de Airflow (con valor por defecto) mediante templates de Jinja
FECHA_SQL = "CAST('{{ var.value.get('fecha_liquidacion', '2026-10-31') }}' AS DATE)"
PCT_SQL = "CAST({{ var.value.get('pct_aportes', 17) }} AS DECIMAL(5,2))"


# ============================================================
# TRANSFORM - Pasos SQL (T-SQL). Cada uno usa lo que calculo el anterior.
# ============================================================
SQL_LIQUIDACION = [
    "DROP TABLE IF EXISTS liquidacion;",

    # Paso 1: copia limpia. Las columnas con CAST(NULL AS tipo) nacen vacias, con su tipo.
    """
    SELECT id_empleado,
           LTRIM(RTRIM(nombre)) AS nombre,
           UPPER(LTRIM(RTRIM(departamento))) AS departamento,
           CAST(NULL AS VARCHAR(150)) AS email,
           COALESCE(evaluacion, 3) AS evaluacion,
           CAST(salario_base AS DECIMAL(12,2)) AS salario_base,
           horas_extra,
           DATEDIFF(YEAR, fecha_ingreso, __FECHA__)
             - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, fecha_ingreso, __FECHA__), fecha_ingreso)
                         > __FECHA__
                    THEN 1 ELSE 0 END AS antiguedad_anios,
           CAST(NULL AS DECIMAL(12,2)) AS bono_antiguedad,
           CAST(NULL AS DECIMAL(12,2)) AS bono_evaluacion,
           CAST(NULL AS DECIMAL(12,2)) AS pago_horas_extra,
           CAST(NULL AS DECIMAL(13,2)) AS salario_bruto,
           CAST(NULL AS DECIMAL(12,2)) AS aportes,
           CAST(NULL AS DECIMAL(13,2)) AS salario_neto,
           CAST(NULL AS INT) AS cuartil_neto
    INTO liquidacion
    FROM stg_Empleados;
    """,

    # Paso 2a: email (los nombres tienen 2 palabras)
    """
    UPDATE liquidacion
    SET email = LOWER(REPLACE(nombre, ' ', '.')) + '@empresa.com';
    """,

    # Paso 2b: nombre con la inicial de cada palabra en mayuscula
    """
    UPDATE liquidacion
    SET nombre = UPPER(LEFT(nombre, 1))
               + LOWER(SUBSTRING(nombre, 2, CHARINDEX(' ', nombre) - 1))
               + UPPER(SUBSTRING(nombre, CHARINDEX(' ', nombre) + 1, 1))
               + LOWER(SUBSTRING(nombre, CHARINDEX(' ', nombre) + 2, 100));
    """,

    # Paso 3: bonos y horas extra
    """
    UPDATE liquidacion
    SET bono_antiguedad  = ROUND(salario_base *
                             CASE WHEN antiguedad_anios * 2 > 20 THEN 20 ELSE antiguedad_anios * 2 END
                             / 100.0, 2),
        bono_evaluacion  = ROUND(salario_base *
                             CASE evaluacion WHEN 5 THEN 15 WHEN 4 THEN 10 WHEN 3 THEN 5 ELSE 0 END
                             / 100.0, 2),
        pago_horas_extra = ROUND(horas_extra * (salario_base / 160.0) * 1.5, 2);
    """,

    # Paso 4: bruto, aportes y neto (uno por vez, cada uno usa el anterior)
    """
    UPDATE liquidacion
    SET salario_bruto = salario_base + bono_antiguedad + bono_evaluacion + pago_horas_extra;
    """,
    """
    UPDATE liquidacion
    SET aportes = ROUND(salario_bruto * __PCT__ / 100.0, 2);
    """,
    """
    UPDATE liquidacion
    SET salario_neto = salario_bruto - aportes;
    """,

    # Paso 5: cuartil segun el neto
    """
    UPDATE l
    SET cuartil_neto = q.cuartil
    FROM liquidacion l
    JOIN (SELECT id_empleado, NTILE(4) OVER (ORDER BY salario_neto) AS cuartil
          FROM liquidacion) q
      ON q.id_empleado = l.id_empleado;
    """,
]

SQL_REPORTE = [
    "DROP TABLE IF EXISTS reporte_departamentos;",

    # Paso 6: reporte por departamento
    """
    SELECT l.departamento,
           COUNT(*) AS cantidad_empleados,
           SUM(l.salario_bruto) AS masa_salarial_bruta,
           CAST(ROUND(AVG(l.salario_neto), 2) AS DECIMAL(12,2)) AS neto_promedio,
           CAST(ROUND(SUM(l.salario_bruto) * 100.0 /
                      (SELECT SUM(salario_bruto) FROM liquidacion), 1) AS DECIMAL(5,1)) AS participacion_pct,
           MAX(l.salario_neto) AS mayor_neto,
           (SELECT TOP 1 x.nombre
            FROM liquidacion x
            WHERE x.departamento = l.departamento
            ORDER BY x.salario_neto DESC) AS empleado_mayor_neto
    INTO reporte_departamentos
    FROM liquidacion l
    GROUP BY l.departamento;
    """,
]

# Se completan los parametros (que son templates de Jinja) en cada sentencia
SQL_LIQUIDACION = [s.replace('__FECHA__', FECHA_SQL).replace('__PCT__', PCT_SQL) for s in SQL_LIQUIDACION]


# ============================================================
# EXTRACT / LOAD - funciones de apoyo
# ============================================================
def leer_empleados_csv(file_path):
    """Lee el CSV sin limpiar nada (todo como texto). Detecta si usa coma o punto y coma."""
    with open(file_path, encoding='utf-8-sig') as f:
        primera_linea = f.readline()
    separador = ';' if primera_linea.count(';') > primera_linea.count(',') else ','
    df = pd.read_csv(file_path, sep=separador, dtype=str, encoding='utf-8-sig')
    df.columns = df.columns.str.strip()
    return df


def a_entero(valor):
    """Convierte a int; si el valor esta vacio devuelve None (NULL en SQL)."""
    return None if pd.isna(valor) else int(valor)


def a_texto(valor):
    return None if pd.isna(valor) else str(valor)


def armar_filas(df):
    """Convierte el DataFrame en una lista de tuplas con los tipos que espera stg_Empleados."""
    df = df.copy()
    df['fecha_ingreso'] = pd.to_datetime(df['fecha_ingreso'], errors='coerce')
    filas = []
    for _, fila in df.iterrows():
        fecha = None if pd.isna(fila['fecha_ingreso']) else fila['fecha_ingreso'].date()
        filas.append((
            a_entero(fila['id_empleado']),
            a_texto(fila['nombre']),
            a_texto(fila['departamento']),
            fecha,
            a_entero(fila['salario_base']),
            a_entero(fila['evaluacion']),      # vacio -> NULL
            a_entero(fila['horas_extra']),
        ))
    return filas


def exportar_consulta(consulta, ruta):
    """Ejecuta la consulta en SQL Server y guarda el resultado como CSV."""
    conn = MsSqlHook(mssql_conn_id=CONN_ID).get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(consulta)
        columnas = [c[0] for c in cursor.description]
        filas = cursor.fetchall()
    finally:
        conn.close()
    os.makedirs(CARPETA_SALIDA, exist_ok=True)
    pd.DataFrame(filas, columns=columnas).to_csv(ruta, index=False, encoding='utf-8-sig')
    print(f'Exportado: {ruta} ({len(filas)} filas)')


# ============================================================
# EL DAG
# ============================================================
@dag(
    dag_id='liquidacion_sueldos_sqlserver',
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Argentina/Buenos_Aires"),
    schedule=None,   # solo corre cuando lo disparas a mano
    catchup=False,
)
def liquidacion_sueldos_sqlserver():

    # 1) Esperar a que llegue empleados.csv a carpeta1
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=EMPLEADOS_CSV,
        poke_interval=10,
        timeout=60,
    )

    # 2) Crear la base TestDB si no existe (hay que conectarse a master, y CREATE DATABASE pide autocommit)
    @task()
    def crear_base():
        hook = MsSqlHook(mssql_conn_id=CONN_ID, schema='master')
        hook.run(f"IF DB_ID('{DATABASE}') IS NULL CREATE DATABASE {DATABASE};", autocommit=True)
        print(f"Base '{DATABASE}' lista")

    # 3) Cargar el CSV tal cual en stg_Empleados; la cantidad de filas viaja por XCom
    @task()
    def load_empleados():
        filas = armar_filas(leer_empleados_csv(EMPLEADOS_CSV))
        hook = MsSqlHook(mssql_conn_id=CONN_ID)
        hook.run([
            "DROP TABLE IF EXISTS stg_Empleados;",
            """CREATE TABLE stg_Empleados (
                   id_empleado INT PRIMARY KEY,
                   nombre VARCHAR(100),
                   departamento VARCHAR(100),
                   fecha_ingreso DATE,
                   salario_base INT,
                   evaluacion INT,
                   horas_extra INT
               );""",
        ], autocommit=True)
        hook.insert_rows(
            table='stg_Empleados',
            rows=filas,
            target_fields=['id_empleado', 'nombre', 'departamento', 'fecha_ingreso',
                           'salario_base', 'evaluacion', 'horas_extra'],
        )
        print(f'{len(filas)} filas cargadas en stg_Empleados')
        return len(filas)

    # 4) Transformaciones en SQL
    build_liquidacion = SQLExecuteQueryOperator(
        task_id='build_liquidacion',
        conn_id=CONN_ID,
        autocommit=True,
        sql=SQL_LIQUIDACION,
    )

    build_reporte = SQLExecuteQueryOperator(
        task_id='build_reporte',
        conn_id=CONN_ID,
        autocommit=True,
        sql=SQL_REPORTE,
    )

    # 5) Validacion: compara con el XCom de la carga y busca netos invalidos
    @task()
    def validate(filas_csv):
        hook = MsSqlHook(mssql_conn_id=CONN_ID)
        filas_db = hook.get_first("SELECT COUNT(*) FROM liquidacion")[0]
        invalidos = hook.get_first(
            "SELECT COUNT(*) FROM liquidacion WHERE salario_neto IS NULL OR salario_neto <= 0"
        )[0]
        print(f'Filas en el CSV: {filas_csv} | Filas en liquidacion: {filas_db} | Netos invalidos: {invalidos}')
        if filas_db != filas_csv:
            raise ValueError('La cantidad de filas de liquidacion no coincide con la del CSV')
        if invalidos > 0:
            raise ValueError('Hay salarios netos nulos o menores o iguales a 0')

    # 6) Exportaciones a carpeta2 (en paralelo)
    @task()
    def export_liquidacion():
        fecha = Variable.get('fecha_liquidacion', default='2026-10-31')
        exportar_consulta(
            "SELECT * FROM liquidacion ORDER BY id_empleado",
            os.path.join(CARPETA_SALIDA, f'liquidacion_{fecha}.csv'),
        )

    @task()
    def export_reporte():
        exportar_consulta(
            "SELECT * FROM reporte_departamentos ORDER BY departamento",
            os.path.join(CARPETA_SALIDA, 'reporte_departamentos.csv'),
        )

    filas = load_empleados()
    validado = validate(filas)   # usa el XCom de la carga

    wait_for_file >> crear_base() >> filas >> build_liquidacion >> build_reporte >> validado >> [export_liquidacion(), export_reporte()]


liquidacion_sueldos_sqlserver()