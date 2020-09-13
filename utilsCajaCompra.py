from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, udf, desc
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType, ArrayType)
from pyspark.sql.functions import sum, round, rank, DataFrame, split, count
from pyspark.sql.window import Window
from pyspark.sql.functions import explode
from pyspark import SparkContext, SparkConf, SQLContext
import os
import sys
import json


conf = (SparkConf()
        .setMaster('local')
        .setAppName('utilsCajaCompra')
        .set('spark.executor.memory', '1g'))
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

spark = SparkSession.builder.appName('utilsCajaCompra').getOrCreate()


# Funcion No. 1
# Obtiene una caja tabulada. Recibe por entrada el path de un .json (Caja) y devuelve un dataframe.(Explotado).
def obt_CajasTabuladas(json_file_path):
    df = spark.read.option('multiline', True).json(json_file_path)

    caja_compras_df = df.select(df.numero_caja, explode(df.compras))

    compras_df = caja_compras_df.select(
        caja_compras_df.numero_caja, explode(caja_compras_df.col))

    comprasN3_df = compras_df.select(compras_df.numero_caja, col('col.*'))

    return comprasN3_df


# Funcion No. 2
# Obtiene una caja tabulada. Recibe por entradael contenido de un archivo json y devuelve un dataframe. (Explotado).
def obt_CajasTabuladasJson(json_file):
    df = spark.read.json(sc.parallelize([json_file]))

    caja_compras_df = df.select(df.numero_caja, explode(df.compras))
    compras_df = caja_compras_df.select(
        caja_compras_df.numero_caja, explode(caja_compras_df.col))
    comprasN3_df = compras_df.select(compras_df.numero_caja, col('col.*'))

    return comprasN3_df


# Funcion No. 3
# Recibe un dataframe y un path y lo guarda como .csv.
def guardar_Archivo(df, filepath):
    df.coalesce(1).write.format('csv').option('header', True).mode(
        'overwrite').save(filepath)
    os.system('mv {}/*.csv {}'.format(filepath, filepath + '.csv'))

    return 1

# Funcion No. 4
# Enunciado: total_productos.csv
# Recibe como entrada un df con todas las cajas y devuelve un df con dos columnas. Nombre de producto y cantidad.


def obt_TotalProducto(df):
    df_resul = df.groupBy('nombre').agg(
        sum(df.cantidad).alias('cantidad_total')).sort('nombre')

    return df_resul

# Funcion No. 5
# Enunciado: total_cajas.csv
# Recibe como entrada un df con todas las cajas y devuelve un df con dos columnas. Nombre de producto y cantidad.


def obt_TotalCajas(df):
    df_resul = df.groupBy('numero_caja').agg(
        sum(df.precio_unitario).alias('total_vendido')).sort('numero_caja')

    return df_resul

# Funcion No. 6
# Enunciado: metricas.csv
# Recibe como entrada un df con todas las cajas y devuelve un df con dos columnas. Metricas de ventas.


def obt_Metricas(total_productos, total_cajas, CajaTotal_df):
    # Caja con más ventas (en dinero)
    caja_con_mas_ventas = total_cajas.sort(
        'total_vendido', ascending=False).head()[0]

    # Caja con menos ventas (en dinero)
    caja_con_menos_ventas = total_cajas.sort(
        'total_vendido', ascending=True).head()[0]

    # Calculo de Percentiles

    percentiles_por_caja = total_cajas.sort('total_vendido', ascending=True).approxQuantile(
        'total_vendido', probabilities=[0.25, 0.5, 0.75], relativeError=1)

    percentil_25_por_caja = percentiles_por_caja[0]

    percentil_50_por_caja = percentiles_por_caja[1]

    percentil_75_por_caja = percentiles_por_caja[2]

    # Producto mas vendido
    producto_mas_vendido_por_unidad = total_productos.sort(
        'cantidad_total', ascending=False).head()[0]

    # Precios unitarios
    df_unitario = CajaTotal_df.select('nombre', 'precio_unitario').distinct()

    # Producto de mayor ingreso
    producto_de_mayor_ingreso = df_unitario.join(total_productos, on='nombre').select(df_unitario['nombre'], (col(
        'precio_unitario') * col('cantidad_total')).alias('ingreso')).sort('ingreso', ascending=False).head()[0]

    metricas_data = [('caja_con_mas_ventas',
                      caja_con_mas_ventas),
                     ('caja_con_menos_ventas',
                      caja_con_menos_ventas),
                     ('percentil_25_por_caja',
                      percentil_25_por_caja),
                     ('percentil_50_por_caja',
                      percentil_50_por_caja),
                     ('percentil_75_por_caja',
                      percentil_75_por_caja),
                     ('producto_mas_vendido_por_unidad',
                      producto_mas_vendido_por_unidad),
                     ('producto_de_mayor_ingreso',
                      producto_de_mayor_ingreso)]

    schema = StructType([
        StructField('metrica', StringType(), True),
        StructField('valor', StringType(), True)
    ])

    df_metricas = spark.createDataFrame(metricas_data, schema)

    return df_metricas


if __name__ == '__main__':
    spark.sparkContext.setLogLevel('WARN')

    schema = StructType([
        StructField('numero_caja', IntegerType(), True),
        StructField('cantidad', IntegerType(), True),
        StructField('nombre', StringType(), True),
        StructField('precio_unitario', FloatType(), True)
    ])
    CajaTotal_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    if len(sys.argv) < 2:
        print('Faltan los parametros de los archivos JSON.')
        sys.exit()

    for x in range(len(sys.argv)-1):
        print('Archivo: ', x)
        with open(sys.argv[x+1]) as json_data:
            data = json.load(json_data)
            Caja_df = obt_CajasTabuladasJson(data)
            CajaTotal_df = CajaTotal_df.union(Caja_df)

    total_productos = obt_TotalProducto(CajaTotal_df)
    total_cajas = obt_TotalCajas(CajaTotal_df)
    metricas = obt_Metricas(total_productos, total_cajas, CajaTotal_df)

    # total_productos.show()
    # total_cajas.show()
    # metricas.show()

    guardar_Archivo(total_productos, 'totalproductos')
    guardar_Archivo(total_cajas, 'totalcajas')
    guardar_Archivo(metricas, 'metricas')
