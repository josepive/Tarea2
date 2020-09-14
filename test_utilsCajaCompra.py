from .utilsCajaCompra import obt_CajasTabuladas, obt_CajasTabuladasJson, guardar_Archivo, obt_TotalProducto, obt_TotalCajas, obt_Metricas
from pyspark.sql.types import StructType,StructField, StringType


def test_CajasTabuladas(spark_session):

    # Definicion de datos actuales.

    caja21 = '{"numero_caja": 21,"compras": [[{"nombre": "manzana","cantidad": 3 ,"precio_unitario": 20},{"nombre": "brocoli","cantidad": 2 ,"precio_unitario": 33}],[{"nombre": "aguacate","cantidad": 1 ,"precio_unitario": 5000}]]}'

    actual_ds = obt_CajasTabuladasJson(caja21)

    # Definicion de datos esperados.
    expected_ds = spark_session.createDataFrame(
        [(21, 3, 'manzana', 20),
         (21, 2, 'brocoli', 33),
         (21, 1, 'aguacate', 5000)],
        ['numero_caja', 'cantidad', 'nombre', 'precio_unitario'])

    print('Actual: ')
    actual_ds.show(30)
    print('Esperado: ')
    expected_ds.show(30)

    assert actual_ds.collect() == expected_ds.collect()


def test_obtTotalProducto(spark_session):

    # Definicion de datos actuales.
    caja21 = '{"numero_caja": 21,"compras": [[{"nombre": "manzana","cantidad": 3 ,"precio_unitario": 20},{"nombre": "brocoli","cantidad": 2 ,"precio_unitario": 33}],[{"nombre": "aguacate","cantidad": 1 ,"precio_unitario": 5000}]]}'
    caja22 = '{"numero_caja": 22,"compras": [[{"nombre": "manzana","cantidad": 1 ,"precio_unitario": 20},{"nombre": "brocoli","cantidad": 12 ,"precio_unitario": 33}],[{"nombre": "pan","cantidad": 10 ,"precio_unitario": 800}]]}'

    # obt_TotalProducto
    Caja21_df = obt_CajasTabuladasJson(caja21)
    Caja22_df = obt_CajasTabuladasJson(caja22)
    actual_ds = Caja21_df.union(Caja22_df)

    actual_ds = obt_TotalProducto(actual_ds)

    # Definicion de datos esperados.
    expected_ds = spark_session.createDataFrame(
        [('aguacate', 1),
         ('brocoli', 14),
         ('manzana', 4),
         ('pan', 10)],
        ['nombre', 'cantidad_total'])

    print('Actual: ')
    actual_ds.show()
    print('Esperado: ')
    expected_ds.show()


    assert actual_ds.collect() == expected_ds.collect()

def test_obtTotalCajas(spark_session):

    # Definicion de datos actuales.
    caja21 = '{"numero_caja": 21,"compras": [[{"nombre": "manzana","cantidad": 3 ,"precio_unitario": 20},{"nombre": "brocoli","cantidad": 2 ,"precio_unitario": 33}],[{"nombre": "aguacate","cantidad": 1 ,"precio_unitario": 5000}]]}'
    caja22 = '{"numero_caja": 22,"compras": [[{"nombre": "manzana","cantidad": 1 ,"precio_unitario": 20},{"nombre": "brocoli","cantidad": 12 ,"precio_unitario": 33}],[{"nombre": "pan","cantidad": 10 ,"precio_unitario": 800}]]}'

    # obt_TotalCajas
    Caja21_df = obt_CajasTabuladasJson(caja21)
    Caja22_df = obt_CajasTabuladasJson(caja22)
    actual_ds = Caja21_df.union(Caja22_df)

    actual_ds = obt_TotalCajas(actual_ds)

    # Definicion de datos esperados.
    expected_ds = spark_session.createDataFrame(
        [(21, 5053),(22, 853)],
        ['numero_caja', 'total_vendido'])

    print('Actual: ')
    actual_ds.show()
    print('Esperado: ')
    expected_ds.show()


    assert actual_ds.collect() == expected_ds.collect()    

def test_obtMetricas(spark_session):

    schema = StructType([StructField('metrica', StringType(), True),StructField('valor', StringType(), True)])

    expected_ds = spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(),schema)

    caja21 = '{"numero_caja": 21,"compras": [[{"nombre": "manzana","cantidad": 3 ,"precio_unitario": 20},{"nombre": "brocoli","cantidad": 2 ,"precio_unitario": 33}],[{"nombre": "aguacate","cantidad": 1 ,"precio_unitario": 5000}]]}'
    caja22 = '{"numero_caja": 22,"compras": [[{"nombre": "manzana","cantidad": 1 ,"precio_unitario": 20},{"nombre": "brocoli","cantidad": 12 ,"precio_unitario": 33}],[{"nombre": "pan","cantidad": 10 ,"precio_unitario": 800}]]}'

    Caja21_df = obt_CajasTabuladasJson(caja21)
    Caja22_df = obt_CajasTabuladasJson(caja22)
    caja_completa_df = Caja21_df.union(Caja22_df)

    actual_ds = Caja21_df.union(Caja22_df)
    total_producto_df = obt_TotalProducto(actual_ds)

    total_caja_df = obt_TotalCajas(actual_ds)

    actual_ds = obt_Metricas(total_producto_df, total_caja_df, caja_completa_df)
    
    ## Definicion de datos esperados. 


    expected_0_ds = spark_session.createDataFrame(
        [('caja_con_mas_ventas', 21),
        ('caja_con_menos_ventas', 22)],
        ['metrica', 'valor'])  

    expected_1_ds = spark_session.createDataFrame(
        [('percentil_25_por_caja', 853.0),
        ('percentil_50_por_caja', 853.0),
        ('percentil_75_por_caja', 5053.0)],
        ['metrica', 'valor'])  

    expected_2_ds = spark_session.createDataFrame(
        [('producto_mas_vendido_por_unidad', 'brocoli'),
        ('producto_de_mayor_ingreso', 'pan')],
        ['metrica', 'valor'])  
    
    expected_ds = expected_ds.union(expected_0_ds)
    expected_ds = expected_ds.union(expected_1_ds)
    expected_ds = expected_ds.union(expected_2_ds)


    actual_ds.show()

    expected_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()
