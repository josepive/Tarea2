from .utilsCajaCompra import obt_CajasTabuladas, obt_CajasTabuladasJson, guardar_Archivo, obt_TotalProducto, obt_TotalCajas, obt_Metricas
from pyspark.sql.types import StructType,StructField, StringType


def test_CajasTabuladas(spark_session):

    # Definicion de datos actuales.

    caja21 = '{"numero_caja": 21,"compras": [[{"nombre": "manzana","cantidad": 3 ,"precio_unitario": 20},{"nombre": "brocoli","cantidad": 2 ,"precio_unitario": 33}],[{"nombre": "aguacate","cantidad": 1 ,"precio_unitario": 5000},{"nombre": "pan","cantidad": 1 ,"precio_unitario": 800},{"nombre": "cafe","cantidad": 1 ,"precio_unitario": 800},{"nombre": "azucar","cantidad": 1 ,"precio_unitario": 700}],[{"nombre": "cafe","cantidad": 3 ,"precio_unitario": 600},{"nombre": "leche","cantidad": 2, "precio_unitario": 500}],[{"nombre": "cafe","cantidad": 1 ,"precio_unitario": 600},{"nombre": "banano","cantidad": 20 ,"precio_unitario": 50}],[{"nombre": "cafe", "cantidad": 1 ,"precio_unitario": 600}],[{"nombre": "vino","cantidad": 3 ,"precio_unitario": 4000},{"nombre": "cerveza","cantidad": 20 ,"precio_unitario": 500}],[{"nombre": "cerveza","cantidad": 1 ,"precio_unitario": 500},{"nombre": "aguacate","cantidad": 2 ,"precio_unitario": 5000},{"nombre": "aguacate","cantidad": 1 ,"precio_unitario": 5000},{"nombre": "pan","cantidad": 2 ,"precio_unitario": 800}],[{"nombre": "cafe","cantidad": 3 ,"precio_unitario": 600},{"nombre": "leche","cantidad": 3 ,"precio_unitario": 500}],[{"nombre": "cafe","cantidad": 1 ,"precio_unitario": 600},{"nombre": "banano","cantidad": 25 ,"precio_unitario": 50}],[{"nombre": "cafe","cantidad": 10 ,"precio_unitario": 600}]]}'

    actual_ds = obt_CajasTabuladasJson(caja21)
   # Caja46_df = obt_CajasTabuladasJson(caja46)
   # actual_ds = Caja45_df.union(Caja46_df)

    # Definicion de datos esperados.
    expected_ds = spark_session.createDataFrame(
        [(21, 3, 'manzana', 20),
         (21, 2, 'brocoli', 33),
         (21, 1, 'aguacate', 5000),
         (21, 1, 'pan', 800),
         (21, 1, 'cafe', 800),
         (21, 1, 'azucar', 700),
         (21, 3, 'cafe', 600),
         (21, 2, 'leche', 500),
         (21, 1, 'cafe', 600),
         (21, 20, 'banano', 50),
         (21, 1, 'cafe', 600),
         (21, 3, 'vino', 4000),
         (21, 20, 'cerveza', 500),
         (21, 1, 'cerveza', 500),
         (21, 2, 'aguacate', 5000),
         (21, 1, 'aguacate', 5000),
         (21, 2, 'pan', 800),
         (21, 3, 'cafe', 600),
         (21, 3, 'leche', 500),
         (21, 1, 'cafe', 600),
         (21, 25, 'banano', 50),
         (21, 10, 'cafe', 600)],
        ['numero_caja', 'cantidad', 'nombre', 'precio_unitario'])

    print('Actual: ')
    actual_ds.show(30)
    print('Esperado: ')
    expected_ds.show(30)

    assert actual_ds.collect() == expected_ds.collect()


def test_obtTotalProducto(spark_session):

    # Definicion de datos actuales.
    caja45 = '{"numero_caja": 45,"compras": [[{"nombre": "manzana","cantidad": 3 ,"precio_unitario": 22},{"nombre": "brocoli","cantidad": 2 ,"precio_unitario": 33}],[{"nombre": "mango","cantidad": 4 ,"precio_unitario": 90}]]}'
    caja46 = '{"numero_caja": 46,"compras": [[{"nombre": "kiwi","cantidad": 2 ,"precio_unitario": 22},{"nombre": "uvas","cantidad": 2 ,"precio_unitario": 24}],[{"nombre": "pasas","cantidad": 3 ,"precio_unitario": 12}]]}'

    # obt_TotalProducto
    Caja45_df = obt_CajasTabuladasJson(caja45)
    Caja46_df = obt_CajasTabuladasJson(caja46)
    actual_ds = Caja45_df.union(Caja46_df)

    actual_ds = obt_TotalProducto(actual_ds)

    # Definicion de datos esperados.
    expected_ds = spark_session.createDataFrame(
        [('brocoli', 2),
         ('kiwi', 2),
         ('mango', 4),
         ('manzana', 3),
         ('pasas', 3),
         ('uvas', 2)],
        ['nombre', 'cantidad_total'])

    print('Actual: ')
    actual_ds.show()
    print('Esperado: ')
    expected_ds.show()


    assert actual_ds.collect() == expected_ds.collect()

def test_obtMetricas(spark_session):

    schema = StructType([StructField('metrica', StringType(), True),StructField('valor', StringType(), True)])

    expected_ds = spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(),schema)

    caja45 = '{"numero_caja": 45,"compras": [[{"nombre": "manzana","cantidad": 3 ,"precio_unitario": 22},{"nombre": "brocoli","cantidad": 2 ,"precio_unitario": 33}],[{"nombre": "mango","cantidad": 4 ,"precio_unitario": 90}]]}'
    caja46 = '{"numero_caja": 46,"compras": [[{"nombre": "mani","cantidad": 2 ,"precio_unitario": 22},{"nombre": "almendras","cantidad": 2 ,"precio_unitario": 24}],[{"nombre": "pasas","cantidad": 3 ,"precio_unitario": 12}],[{"nombre": "ciruelas","cantidad": 1 ,"precio_unitario": 28}]]}'

    Caja45_df = obt_CajasTabuladasJson(caja45)
    Caja46_df = obt_CajasTabuladasJson(caja46)
    caja_completa_df = Caja45_df.union(Caja46_df)

    actual_ds = Caja45_df.union(Caja46_df)
    total_producto_df = obt_TotalProducto(actual_ds)

    total_caja_df = obt_TotalCajas(actual_ds)

    actual_ds = obt_Metricas(total_producto_df, total_caja_df, caja_completa_df)
    
    ## Definicion de datos esperados. 


    expected_0_ds = spark_session.createDataFrame(
        [('caja_con_mas_ventas', 45),
        ('caja_con_menos_ventas', 46)],
        ['metrica', 'valor'])  

    expected_1_ds = spark_session.createDataFrame(
        [('percentil_25_por_caja', 86.0),
        ('percentil_50_por_caja', 86.0),
        ('percentil_75_por_caja', 86.0)],
        ['metrica', 'valor'])  

    expected_2_ds = spark_session.createDataFrame(
        [('producto_mas_vendido_por_unidad', 'mango'),
        ('producto_de_mayor_ingreso', 'mango')],
        ['metrica', 'valor'])  
    
    expected_ds = expected_ds.union(expected_0_ds)
    expected_ds = expected_ds.union(expected_1_ds)
    expected_ds = expected_ds.union(expected_2_ds)


    actual_ds.show()

    expected_ds.show()
    
    assert actual_ds.collect() == expected_ds.collect()
