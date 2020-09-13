from .utilsCajaCompra import obt_CajasTabuladas, obt_CajasTabuladasJson, guardar_Archivo, obt_TotalProducto, obt_TotalCajas


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
