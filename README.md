# Tarea 2
## Jose Piedra Venegas

Se crean 5 archivos .json: Caja_21, Caja22, Caja23, Caja24, Caja25

1. Construir imagen de Docker (tarea2_jpiedra)

```
sh build_image.sh
```

2. Correr imagen de Docker (tarea2_jpiedra)

```
sh run_image.sh
```

3. Dentro del shell de la imagen (tarea2_jpiedra) ejecutar spark-submit

```
sh run_utilsCajaCompra.sh

```
Debe generar: metricas.csv, totalcajas.csv, totalproductos.csv



4. Las pruebas se ejecutan con

```
pytest
```

y son las siguientes:

- test_CajasTabuladas: prueba la funcion de tabulacion de las compras de una caja, la #21
- test_obtTotalProducto: toma dos cajas #21 y #22 y suma los productos comunes en las dos
- test_test_obtTotalCajas: suma el total de compras (dinero) de cada caja, 21 y 22
- test_test_obtMetricas: Prueba las metricas pedidas:
    - test_single: prueba de promedios ponderados con una sola fila de estudiante
    - caja_con_mas_ventas
    - caja_con_menos_ventas
    - percentil_25_por_caja
    - percentil_50_por_caja
    - percentil_75_por_caja
    - producto_mas_vendido_por_unidad
    - producto_de_mayor_ingreso