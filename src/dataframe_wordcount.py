"""Word Count con DataFrames (Spark SQL) sobre el mismo dataset.

Pipeline: spark.read.text -> split + explode -> groupBy().count() -> orderBy -> write.csv

Uso:
    python src/dataframe_wordcount.py
    python src/dataframe_wordcount.py --input data/dataset.txt --output results/df_output
"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Separador = cualquier caracter que NO sea letra. Es el complemento exacto
# del regex usado en la version con RDDs, para que la comparacion sea justa.
SEPARADOR = "[^a-záéíóúñü]+"

ENTRADA_POR_DEFECTO = "data/dataset.txt"
SALIDA_POR_DEFECTO = "results/df_output"


def run(spark, input_path, output_path):
    """Ejecuta el Word Count con DataFrames y devuelve el tiempo en segundos."""
    start = time.perf_counter()

    lineas = spark.read.text(input_path)       # DataFrame con la columna "value"

    palabras = (lineas
                .select(F.explode(
                    F.split(F.lower(F.col("value")), SEPARADOR)
                ).alias("word"))               # una fila por palabra
                .filter(F.col("word") != ""))

    conteos = (palabras
               .groupBy("word")                # agregacion optimizada
               .count()
               .orderBy(F.col("count").desc()))

    (conteos.write
            .mode("overwrite")
            .option("header", True)
            .csv(output_path))                 # accion: dispara la ejecucion

    return time.perf_counter() - start


def crear_sesion():
    """Crea la SparkSession usada cuando el script corre por su cuenta."""
    spark = (SparkSession.builder
             .appName("WordCount-DataFrame")
             .master("local[*]")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    parser = argparse.ArgumentParser(description="Word Count con DataFrames.")
    parser.add_argument("--input", default=ENTRADA_POR_DEFECTO)
    parser.add_argument("--output", default=SALIDA_POR_DEFECTO)
    args = parser.parse_args()

    spark = crear_sesion()
    elapsed = run(spark, args.input, args.output)
    print("DataFrame Word Count: {:.2f} s".format(elapsed))
    print("Resultados en: {}".format(args.output))
    spark.stop()


if __name__ == "__main__":
    main()
