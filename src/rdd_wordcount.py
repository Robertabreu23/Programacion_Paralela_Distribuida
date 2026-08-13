"""Word Count con RDDs (Spark Core) sobre un dataset de texto grande.

Pipeline: textFile -> flatMap -> map -> reduceByKey -> sortBy -> saveAsTextFile

Uso:
    python src/rdd_wordcount.py
    python src/rdd_wordcount.py --input data/dataset.txt --output results/rdd_output
"""

import argparse
import re
import shutil
import time
from operator import add

from pyspark.sql import SparkSession

# Misma tokenizacion que la version DataFrame: solo secuencias de letras.
TOKEN_RE = re.compile(r"[a-záéíóúñü]+")

ENTRADA_POR_DEFECTO = "data/dataset.txt"
SALIDA_POR_DEFECTO = "results/rdd_output"


def tokenize(line):
    """Convierte una linea de texto en una lista de palabras en minuscula."""
    return TOKEN_RE.findall(line.lower())


def run(spark, input_path, output_path):
    """Ejecuta el Word Count con RDDs y devuelve el tiempo en segundos."""
    shutil.rmtree(output_path, ignore_errors=True)

    sc = spark.sparkContext
    start = time.perf_counter()

    lines = sc.textFile(input_path)            # RDD[str] de lineas
    counts = (lines
              .flatMap(tokenize)               # RDD[str] de palabras
              .map(lambda word: (word, 1))     # RDD[(str, int)]
              .reduceByKey(add))               # suma por clave (shuffle)

    ordered = counts.sortBy(lambda kv: kv[1], ascending=False)
    (ordered
     .map(lambda kv: "{},{}".format(kv[0], kv[1]))
     .saveAsTextFile(output_path))             # accion: dispara la ejecucion

    return time.perf_counter() - start


def crear_sesion():
    """Crea la SparkSession usada cuando el script corre por su cuenta."""
    spark = (SparkSession.builder
             .appName("WordCount-RDD")
             .master("local[*]")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    parser = argparse.ArgumentParser(description="Word Count con RDDs.")
    parser.add_argument("--input", default=ENTRADA_POR_DEFECTO)
    parser.add_argument("--output", default=SALIDA_POR_DEFECTO)
    args = parser.parse_args()

    spark = crear_sesion()
    elapsed = run(spark, args.input, args.output)
    print("RDD Word Count: {:.2f} s".format(elapsed))
    print("Resultados en: {}".format(args.output))
    spark.stop()


if __name__ == "__main__":
    main()
