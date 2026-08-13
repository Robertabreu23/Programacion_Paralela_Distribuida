"""Compara RDD vs DataFrame: mide tiempos, calcula speedup y grafica.

Crea una unica SparkSession para que ambos metodos corran en igualdad de
condiciones, descarta una corrida de calentamiento de la JVM y promedia
tres corridas de cada enfoque.

Uso:
    python src/compare.py
    python src/compare.py --corridas 5 --input data/dataset.txt
"""

import argparse
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

import rdd_wordcount
import dataframe_wordcount

AQUI = os.path.dirname(os.path.abspath(__file__))
ENTRADA_POR_DEFECTO = "data/dataset.txt"
CARPETA_RESULTADOS = "results"
CORRIDAS_POR_DEFECTO = 3


def medir(spark, entrada, corridas):
    """Ejecuta ambos metodos varias veces y devuelve los tiempos medidos."""
    # Corrida de calentamiento (se descarta): evita que el primer metodo
    # cargue con el costo de inicializar la JVM y generar el codigo.
    print("Calentando la JVM (esta corrida se descarta)...")
    rdd_wordcount.run(spark, entrada, os.path.join(CARPETA_RESULTADOS, "warmup_rdd"))
    dataframe_wordcount.run(spark, entrada, os.path.join(CARPETA_RESULTADOS, "warmup_df"))

    tiempos = {"RDD": [], "DataFrame": []}
    for i in range(1, corridas + 1):
        tiempos["RDD"].append(
            rdd_wordcount.run(spark, entrada,
                              os.path.join(CARPETA_RESULTADOS, "rdd_output")))
        tiempos["DataFrame"].append(
            dataframe_wordcount.run(spark, entrada,
                                    os.path.join(CARPETA_RESULTADOS, "df_output")))
        print("Corrida {}/{}: RDD {:.2f} s | DataFrame {:.2f} s".format(
            i, corridas, tiempos["RDD"][-1], tiempos["DataFrame"][-1]))
    return tiempos


def guardar_csv(tiempos, prom_rdd, prom_df, ruta):
    """Escribe la tabla de tiempos en un CSV."""
    corridas = len(tiempos["RDD"])
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metodo"]
                   + ["corrida_{}".format(i) for i in range(1, corridas + 1)]
                   + ["promedio"])
        w.writerow(["RDD"] + ["{:.2f}".format(t) for t in tiempos["RDD"]]
                   + ["{:.2f}".format(prom_rdd)])
        w.writerow(["DataFrame"] + ["{:.2f}".format(t) for t in tiempos["DataFrame"]]
                   + ["{:.2f}".format(prom_df)])
    print("Tabla de tiempos: {}".format(ruta))


def graficar(prom_rdd, prom_df, speedup, ruta):
    """Genera el grafico de barras con los tiempos promedio."""
    fig, ax = plt.subplots(figsize=(6, 4))
    barras = ax.bar(["RDD", "DataFrame"], [prom_rdd, prom_df],
                    color=["#c0504d", "#4472c4"])
    for barra in barras:
        ax.annotate("{:.2f} s".format(barra.get_height()),
                    xy=(barra.get_x() + barra.get_width() / 2, barra.get_height()),
                    xytext=(0, 3), textcoords="offset points", ha="center")
    ax.set_ylabel("Tiempo promedio (s)")
    ax.set_title("Word Count: RDD vs DataFrame (speedup {:.2f}x)".format(speedup))
    fig.savefig(ruta, dpi=150, bbox_inches="tight")
    print("Grafico: {}".format(ruta))


def main():
    parser = argparse.ArgumentParser(description="Compara RDD vs DataFrame.")
    parser.add_argument("--input", default=ENTRADA_POR_DEFECTO)
    parser.add_argument("--corridas", type=int, default=CORRIDAS_POR_DEFECTO)
    args = parser.parse_args()

    os.makedirs(CARPETA_RESULTADOS, exist_ok=True)

    # Una sola SparkSession: misma JVM, mismos recursos, misma configuracion.
    spark = (SparkSession.builder
             .appName("Comparacion-RDD-vs-DataFrame")
             .master("local[*]")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # Aqui rdd_wordcount se importa como modulo (no es __main__), asi que
    # tokenize viaja a los workers por referencia: hay que enviarles el archivo.
    spark.sparkContext.addPyFile(os.path.join(AQUI, "rdd_wordcount.py"))

    tiempos = medir(spark, args.input, args.corridas)

    prom_rdd = sum(tiempos["RDD"]) / args.corridas
    prom_df = sum(tiempos["DataFrame"]) / args.corridas
    speedup = prom_rdd / prom_df

    print("\nPromedio RDD:       {:.2f} s".format(prom_rdd))
    print("Promedio DataFrame: {:.2f} s".format(prom_df))
    print("Speedup (RDD / DataFrame) = {:.2f}x".format(speedup))

    guardar_csv(tiempos, prom_rdd, prom_df,
                os.path.join(CARPETA_RESULTADOS, "timings.csv"))
    graficar(prom_rdd, prom_df, speedup,
             os.path.join(CARPETA_RESULTADOS, "speedup.png"))

    spark.stop()


if __name__ == "__main__":
    main()
