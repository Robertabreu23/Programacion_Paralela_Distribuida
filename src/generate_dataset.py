"""Genera el dataset de texto (~124 MB) usado por los dos Word Count.

Descarga seis libros de dominio publico de Project Gutenberg y los
concatena repetidamente hasta alcanzar el tamano objetivo. El dataset no
se versiona en el repositorio (es pesado): se regenera con este script.

Uso:
    python src/generate_dataset.py
    python src/generate_dataset.py --target-mb 150 --output data/dataset.txt
"""

import argparse
import os
import urllib.request

# Libros de dominio publico (Project Gutenberg, texto plano UTF-8).
LIBROS = {
    "guerra_y_paz": "https://www.gutenberg.org/files/2600/2600-0.txt",
    "orgullo_y_prejuicio": "https://www.gutenberg.org/files/1342/1342-0.txt",
    "moby_dick": "https://www.gutenberg.org/files/2701/2701-0.txt",
    "alicia": "https://www.gutenberg.org/files/11/11-0.txt",
    "sherlock_holmes": "https://www.gutenberg.org/files/1661/1661-0.txt",
    "frankenstein": "https://www.gutenberg.org/files/84/84-0.txt",
}

CACHE_DIR = "data/libros"
SALIDA_POR_DEFECTO = "data/dataset.txt"
TAMANO_OBJETIVO_MB = 124


def descargar_libros(cache_dir):
    """Descarga los libros a una cache local y devuelve sus rutas."""
    os.makedirs(cache_dir, exist_ok=True)
    rutas = []
    for nombre, url in LIBROS.items():
        destino = os.path.join(cache_dir, nombre + ".txt")
        if not os.path.exists(destino):
            print("Descargando {}...".format(nombre))
            urllib.request.urlretrieve(url, destino)
        else:
            print("Ya en cache: {}".format(nombre))
        rutas.append(destino)
    return rutas


def leer_textos(rutas):
    """Lee los libros descargados y devuelve su contenido concatenado."""
    partes = []
    for ruta in rutas:
        with open(ruta, "r", encoding="utf-8", errors="ignore") as f:
            partes.append(f.read())
    return "\n".join(partes)


def generar(output_path, target_mb, cache_dir):
    """Escribe el dataset repitiendo el texto base hasta el tamano objetivo."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    texto_base = leer_textos(descargar_libros(cache_dir))
    bytes_base = len(texto_base.encode("utf-8"))
    objetivo = target_mb * 1024 * 1024
    repeticiones = max(1, objetivo // bytes_base)

    print("Texto base: {:.1f} MB".format(bytes_base / 1024 / 1024))
    print("Repeticiones necesarias: {}".format(repeticiones))

    escritos = 0
    with open(output_path, "w", encoding="utf-8") as salida:
        for _ in range(repeticiones):
            salida.write(texto_base)
            salida.write("\n")
            escritos += bytes_base + 1

    print("Dataset generado en {} ({:.1f} MB)".format(
        output_path, escritos / 1024 / 1024))


def main():
    parser = argparse.ArgumentParser(description="Genera el dataset de texto.")
    parser.add_argument("--output", default=SALIDA_POR_DEFECTO,
                        help="Ruta del archivo de salida.")
    parser.add_argument("--target-mb", type=int, default=TAMANO_OBJETIVO_MB,
                        help="Tamano objetivo del dataset en MB.")
    parser.add_argument("--cache-dir", default=CACHE_DIR,
                        help="Carpeta donde se guardan los libros descargados.")
    args = parser.parse_args()

    generar(args.output, args.target_mb, args.cache_dir)


if __name__ == "__main__":
    main()
