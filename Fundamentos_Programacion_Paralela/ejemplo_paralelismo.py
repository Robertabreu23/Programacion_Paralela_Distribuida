"""
Ejemplo práctico de paralelismo en Python
Compara la ejecución secuencial vs paralela usando multiprocessing.
"""

from multiprocessing import Pool
import time


def cuadrado(n):
    """Calcula el cuadrado de un número, simulando un trabajo pesado de 1 segundo."""
    print(f"  Calculando el cuadrado de {n}...")
    time.sleep(1)               # Simula una tarea que toma tiempo
    return n * n


if __name__ == "__main__":
    numeros = [1, 2, 3, 4, 5]

    #SECUENCIAL
    print("== Ejecución SECUENCIAL ==")
    inicio = time.time()
    resultados_sec = [cuadrado(n) for n in numeros]
    tiempo_sec = time.time() - inicio
    print(f"Resultados: {resultados_sec}")
    print(f"Tiempo total: {tiempo_sec:.2f} segundos\n")

    #PARALELA
    print("== Ejecución PARALELA ==")
    inicio = time.time()
    with Pool(processes=5) as pool:           # 5 procesos al mismo tiempo
        resultados_par = pool.map(cuadrado, numeros)
    tiempo_par = time.time() - inicio
    print(f"Resultados: {resultados_par}")
    print(f"Tiempo total: {tiempo_par:.2f} segundos\n")

    # ---------- Comparación ----------
    print(f"La versión paralela fue {tiempo_sec / tiempo_par:.1f}x más rápida.")
