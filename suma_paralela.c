/* ============================================================
 * suma_paralela.c
 * Suma de un arreglo grande de enteros usando OpenMP.
 *
 * Compilar:
 *     gcc -O2 -fopenmp suma_paralela.c -o suma_paralela
 *
 * Ejecutar (ejemplos):
 *     OMP_NUM_THREADS=1  ./suma_paralela
 *     OMP_NUM_THREADS=2  ./suma_paralela
 *     OMP_NUM_THREADS=4  ./suma_paralela
 *     OMP_NUM_THREADS=8  ./suma_paralela
 *     OMP_NUM_THREADS=16 ./suma_paralela
 * ============================================================ */

#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

/* 200 millones de enteros (~1.6 GB con long). Reduce si tu RAM es limitada. */
#define N 200000000

int main(void) {
    long *arreglo = (long *) malloc((size_t)N * sizeof(long));
    if (arreglo == NULL) {
        fprintf(stderr, "Error: no se pudo reservar memoria.\n");
        return EXIT_FAILURE;
    }

    /* Inicializacion paralela (first-touch para repartir paginas). */
    #pragma omp parallel for schedule(static)
    for (long i = 0; i < N; i++) {
        arreglo[i] = i % 100;
    }

    /* --- Region medida: suma con reduccion --- */
    long long suma = 0;
    double t_inicio = omp_get_wtime();

    #pragma omp parallel for reduction(+:suma) schedule(static)
    for (long i = 0; i < N; i++) {
        suma += arreglo[i];
    }

    double t_fin = omp_get_wtime();
    double tiempo = t_fin - t_inicio;
    int hilos = omp_get_max_threads();

    printf("Hilos: %2d | Suma: %lld | Tiempo: %.6f s\n", hilos, suma, tiempo);

    free(arreglo);
    return EXIT_SUCCESS;
}
