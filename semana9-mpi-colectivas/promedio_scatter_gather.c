/*
 * promedio_scatter_gather.c
 * Semana 9 - MPI Avanzado: Comunicaciones Colectivas (variante)
 *
 * Misma tarea (promedio global) pero con el esquema inverso: aqui el proceso
 * raiz genera TODO el arreglo y lo reparte con MPI_Scatter. Cada proceso suma
 * su bloque y devuelve la suma parcial con MPI_Gather. Sirve para contrastar
 * el costo de comunicacion frente a la version con generacion local.
 *
 * Compilacion:  mpicc -Wall -O2 -o promedio_sg promedio_scatter_gather.c -lm
 * Ejecucion:    mpirun -np 4 ./promedio_sg 250000
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define RANGO_MAX 100.0

int main(int argc, char *argv[])
{
    int rank, size;
    long N = 0;                  /* valores por proceso                    */
    double *global = NULL;       /* arreglo completo (solo en el rank 0)   */
    double *local = NULL;        /* bloque que recibe cada proceso         */
    double *parciales = NULL;    /* sumas parciales recolectadas (rank 0)  */
    double suma_local = 0.0, suma_total = 0.0, promedio = 0.0;
    double t_inicio, t_fin;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* --- El raiz determina N y lo difunde ---------------------------- */
    if (rank == 0) {
        if (argc > 1) {
            N = strtol(argv[1], NULL, 10);
        } else {
            printf("Ingrese N (valores por proceso): ");
            fflush(stdout);
            if (scanf("%ld", &N) != 1) N = 0;
        }
        if (N <= 0) fprintf(stderr, "Error: N debe ser positivo.\n");
    }
    MPI_Bcast(&N, 1, MPI_LONG, 0, MPI_COMM_WORLD);
    if (N <= 0) { MPI_Finalize(); return EXIT_FAILURE; }

    /* --- El raiz construye el arreglo global de N*size elementos ------ */
    if (rank == 0) {
        global = (double *)malloc((size_t)N * (size_t)size * sizeof(double));
        parciales = (double *)malloc((size_t)size * sizeof(double));
        if (global == NULL || parciales == NULL) {
            fprintf(stderr, "Error: memoria insuficiente en el rank 0.\n");
            MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        }
        srand((unsigned int)time(NULL));
        for (long i = 0; i < N * (long)size; i++) {
            global[i] = ((double)rand() / ((double)RAND_MAX + 1.0)) * RANGO_MAX;
        }
    }

    local = (double *)malloc((size_t)N * sizeof(double));
    if (local == NULL) {
        fprintf(stderr, "Error: memoria insuficiente en el rank %d.\n", rank);
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    t_inicio = MPI_Wtime();

    /* --- Scatter: el bloque i-esimo de 'global' viaja al proceso i ---- */
    MPI_Scatter(global, (int)N, MPI_DOUBLE,
                local, (int)N, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    for (long i = 0; i < N; i++) suma_local += local[i];

    /* --- Gather: el raiz recibe las 'size' sumas parciales ordenadas -- */
    MPI_Gather(&suma_local, 1, MPI_DOUBLE,
               parciales, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        for (int p = 0; p < size; p++) {
            printf("[Gather] suma parcial del rank %d = %.4f\n", p, parciales[p]);
            suma_total += parciales[p];
        }
        promedio = suma_total / ((double)N * (double)size);
        printf("Suma total = %.6f | Promedio global = %.6f\n",
               suma_total, promedio);
        fflush(stdout);
    }

    /* --- Broadcast del promedio hacia todos los procesos -------------- */
    MPI_Bcast(&promedio, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    t_fin = MPI_Wtime();

    printf("[Rank %d de %d] promedio recibido = %.6f | tiempo = %.4f s\n",
           rank, size, promedio, t_fin - t_inicio);
    fflush(stdout);

    free(local);
    if (rank == 0) { free(global); free(parciales); }

    MPI_Finalize();
    return EXIT_SUCCESS;
}
