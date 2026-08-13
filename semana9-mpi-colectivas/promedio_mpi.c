/*
 * promedio_mpi.c
 * Semana 9 - MPI Avanzado: Comunicaciones Colectivas
 *
 * Calcula el promedio de valores aleatorios generados por cada proceso usando
 * MPI_Bcast (distribuir N), MPI_Reduce (sumar las contribuciones parciales) y
 * un segundo MPI_Bcast (repartir el promedio final). Ademas usa MPI_Gather
 * para recolectar las sumas parciales y validar el resultado de la reduccion.
 *
 * Compilacion:  mpicc -Wall -O2 -o promedio_mpi promedio_mpi.c -lm
 * Ejecucion:    mpirun -np 4 ./promedio_mpi          (pide N por teclado)
 *               mpirun -np 4 ./promedio_mpi 1000000  (N por argumento)
 *               mpirun -np 4 ./promedio_mpi 1000000 --fija  (semilla fija)
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define RANGO_MAX 100.0 /* los valores aleatorios caen en [0, 100) */

/* Devuelve un double pseudoaleatorio uniforme en [0, RANGO_MAX). */
static double valor_aleatorio(unsigned int *semilla)
{
    return ((double)rand_r(semilla) / ((double)RAND_MAX + 1.0)) * RANGO_MAX;
}

int main(int argc, char *argv[])
{
    int rank, size;           /* identificador y cantidad de procesos      */
    long N = 0;               /* cantidad de valores por proceso           */
    int semilla_fija = 0;     /* 1 = modo reproducible para validar        */
    double suma_local = 0.0;  /* suma de los N valores de este proceso     */
    double suma_total = 0.0;  /* suma global, solo valida en el rank 0     */
    double promedio = 0.0;    /* promedio global que recibiran todos       */
    double *sumas_parciales = NULL; /* buffer de recepcion del Gather      */
    double t_inicio, t_fin;   /* medicion de tiempo                        */

    /* ------------------------------------------------------------------ */
    /* 1. Inicializacion del entorno MPI                                   */
    /* ------------------------------------------------------------------ */
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); /* quien soy  */
    MPI_Comm_size(MPI_COMM_WORLD, &size); /* cuantos somos */

    /* Bandera opcional: misma semilla en cada corrida para poder validar. */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--fija") == 0) {
            semilla_fija = 1;
        }
    }

    /* ------------------------------------------------------------------ */
    /* 2. El proceso raiz obtiene N (por argumento o por teclado)          */
    /* ------------------------------------------------------------------ */
    if (rank == 0) {
        if (argc > 1 && argv[1][0] != '-') {
            N = strtol(argv[1], NULL, 10);
        } else {
            printf("Procesos activos: %d\n", size);
            printf("Ingrese N (cantidad de valores por proceso): ");
            fflush(stdout); /* el prompt debe salir antes de bloquear en scanf */
            if (scanf("%ld", &N) != 1) {
                N = 0; /* entrada invalida: se propaga como senal de aborto */
            }
        }
        if (N <= 0) {
            fprintf(stderr, "Error: N debe ser un entero positivo.\n");
            N = 0;
        }
    }

    /* ------------------------------------------------------------------ */
    /* 3. Broadcast de N: del rank 0 hacia todos los procesos              */
    /*    Todos los procesos ejecutan la MISMA llamada; en el raiz el      */
    /*    buffer actua como origen y en el resto como destino.             */
    /* ------------------------------------------------------------------ */
    MPI_Bcast(&N, 1, MPI_LONG, 0, MPI_COMM_WORLD);

    /* Salida ordenada y sin interbloqueos: si N no es valido, TODOS los
       procesos ya lo saben (recibieron 0) y finalizan de forma colectiva. */
    if (N <= 0) {
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    /* ------------------------------------------------------------------ */
    /* 4. Generacion local de N valores y suma parcial                     */
    /*    Cada proceso usa una semilla distinta (derivada de su rank) para */
    /*    no generar exactamente la misma secuencia en todos los nodos.    */
    /* ------------------------------------------------------------------ */
    unsigned int semilla = semilla_fija
                               ? (unsigned int)(12345u + 7919u * (unsigned)rank)
                               : (unsigned int)(time(NULL) + 7919u * (unsigned)rank);

    MPI_Barrier(MPI_COMM_WORLD); /* alinea el arranque antes de cronometrar */
    t_inicio = MPI_Wtime();

    for (long i = 0; i < N; i++) {
        suma_local += valor_aleatorio(&semilla);
    }

    printf("[Rank %d] genero %ld valores | suma parcial = %.4f\n",
           rank, N, suma_local);
    fflush(stdout);

    /* ------------------------------------------------------------------ */
    /* 5. Reduccion: MPI_SUM de todas las sumas parciales en el rank 0     */
    /* ------------------------------------------------------------------ */
    MPI_Reduce(&suma_local, &suma_total, 1, MPI_DOUBLE, MPI_SUM, 0,
               MPI_COMM_WORLD);

    /* ------------------------------------------------------------------ */
    /* 5b. Gather (validacion): el raiz recibe ademas cada suma parcial    */
    /*     por separado para comprobar que la reduccion es correcta.       */
    /* ------------------------------------------------------------------ */
    if (rank == 0) {
        sumas_parciales = (double *)malloc((size_t)size * sizeof(double));
        if (sumas_parciales == NULL) {
            fprintf(stderr, "Error: memoria insuficiente en el rank 0.\n");
            MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        }
    }
    MPI_Gather(&suma_local, 1, MPI_DOUBLE,
               sumas_parciales, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    /* ------------------------------------------------------------------ */
    /* 6. El raiz calcula el promedio global                               */
    /* ------------------------------------------------------------------ */
    if (rank == 0) {
        promedio = suma_total / ((double)N * (double)size);

        double control = 0.0;
        for (int p = 0; p < size; p++) {
            control += sumas_parciales[p];
        }

        printf("\n----------------- RESULTADOS EN EL RANK 0 -----------------\n");
        printf("Valores totales procesados : %ld  (N=%ld x %d procesos)\n",
               N * (long)size, N, size);
        printf("Suma total (MPI_Reduce)    : %.6f\n", suma_total);
        printf("Suma total (MPI_Gather)    : %.6f\n", control);
        printf("Diferencia absoluta        : %.3e\n", suma_total - control);
        printf("Promedio global            : %.6f\n", promedio);
        printf("-----------------------------------------------------------\n\n");
        fflush(stdout);

        free(sumas_parciales);
    }

    /* ------------------------------------------------------------------ */
    /* 7. Broadcast del promedio: del rank 0 hacia todos                   */
    /* ------------------------------------------------------------------ */
    MPI_Bcast(&promedio, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    t_fin = MPI_Wtime();

    /* ------------------------------------------------------------------ */
    /* 8. Cada proceso imprime el promedio recibido junto con su rank      */
    /* ------------------------------------------------------------------ */
    printf("[Rank %d de %d] promedio recibido = %.6f | tiempo = %.4f s\n",
           rank, size, promedio, t_fin - t_inicio);
    fflush(stdout);

    /* ------------------------------------------------------------------ */
    /* 9. Cierre del entorno MPI                                           */
    /* ------------------------------------------------------------------ */
    MPI_Finalize();
    return EXIT_SUCCESS;
}
