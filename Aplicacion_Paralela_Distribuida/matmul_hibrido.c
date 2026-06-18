/*
 * ============================================================================
 *  matmul_hibrido.c
 *
 *  Multiplicacion de matrices C = A x B usando un enfoque HIBRIDO:
 *      - MPI    : distribuye las filas de A entre varios procesos (nodos).
 *      - OpenMP : paraleliza el calculo dentro de cada proceso con varios hilos.
 *
 *  Flujo general (modelo maestro/trabajadores con descomposicion por filas):
 *      1. El proceso 0 genera las matrices A (N x N) y B (N x N).
 *      2. Se difunde B completa a todos los procesos        -> MPI_Bcast
 *      3. Se reparten bloques de filas de A entre procesos   -> MPI_Scatter
 *      4. Cada proceso multiplica su bloque local con OpenMP -> #pragma omp
 *      5. Se recolectan los bloques de C en el proceso 0     -> MPI_Gather
 *      6. El proceso 0 mide tiempo, verifica y reporta metricas.
 *
 *  Para que MPI_Scatter / MPI_Gather repartan bloques iguales, el numero de
 *  filas debe ser divisible entre el numero de procesos. Si N no lo es, el
 *  programa "rellena" (padding) la dimension hasta el siguiente multiplo de
 *  'size'; las filas extra contienen ceros y no afectan el resultado.
 *
 *  Compilacion:
 *      mpicc -O3 -fopenmp -Wall -o matmul_hibrido matmul_hibrido.c
 *
 *  Ejecucion (ejemplo: 4 procesos MPI, 4 hilos OpenMP cada uno, N=1024):
 *      OMP_NUM_THREADS=4 mpirun -np 4 ./matmul_hibrido 1024
 *
 *  Argumentos:
 *      argv[1] = N        (dimension de la matriz, por defecto 512)
 *      argv[2] = verify   (1 = verificar contra calculo serial, 0 = no; por
 *                          defecto se verifica solo si N <= 1024)
 *
 *  Autor: <completar>           Fecha: 2026
 * ============================================================================
 */

#include <mpi.h>
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* Inicializa una matriz de 'filas x cols' con valores deterministas y
 * reproducibles (para poder verificar el resultado entre ejecuciones). */
static void inicializar_matriz(double *M, int filas, int cols, unsigned semilla)
{
    for (int i = 0; i < filas; i++)
        for (int j = 0; j < cols; j++)
            /* valor en [0,1) dependiente de la posicion y la semilla */
            M[(long)i * cols + j] =
                (double)(((i * 31 + j * 17 + semilla) % 1000)) / 1000.0;
}

/*
 * Kernel de multiplicacion paralelizado con OpenMP.
 *   C_local (filas_local x N) = A_local (filas_local x N) * B (N x N)
 *
 * Se usa el orden de bucles i-k-j (en lugar de i-j-k) porque recorre B por
 * filas, lo que mejora notablemente el aprovechamiento de la cache.
 * El reparto de filas entre hilos lo hace OpenMP con 'schedule(static)';
 * como cada hilo escribe en filas distintas de C_local NO hay condiciones de
 * carrera y no se necesitan secciones criticas.
 */
static void multiplicar_local(const double *A_local, const double *B,
                              double *C_local, int filas_local, int N)
{
    #pragma omp parallel for schedule(static)
    for (int i = 0; i < filas_local; i++) {
        double *fila_C = &C_local[(long)i * N];
        for (int j = 0; j < N; j++)
            fila_C[j] = 0.0;
        for (int k = 0; k < N; k++) {
            double a_ik = A_local[(long)i * N + k];
            const double *fila_B = &B[(long)k * N];
            for (int j = 0; j < N; j++)
                fila_C[j] += a_ik * fila_B[j];
        }
    }
}

/* Multiplicacion serial de referencia (un solo hilo) usada como linea base
 * para calcular el speedup y para verificar la correctitud. */
static void multiplicar_serial(const double *A, const double *B,
                               double *C, int N)
{
    for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++)
            C[(long)i * N + j] = 0.0;
        for (int k = 0; k < N; k++) {
            double a_ik = A[(long)i * N + k];
            for (int j = 0; j < N; j++)
                C[(long)i * N + j] += a_ik * B[(long)k * N + j];
        }
    }
}

int main(int argc, char **argv)
{
    int rank, size, provided;

    /* ---- 1. Inicializacion del entorno MPI ----
     * Usamos MPI_Init_thread con MPI_THREAD_FUNNELED porque solo el hilo
     * principal de cada proceso realiza llamadas MPI (los hilos OpenMP solo
     * hacen calculo). Es el nivel de soporte adecuado para MPI + OpenMP. */
    MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);   /* identificador del proceso  */
    MPI_Comm_size(MPI_COMM_WORLD, &size);   /* numero total de procesos   */

    /* ---- Lectura de parametros ---- */
    int N = (argc > 1) ? atoi(argv[1]) : 512;
    if (N <= 0) {
        if (rank == 0) fprintf(stderr, "N debe ser un entero positivo.\n");
        MPI_Finalize();
        return 1;
    }
    /* Verificar por defecto solo para tamanos manejables. */
    int verificar = (argc > 2) ? atoi(argv[2]) : (N <= 1024);

    int hilos = omp_get_max_threads();

    /* ---- Padding: ampliar el numero de filas al siguiente multiplo de size
     * para que MPI_Scatter/Gather repartan bloques iguales. ---- */
    int filas_local = (N + size - 1) / size;   /* techo de N/size */
    int N_pad       = filas_local * size;      /* filas totales con relleno */

    if (rank == 0) {
        printf("=====================================================\n");
        printf(" Multiplicacion de matrices HIBRIDA  (MPI + OpenMP)\n");
        printf("=====================================================\n");
        printf(" Dimension matriz N      : %d x %d\n", N, N);
        printf(" Procesos MPI            : %d\n", size);
        printf(" Hilos OpenMP por proceso: %d\n", hilos);
        printf(" Paralelismo total       : %d unidades de calculo\n",
               size * hilos);
        printf(" Filas por proceso       : %d (N_padded=%d)\n",
               filas_local, N_pad);
        printf("-----------------------------------------------------\n");
        fflush(stdout);
    }

    /* ---- Reserva de memoria ----
     * Proceso 0: A y C completas con relleno (N_pad x N). Resto: solo B y los
     * bloques locales. B se reserva en todos porque se difunde completa. */
    double *A = NULL, *C = NULL, *C_serial = NULL;
    double *B        = malloc((size_t)N * N * sizeof(double));
    double *A_local  = malloc((size_t)filas_local * N * sizeof(double));
    double *C_local  = malloc((size_t)filas_local * N * sizeof(double));

    if (rank == 0) {
        A = calloc((size_t)N_pad * N, sizeof(double)); /* calloc -> filas extra a 0 */
        C = malloc((size_t)N_pad * N * sizeof(double));
        inicializar_matriz(A, N, N, 1);   /* solo las N filas reales */
        inicializar_matriz(B, N, N, 7);
    }
    if (!B || !A_local || !C_local ||
        (rank == 0 && (!A || !C))) {
        fprintf(stderr, "[rank %d] Error de reserva de memoria.\n", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* Barrera + cronometro: aseguramos que todos los procesos empiezan a medir
     * el tiempo de la fase paralela al mismo tiempo. */
    MPI_Barrier(MPI_COMM_WORLD);
    double t_inicio = MPI_Wtime();

    /* ---- 2. Difusion de B a todos los procesos ---- */
    MPI_Bcast(B, N * N, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    /* ---- 3. Reparto de las filas de A (bloques iguales) ---- */
    MPI_Scatter(A,       filas_local * N, MPI_DOUBLE,
                A_local, filas_local * N, MPI_DOUBLE,
                0, MPI_COMM_WORLD);

    /* ---- 4. Calculo local paralelizado con OpenMP ---- */
    multiplicar_local(A_local, B, C_local, filas_local, N);

    /* ---- 5. Recoleccion de los bloques de C en el proceso 0 ---- */
    MPI_Gather(C_local, filas_local * N, MPI_DOUBLE,
               C,       filas_local * N, MPI_DOUBLE,
               0, MPI_COMM_WORLD);

    double t_fin = MPI_Wtime();
    double t_local = t_fin - t_inicio;

    /* El tiempo paralelo global es el del proceso mas lento (cuello de botella). */
    double t_paralelo;
    MPI_Reduce(&t_local, &t_paralelo, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    /* ---- 6. Verificacion y metricas de rendimiento (solo proceso 0) ---- */
    if (rank == 0) {
        double t_serial = 0.0;
        int correcto = 1;

        if (verificar) {
            C_serial = malloc((size_t)N * N * sizeof(double));
            double s0 = MPI_Wtime();
            multiplicar_serial(A, B, C_serial, N);   /* A tiene N_pad filas, usamos N */
            t_serial = MPI_Wtime() - s0;

            /* Comparar solo las N x N celdas reales. */
            double max_err = 0.0;
            for (int i = 0; i < N && correcto; i++)
                for (int j = 0; j < N; j++) {
                    double d = fabs(C[(long)i * N + j] - C_serial[(long)i * N + j]);
                    if (d > max_err) max_err = d;
                    if (d > 1e-9) { correcto = 0; break; }
                }
            printf(" Verificacion            : %s (error max = %.2e)\n",
                   correcto ? "CORRECTA" : "FALLIDA", max_err);
        }

        printf("-----------------------------------------------------\n");
        printf(" Tiempo PARALELO (hibrido): %.6f s\n", t_paralelo);

        if (verificar) {
            double speedup    = t_serial / t_paralelo;
            int    p_total    = size * hilos;
            double eficiencia = speedup / p_total;
            printf(" Tiempo SERIAL (1 hilo)  : %.6f s\n", t_serial);
            printf(" Speedup  (Ts/Tp)        : %.2fx\n", speedup);
            printf(" Eficiencia (S/(P*H))    : %.2f%%  (%d unidades)\n",
                   eficiencia * 100.0, p_total);
        } else {
            printf(" (verificacion/speedup desactivados para N grande;"
                   " usar argv[2]=1 para forzar)\n");
        }

        /* Linea en formato CSV para procesar resultados facilmente:
         * N,procesos,hilos,tiempo_paralelo,tiempo_serial,speedup,eficiencia */
        double speedup    = verificar ? t_serial / t_paralelo : 0.0;
        double eficiencia = verificar ? speedup / (size * hilos) : 0.0;
        printf("CSV,%d,%d,%d,%.6f,%.6f,%.4f,%.4f\n",
               N, size, hilos, t_paralelo, t_serial, speedup, eficiencia);
        printf("=====================================================\n");

        free(C_serial);
    }

    /* ---- Liberacion de memoria ---- */
    free(B); free(A_local); free(C_local);
    if (rank == 0) { free(A); free(C); }

    MPI_Finalize();
    return 0;
}
