
#include <stdio.h>
#include <mpi.h>

int main(int argc, char** argv) {
    int rank;          /* identificador del proceso actual        */
    int size;          /* numero total de procesos                */
    int dato;          /* variable que se envia / recibe          */
    const int TAG = 0; /* etiqueta del mensaje                    */
    MPI_Status status; /* estado de la recepcion                  */

    /* 1. Inicializacion del entorno MPI */
    MPI_Init(&argc, &argv);

    /* Obtener el rank del proceso actual */
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Obtener el numero total de procesos */
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* Verificar que haya al menos 2 procesos */
    if (size < 2) {
        if (rank == 0) {
            printf("Error: se requieren al menos 2 procesos. ");
            printf("Ejecute con: mpirun -np 2 ./comunicacion_p2p\n");
        }
        MPI_Finalize();
        return 1;
    }

    /* 2. Comunicacion punto a punto */
    if (rank == 0) {
        /* El proceso 0 define el dato y lo envia al proceso 1 */
        dato = 100;
        printf("Proceso %d: enviando el valor %d al proceso 1...\n", rank, dato);
        MPI_Send(&dato, 1, MPI_INT, 1, TAG, MPI_COMM_WORLD);
        printf("Proceso %d: envio completado.\n", rank);
    } else if (rank == 1) {
        /* El proceso 1 recibe el dato del proceso 0 */
        MPI_Recv(&dato, 1, MPI_INT, 0, TAG, MPI_COMM_WORLD, &status);
        printf("Proceso %d: valor recibido = %d (enviado por el proceso %d)\n",
               rank, dato, status.MPI_SOURCE);
    }

    /* 3. Finalizacion del entorno MPI */
    MPI_Finalize();
    return 0;
}
