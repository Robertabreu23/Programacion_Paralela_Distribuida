# Makefile para la aplicacion hibrida MPI + OpenMP
CC      = mpicc
CFLAGS  = -O3 -fopenmp -Wall -Wextra
LDFLAGS = -lm
TARGET  = matmul_hibrido
SRC     = matmul_hibrido.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

# Ejecucion rapida de prueba: 4 procesos x 4 hilos, N=1024
run: $(TARGET)
	OMP_NUM_THREADS=4 mpirun -np 4 ./$(TARGET) 1024

# Barrido completo de rendimiento -> resultados.csv
bench: $(TARGET)
	./benchmark.sh

clean:
	rm -f $(TARGET) resultados.csv

.PHONY: all run bench clean
