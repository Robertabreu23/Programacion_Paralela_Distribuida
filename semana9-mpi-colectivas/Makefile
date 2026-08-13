CC      = mpicc
CFLAGS  = -Wall -Wextra -O2 -std=c11
LDFLAGS = -lm
NP     ?= 4
N      ?= 1000000

all: promedio_mpi promedio_sg

promedio_mpi: promedio_mpi.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

promedio_sg: promedio_scatter_gather.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

run: promedio_mpi
	mpirun --oversubscribe -np $(NP) ./promedio_mpi $(N)

run-sg: promedio_sg
	mpirun --oversubscribe -np $(NP) ./promedio_sg $(N)

# Prueba de escalabilidad con carga TOTAL constante (40 millones de valores).
escalabilidad: promedio_mpi
	@for np in 1 2 4 8; do \
		echo "--- np=$$np ---"; \
		mpirun --oversubscribe -np $$np ./promedio_mpi $$((40000000/$$np)) --fija \
			| grep "promedio recibido"; \
	done

clean:
	rm -f promedio_mpi promedio_sg

.PHONY: all run run-sg escalabilidad clean
