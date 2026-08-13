# Semana 9 — MPI Avanzado: Comunicaciones Colectivas

Cálculo distribuido del promedio de valores aleatorios usando `MPI_Bcast`,
`MPI_Reduce`, `MPI_Scatter` y `MPI_Gather`.

**Asignatura:** Computación Paralela y Distribuida
**Autor:** Robert Abreu (23-0121) — Sección 02

## Contenido

| Archivo | Descripción |
|---|---|
| `promedio_mpi.c` | Programa principal: Bcast de N → generación local → Reduce → Bcast del promedio. Incluye un `MPI_Gather` de validación. |
| `promedio_scatter_gather.c` | Variante: el raíz genera todo el arreglo y lo reparte con `MPI_Scatter`; las sumas parciales vuelven con `MPI_Gather`. |
| `Makefile` | Compilación y ejecución automatizadas. |

## Requisitos

- Open MPI 4.x (`sudo apt install libopenmpi-dev openmpi-bin`) o MPICH
- GCC con soporte C11

## Compilación

```bash
make                      # compila los dos programas
# equivalente manual:
mpicc -Wall -Wextra -O2 -std=c11 -o promedio_mpi promedio_mpi.c -lm
```

## Ejecución

```bash
mpirun -np 4 ./promedio_mpi              # pide N por teclado
mpirun -np 4 ./promedio_mpi 1000000      # N por argumento
mpirun -np 4 ./promedio_mpi 1000000 --fija   # semilla fija (reproducible)
mpirun -np 4 ./promedio_sg 250000        # variante Scatter/Gather

make run NP=8 N=500000                   # atajos del Makefile
make escalabilidad                       # np = 1, 2, 4, 8 con carga total fija
```

> Si `mpirun` reporta que no hay suficientes ranuras, agregue `--oversubscribe`.

## Salida esperada

```
[Rank 0] genero 1000000 valores | suma parcial = 50022275.5214
[Rank 2] genero 1000000 valores | suma parcial = 50023872.7366
...
Suma total (MPI_Reduce)    : 200039527.878175
Suma total (MPI_Gather)    : 200039527.878175
Promedio global            : 50.009882
[Rank 0 de 4] promedio recibido = 50.009882 | tiempo = 0.0228 s
```

Como los valores se generan uniformemente en `[0, 100)`, el promedio debe
converger a **50.0** a medida que crece `N`; ese es el criterio de validación.

## Licencia

Uso académico.
