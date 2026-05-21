# Repositorio: Programación Paralela — Semana 2
## Memoria Compartida, OpenMP y TBB

Actividad individual — Valor: 5 puntos  
Entrega: 20 de mayo de 2025

---

## Estructura del Repositorio

```
/
├── README.md
├── openmp/
│   └── multiplicacion_matrices_openmp.cpp   ← Ejemplo OpenMP
└── tbb/
    └── parallel_reduce_tbb.cpp              ← Ejemplo TBB
```

---

## Ejemplo 1 — OpenMP: Multiplicación de Matrices

**Archivo:** `openmp/multiplicacion_matrices_openmp.cpp`

Multiplica dos matrices cuadradas de 1024×1024 en paralelo. Demuestra:
- `#pragma omp parallel for` con `schedule(static)`
- Cláusulas `shared` y `private`
- Comparación de tiempo secuencial vs. paralelo y cálculo del speedup.

### Compilar y ejecutar

```bash
# Requiere GCC con soporte OpenMP (instalado por defecto en Ubuntu/Debian)
g++ -O2 -fopenmp -o mm_omp openmp/multiplicacion_matrices_openmp.cpp

# Ejecutar con 4 hilos
OMP_NUM_THREADS=4 ./mm_omp

# Ejecutar con todos los núcleos disponibles
./mm_omp
```

### Salida esperada (máquina de 8 núcleos)

```
=== Multiplicación de Matrices 1024x1024 con OpenMP ===
Hilos disponibles: 8
--------------------------------------------------
Tiempo secuencial : 8423.512 ms
Tiempo paralelo   : 1124.033 ms
Speedup           : 7.49x
Error acumulado   : 0.000 (debe ser ~0)
C[0][0] = 524799.488
```

---

## Ejemplo 2 — TBB: Reducción Paralela de Vector

**Archivo:** `tbb/parallel_reduce_tbb.cpp`

Inicializa y suma 100 millones de doubles usando `parallel_for` y `parallel_reduce`. Demuestra:
- `tbb::parallel_for` con `blocked_range`
- `tbb::parallel_reduce` con lambdas de reducción y combinación
- Work stealing automático del scheduler de TBB

### Instalar TBB

```bash
# Ubuntu / Debian
sudo apt install libtbb-dev

# macOS (Homebrew)
brew install tbb

# Windows (vcpkg)
vcpkg install tbb
```

### Compilar y ejecutar

```bash
g++ -O2 -std=c++17 -o tbb_reduce tbb/parallel_reduce_tbb.cpp -ltbb
./tbb_reduce
```

### Salida esperada

```
=== Reducción Paralela con TBB ===
Tamaño del vector: 100 millones de elementos
Hilos TBB disponibles: 8
--------------------------------------------------
Tiempo de inicialización (parallel_for): 112.3 ms
Tiempo de reducción (parallel_reduce) : 43.7 ms
Suma paralela   : 4999999950.000000
Suma secuencial : 4999999950.000000
Tiempo secuencial: 287.6 ms
```

---

## Requisitos del Sistema

| Herramienta | Versión mínima | Notas |
|-------------|---------------|-------|
| GCC         | 9.0           | `-fopenmp` para OpenMP |
| Clang       | 10.0          | `-fopenmp` para OpenMP |
| libtbb-dev  | 2021.x (oneTBB) | Apache 2.0 |
| CMake       | 3.14          | Opcional, recomendado para TBB |
| C++ estándar | C++17        | Para TBB moderno |

---

## Referencias

- OpenMP Specification 5.1: https://www.openmp.org/specifications/
- oneTBB GitHub: https://github.com/oneapi-src/oneTBB
- Pacheco, P. S. (2011). *An Introduction to Parallel Programming*. Morgan Kaufmann.
