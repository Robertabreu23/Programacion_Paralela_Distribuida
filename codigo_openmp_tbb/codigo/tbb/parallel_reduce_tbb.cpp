// ============================================================
//  parallel_reduce_tbb.cpp
//  Actividad Semana 2 — Programación Paralela
//  Descripción: Inicialización y reducción paralela con Intel TBB
//
//  Instalar TBB (Ubuntu/Debian):
//    sudo apt install libtbb-dev
//
//  Compilar:
//    g++ -O2 -std=c++17 -o tbb_reduce parallel_reduce_tbb.cpp -ltbb
//
//  Compilar con CMake:
//    find_package(TBB REQUIRED)
//    target_link_libraries(mi_app TBB::tbb)
// ============================================================

#include <iostream>
#include <vector>
#include <numeric>
#include <chrono>
#include <iomanip>
#include <tbb/tbb.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_reduce.h>
#include <tbb/blocked_range.h>
#include <tbb/global_control.h>

int main() {
    const size_t N = 100'000'000;  // 100 millones de elementos

    std::cout << "=== Reducción Paralela con TBB ===" << std::endl;
    std::cout << "Tamaño del vector: " << N / 1'000'000 << " millones de elementos" << std::endl;

    // Mostrar número de hilos disponibles
    int hw_threads = tbb::global_control::active_value(
        tbb::global_control::max_allowed_parallelism);
    std::cout << "Hilos TBB disponibles: " << hw_threads << std::endl;
    std::cout << std::string(50, '-') << std::endl;

    std::vector<double> v(N);

    // ── PASO 1: Inicialización paralela con parallel_for ──────────────────
    // tbb::blocked_range<size_t>(0, N) define el rango completo de índices.
    // TBB divide automáticamente este rango en subrangos (chunks) y los
    // distribuye entre los hilos disponibles usando work stealing.
    {
        auto t0 = std::chrono::high_resolution_clock::now();

        tbb::parallel_for(
            tbb::blocked_range<size_t>(0, N),
            [&](const tbb::blocked_range<size_t>& r) {
                // 'r' es el subrango asignado a este hilo
                for (size_t i = r.begin(); i != r.end(); ++i) {
                    v[i] = static_cast<double>(i) * 0.000001;
                }
            }
        );

        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        std::cout << "Tiempo de inicialización (parallel_for): " << ms << " ms" << std::endl;
    }

    // ── PASO 2: Reducción paralela con parallel_reduce ────────────────────
    // parallel_reduce divide el trabajo en el árbol de subproblemas.
    // Cada hoja del árbol calcula una suma parcial de forma completamente
    // independiente (sin locks ni atomics en el bucle interior).
    // Los nodos internos del árbol combinan los resultados parciales.
    {
        auto t0 = std::chrono::high_resolution_clock::now();

        double suma_paralela = tbb::parallel_reduce(
            // Argumento 1: Rango de trabajo
            tbb::blocked_range<size_t>(0, N),

            // Argumento 2: Valor identidad (neutro para la suma)
            0.0,

            // Argumento 3: Lambda de reducción local
            // 'r' = subrango asignado a esta tarea
            // 'init' = valor inicial (0.0 o resultado de subtarea previa)
            [&](const tbb::blocked_range<size_t>& r, double init) -> double {
                double local = init;
                for (size_t i = r.begin(); i != r.end(); ++i) {
                    local += v[i];  // Suma local — SIN race condition
                }
                return local;
            },

            // Argumento 4: Lambda de combinación (join)
            // Combina dos resultados parciales de subárboles distintos
            [](double a, double b) -> double {
                return a + b;
            }
        );

        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

        std::cout << std::fixed << std::setprecision(6);
        std::cout << "Tiempo de reducción (parallel_reduce) : " << ms << " ms" << std::endl;
        std::cout << "Suma paralela   : " << suma_paralela << std::endl;
    }

    // ── PASO 3: Verificación con suma secuencial (muestra) ────────────────
    {
        auto t0 = std::chrono::high_resolution_clock::now();
        double suma_seq = 0.0;
        for (size_t i = 0; i < N; ++i)
            suma_seq += v[i];
        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        std::cout << "Suma secuencial : " << suma_seq << std::endl;
        std::cout << "Tiempo secuencial: " << ms << " ms" << std::endl;
    }

    return 0;
}
