// ============================================================
//  multiplicacion_matrices_openmp.cpp
//  Actividad Semana 2 — Programación Paralela
//  Descripción: Multiplicación de matrices NxN usando OpenMP
//
//  Compilar:
//    g++ -O2 -fopenmp -o mm_omp multiplicacion_matrices_openmp.cpp
//
//  Ejecutar (4 hilos):
//    OMP_NUM_THREADS=4 ./mm_omp
// ============================================================

#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <omp.h>

// Tamaño de las matrices (NxN)
const int N = 1024;

// Inicializa una matriz con valores basados en posición
void inicializar(std::vector<std::vector<double>>& M, double base) {
    for (int i = 0; i < N; i++)
        for (int j = 0; j < N; j++)
            M[i][j] = base + i * 0.1 + j * 0.01;
}

// Multiplicación secuencial (referencia)
void multiplicar_secuencial(
    const std::vector<std::vector<double>>& A,
    const std::vector<std::vector<double>>& B,
    std::vector<std::vector<double>>& C)
{
    for (int i = 0; i < N; i++)
        for (int j = 0; j < N; j++) {
            double suma = 0.0;
            for (int k = 0; k < N; k++)
                suma += A[i][k] * B[k][j];
            C[i][j] = suma;
        }
}

// Multiplicación paralela con OpenMP
void multiplicar_paralelo(
    const std::vector<std::vector<double>>& A,
    const std::vector<std::vector<double>>& B,
    std::vector<std::vector<double>>& C)
{
    // schedule(static): divide las N filas en bloques iguales por hilo
    // private(j,k):     cada hilo tiene sus propias variables j y k
    // shared(A,B,C):    las matrices son accesibles por todos los hilos
    #pragma omp parallel for schedule(static) \
                             shared(A, B, C)
    for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++) {
            double suma = 0.0;
            for (int k = 0; k < N; k++) {
                suma += A[i][k] * B[k][j];
            }
            C[i][j] = suma;
            // No hay race condition: cada i es exclusivo para un hilo
        }
    }
    // Barrera implícita al final del parallel for
}

int main() {
    std::cout << "=== Multiplicación de Matrices " << N << "x" << N
              << " con OpenMP ===" << std::endl;
    std::cout << "Hilos disponibles: " << omp_get_max_threads() << std::endl;
    std::cout << std::string(50, '-') << std::endl;

    // Reservar matrices en memoria dinámica
    std::vector<std::vector<double>> A(N, std::vector<double>(N));
    std::vector<std::vector<double>> B(N, std::vector<double>(N));
    std::vector<std::vector<double>> C_seq(N, std::vector<double>(N, 0.0));
    std::vector<std::vector<double>> C_par(N, std::vector<double>(N, 0.0));

    inicializar(A, 1.0);
    inicializar(B, 2.0);

    // --- Versión secuencial ---
    auto t0 = std::chrono::high_resolution_clock::now();
    multiplicar_secuencial(A, B, C_seq);
    auto t1 = std::chrono::high_resolution_clock::now();
    double ms_seq = std::chrono::duration<double, std::milli>(t1 - t0).count();

    // --- Versión paralela ---
    auto t2 = std::chrono::high_resolution_clock::now();
    multiplicar_paralelo(A, B, C_par);
    auto t3 = std::chrono::high_resolution_clock::now();
    double ms_par = std::chrono::duration<double, std::milli>(t3 - t2).count();

    // Verificar resultado
    double error = 0.0;
    for (int i = 0; i < N; i++)
        for (int j = 0; j < N; j++)
            error += std::abs(C_seq[i][j] - C_par[i][j]);

    std::cout << std::fixed << std::setprecision(3);
    std::cout << "Tiempo secuencial : " << ms_seq << " ms" << std::endl;
    std::cout << "Tiempo paralelo   : " << ms_par << " ms" << std::endl;
    std::cout << "Speedup           : " << ms_seq / ms_par << "x" << std::endl;
    std::cout << "Error acumulado   : " << error << " (debe ser ~0)" << std::endl;
    std::cout << "C[0][0] = " << C_par[0][0] << std::endl;

    return 0;
}
