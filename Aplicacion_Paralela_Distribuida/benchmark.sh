#!/usr/bin/env bash
# ============================================================================
#  benchmark.sh - Barrido de rendimiento para la aplicacion hibrida MPI+OpenMP
#
#  Recorre combinaciones de (procesos MPI x hilos OpenMP) para un tamano de
#  matriz fijo y guarda los resultados en resultados.csv. Tambien hace una
#  prueba de escalabilidad fuerte aumentando el paralelismo total.
#
#  Uso:   ./benchmark.sh [N]      (N por defecto = 1024)
# ============================================================================
set -euo pipefail

N="${1:-1024}"
BIN=./matmul_hibrido
CSV=resultados.csv

# Total de nucleos fisicos disponibles (para no sobre-suscribir en exceso).
NUCLEOS=$(nproc)

if [[ ! -x "$BIN" ]]; then
    echo "Compilando..."
    make
fi

echo "N,procesos,hilos,tiempo_paralelo,tiempo_serial,speedup,eficiencia" > "$CSV"

# Permitir mas ranks que nucleos en maquinas pequenas (clase/laptop).
OVERSUB="--oversubscribe"

# Combinaciones a evaluar:  procesos x hilos
# (el producto procesos*hilos es el paralelismo total)
COMBINACIONES=(
    "1 1"    # linea base secuencial (referencia)
    "1 2"    # solo OpenMP
    "1 4"
    "1 8"
    "2 1"    # solo MPI
    "4 1"
    "8 1"
    "2 2"    # hibrido
    "2 4"
    "4 2"
    "4 4"
    "8 2"
)

echo "Tamano de matriz: ${N}x${N}   |   Nucleos disponibles: ${NUCLEOS}"
echo "Ejecutando barrido (verificacion ON para medir speedup)..."
echo

for combo in "${COMBINACIONES[@]}"; do
    read -r P H <<< "$combo"
    printf ">> procesos=%s hilos=%s  (total=%s) ... " "$P" "$H" "$((P*H))"
    # argv[2]=1 fuerza la verificacion y el calculo de speedup/eficiencia.
    OUT=$(OMP_NUM_THREADS="$H" mpirun $OVERSUB -np "$P" "$BIN" "$N" 1 2>/dev/null)
    echo "$OUT" | grep '^CSV,' | sed 's/^CSV,//' >> "$CSV"
    LINEA=$(echo "$OUT" | grep '^CSV,' | sed 's/^CSV,//')
    TP=$(echo "$LINEA" | cut -d, -f4)
    SU=$(echo "$LINEA" | cut -d, -f6)
    printf "Tp=%ss  speedup=%sx\n" "$TP" "$SU"
done

echo
echo "Resultados guardados en $CSV"
echo "-----------------------------------------------------"
column -t -s, "$CSV"
