#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Ejecucion local (sin AWS)
#   ./run-local.sh demo     -> ejecuta la demo con el fallo intencionado
#   ./run-local.sh server   -> levanta el endpoint HTTP en http://localhost:8080/task
# -----------------------------------------------------------------------------
set -euo pipefail

JAR="target/actor-serverless.jar"                 # Artefacto generado por Maven

if [ ! -f "$JAR" ]; then                          # Si no existe el jar, lo compilamos
  echo "==> Compilando (mvn package)"
  mvn -B -q clean package
fi

MODE="${1:-demo}"                                 # Modo por defecto: demo

if [ "$MODE" = "server" ]; then
  echo "==> Arrancando servidor HTTP local en el puerto ${PORT:-8080}"
  java -cp "$JAR" com.universidad.semana14.LocalServer
else
  echo "==> Ejecutando demo de actores + supervision"
  java -jar "$JAR"
fi
