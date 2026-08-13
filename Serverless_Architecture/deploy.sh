#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Script de despliegue en AWS Lambda (AWS SAM)
# Requisitos: AWS CLI configurado (aws configure) y AWS SAM CLI instalado.
# Uso: ./deploy.sh
# -----------------------------------------------------------------------------
set -euo pipefail                      # Aborta ante cualquier error o variable no definida

STACK_NAME="actor-serverless-semana14" # Nombre del stack de CloudFormation
REGION="${AWS_REGION:-us-east-1}"      # Region destino (configurable por variable de entorno)

echo "==> 1/3 Compilando el fat jar con Maven"
mvn -B clean package                   # Genera target/actor-serverless.jar con Akka incluido

echo "==> 2/3 Desplegando con AWS SAM"
sam deploy \
  --template-file template.yaml \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --capabilities CAPABILITY_IAM \
  --resolve-s3 \
  --no-confirm-changeset

echo "==> 3/3 URL del endpoint desplegado"
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='FunctionUrl'].OutputValue" \
  --output text

echo "Listo. Prueba con:"
echo "curl -X POST <URL> -H 'Content-Type: application/json' -d '{\"op\":\"sum\",\"payload\":\"1,2,3\"}'"
