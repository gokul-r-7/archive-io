#!/bin/bash
set -e

# Check if environment argument is provided
if [ -z "$1" ]; then
  echo "Error: No environment specified."
  echo "Usage: ./deploy.sh <dev|qa|prod>"
  exit 1
fi

# Set environment variable based on user input (dev, qa, prod)
ENVIRONMENT=$1

echo "============================"
echo " Deploying to $ENVIRONMENT environment "
echo "============================"

# Check if workspace exists, if not create it
if terraform workspace list | grep -q "$ENVIRONMENT"; then
  echo "Workspace '$ENVIRONMENT' exists. Switching to it..."
  terraform workspace select $ENVIRONMENT
else
  echo "Workspace '$ENVIRONMENT' does not exist. Creating it..."
  terraform workspace new $ENVIRONMENT
fi

echo "============================"
echo " Packaging Lambda Layer     "
echo "============================"

docker build -q -f lambda_layer/Dockerfile lambda_layer \
  -t temp-lambda-layer-build \
  --platform linux/amd64

container_id=$(docker create temp-lambda-layer-build)
docker cp "$container_id:/var/task/lambda_layer.zip" lambda_layer/lambda_layer.zip
docker rm "$container_id"

echo "============================"
echo " Packaging Lambda Functions "
echo "============================"

LAMBDA_DIR="./lambda_functions"

find "$LAMBDA_DIR" -mindepth 1 -maxdepth 1 -type d | while read -r lambda_path; do
  echo "→ Packaging Lambda: $lambda_path"

  cd "$lambda_path"
  zip -r lambda_function.zip lambda_function.py > /dev/null
  cd - > /dev/null
done

echo "============================"
echo "   Running Terraform Steps  "
echo "============================"

echo "→ Formatting Terraform code..."
terraform fmt -recursive

echo "→ Validating Terraform configuration..."
terraform validate

echo "→ Initializing Terraform..."
terraform init

echo "→ Planning Terraform changes..."
terraform plan -out=tfplan -var="environment=${ENVIRONMENT}"

echo "→ Applying Terraform changes..."
terraform apply -auto-approve tfplan

# Optionally remove the plan file
rm -f tfplan

echo "============================"
echo "   Deployment Complete      "
echo "============================"
