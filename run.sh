#!/usr/bin/env bash
set -euo pipefail

# Script to run the Spark example job locally via spark-submit
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="${PROJECT_DIR}/target/example-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
INPUT_CSV="${PROJECT_DIR}/src/main/resources/test.csv"
OUTPUT_DIR="${TMPDIR:-/tmp}/spark-example-output"

if [ ! -f "${JAR_FILE}" ]; then
  echo "Jar file not found at ${JAR_FILE}."
  echo "Please build the project first: mvn clean package"
  exit 1
fi

if ! command -v spark-submit &> /dev/null; then
  echo "Error: spark-submit not found on PATH. Ensure SPARK_HOME is set and SPARK_HOME/bin is in PATH."
  exit 1
fi

echo "Cleaning previous output directory at ${OUTPUT_DIR}..."
rm -rf "${OUTPUT_DIR}"

echo "Running Spark job..."
spark-submit \
  --class com.uj.example.Example \
  --master "local[*]" \
  "${JAR_FILE}" \
  "file://${INPUT_CSV}" \
  "${OUTPUT_DIR}"

echo "Job finished successfully! Output written to ${OUTPUT_DIR}"
