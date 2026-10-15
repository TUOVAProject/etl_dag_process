#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/opt/airflow/config/python-venvs/erp_pipeline"
VENV_DIR="${PROJECT_DIR}/.venv"

# Airflow-image asettaa PIP_USER=true, mikä rikkoo venv-asennukset
export PIP_USER=false

echo "Setting up erp_pipeline virtual environment..."

if [ ! -d "${VENV_DIR}" ]; then
    echo "Creating virtual environment: ${VENV_DIR}"
    python -m venv "${VENV_DIR}"
else
    echo "Virtual environment already exists: ${VENV_DIR}"
fi

echo "Installing erp_pipeline and its dependencies..."
"${VENV_DIR}/bin/python" -m pip install --editable "${PROJECT_DIR}"

echo "Verifying installation..."
"${VENV_DIR}/bin/python" -c "import erp_pipeline; print('erp_pipeline installation verified')"

echo "erp_pipeline virtual environment is ready."