#!/usr/bin/env bash
set -euo pipefail

# Fix/recreate Python venv for OpenMLS benchmark.
# Run from the already-cloned repo root.

if [ ! -f "Cargo.toml" ] || [ ! -d "scripts" ]; then
  echo "ERROR: Run this from the OpenMLS benchmark repo root."
  echo "Current directory: $(pwd)"
  exit 1
fi

echo
echo "======================================================================"
echo "1. Fix repo ownership if needed"
echo "======================================================================"

sudo chown -R "$USER:$USER" .

echo
echo "======================================================================"
echo "2. Install Ubuntu Python venv prerequisites"
echo "======================================================================"

sudo apt update
sudo apt install -y \
  python3 \
  python3-venv \
  python3-pip \
  python3-dev \
  python3.12-venv

echo
echo "======================================================================"
echo "3. Remove broken .venv and recreate it"
echo "======================================================================"

rm -rf .venv
python3 -m venv .venv

echo
echo "======================================================================"
echo "4. Verify venv Python and pip"
echo "======================================================================"

.venv/bin/python --version
.venv/bin/python -m pip --version

echo
echo "======================================================================"
echo "5. Upgrade pip tooling"
echo "======================================================================"

.venv/bin/python -m pip install --upgrade pip setuptools wheel

echo
echo "======================================================================"
echo "6. Install Jupyter/Python packages"
echo "======================================================================"

.venv/bin/python -m pip install \
  jupyter==1.1.1 \
  jupyterlab==4.5.6 \
  notebook==7.5.5 \
  ipykernel==7.2.0 \
  ipython==9.12.0 \
  ipywidgets==8.1.8 \
  jupyter_client==8.8.0 \
  jupyter_core==5.9.1 \
  jupyter_server==2.17.0 \
  jupyterlab_server==2.28.0 \
  jupyter_server_terminals==0.5.4 \
  jupyterlab_widgets==3.0.16 \
  widgetsnbextension==4.0.15 \
  nbclient==0.10.4 \
  nbconvert==7.17.1 \
  nbformat==5.10.4 \
  notebook_shim==0.2.4 \
  traitlets==5.14.3 \
  numpy==1.26.4

echo
echo "======================================================================"
echo "7. Register Jupyter kernel"
echo "======================================================================"

.venv/bin/python -m ipykernel install --user \
  --name openmls-benchmark \
  --display-name "Python (OpenMLS benchmark)"

echo
echo "======================================================================"
echo "8. Final verification"
echo "======================================================================"

echo
echo "--- Python ---"
.venv/bin/python --version

echo
echo "--- pip ---"
.venv/bin/python -m pip --version

echo
echo "--- Jupyter ---"
.venv/bin/jupyter --version

echo
echo "--- Kernels ---"
.venv/bin/jupyter kernelspec list

echo
echo "======================================================================"
echo "DONE"
echo "======================================================================"
echo
echo "Run the benchmark with:"
echo
echo "  .venv/bin/python scripts/run_compose_benchmark.py \\"
echo "    --workers 10 \\"
echo "    --build-images \\"
echo "    --force-cleanup-mls-ports \\"
echo "    --relay-port 4400"
echo
