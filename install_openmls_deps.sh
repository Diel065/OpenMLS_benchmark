#!/usr/bin/env bash
set -euo pipefail

# OpenMLS benchmark dependency installer.
# Run this as your normal user from the already-cloned repo root.
# Do NOT run with sudo; the script calls sudo where needed.

if [ "${EUID}" -eq 0 ]; then
  echo "Do not run this script as root. Run it as your normal user."
  exit 1
fi

if [ ! -f "Cargo.toml" ]; then
  echo "Cargo.toml not found."
  echo "Run this script from the root of the already-cloned OpenMLS_benchmark repo."
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

UBUNTU_CODENAME="$(
  . /etc/os-release
  echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}"
)"

if [ "${UBUNTU_CODENAME}" != "noble" ]; then
  echo "WARNING: this script is tuned for Ubuntu 24.04 noble."
  echo "Detected codename: ${UBUNTU_CODENAME}"
  echo
fi

echo
echo "======================================================================"
echo "1. Base Ubuntu build/system dependencies"
echo "======================================================================"

sudo apt update

sudo apt install -y --no-install-recommends \
  git curl wget ca-certificates gnupg lsb-release software-properties-common dirmngr \
  build-essential gcc g++ make cmake pkg-config clang lld musl-tools gfortran \
  libssl-dev openssl \
  python3 python3-venv python3-pip python3-dev \
  libcurl4-openssl-dev libxml2-dev zlib1g-dev libicu-dev \
  libfontconfig1-dev libfreetype6-dev libpng-dev libjpeg-dev libtiff-dev \
  libharfbuzz-dev libfribidi-dev libgit2-dev libzmq3-dev \
  libblas-dev liblapack-dev libbz2-dev liblzma-dev libpcre2-dev

echo
echo "======================================================================"
echo "2. Docker Engine + Docker Compose plugin"
echo "======================================================================"

# Remove conflicting distro packages, but do not fail if they are absent.
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do
  sudo apt remove -y "$pkg" >/dev/null 2>&1 || true
done

sudo install -m 0755 -d /etc/apt/keyrings

sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  -o /etc/apt/keyrings/docker.asc

sudo chmod a+r /etc/apt/keyrings/docker.asc

sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${UBUNTU_CODENAME}
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update

DOCKER_VERSION="5:29.4.1-1~ubuntu.24.04~noble"
COMPOSE_VERSION="5.1.3-1~ubuntu.24.04~noble"

if apt-cache madison docker-ce | grep -Fq "${DOCKER_VERSION}" \
   && apt-cache madison docker-ce-cli | grep -Fq "${DOCKER_VERSION}" \
   && apt-cache madison docker-compose-plugin | grep -Fq "${COMPOSE_VERSION}"; then
  sudo apt install -y \
    docker-ce="${DOCKER_VERSION}" \
    docker-ce-cli="${DOCKER_VERSION}" \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin="${COMPOSE_VERSION}"
else
  echo "Exact Docker/Compose versions not available in apt anymore; installing latest from Docker repo."
  sudo apt install -y \
    docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"

echo
echo "======================================================================"
echo "3. Rust 1.89.0 via rustup"
echo "======================================================================"

if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain 1.89.0
fi

# shellcheck source=/dev/null
source "$HOME/.cargo/env"

rustup toolchain install 1.89.0
rustup default 1.89.0

rustup component add rust-src rustfmt clippy
rustup target add x86_64-unknown-linux-gnu
rustup target add x86_64-unknown-linux-musl

echo
echo "======================================================================"
echo "4. R 4.5.3 from CRAN Ubuntu repo"
echo "======================================================================"

wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc \
  | sudo tee /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc >/dev/null

sudo add-apt-repository -y "deb https://cloud.r-project.org/bin/linux/ubuntu ${UBUNTU_CODENAME}-cran40/"

sudo apt update

R_APT_VERSION="4.5.3-1.2404.0"

if apt-cache madison r-base-core | grep -Fq "${R_APT_VERSION}"; then
  sudo apt install -y --no-install-recommends \
    r-base="${R_APT_VERSION}" \
    r-base-core="${R_APT_VERSION}" \
    r-base-dev="${R_APT_VERSION}" \
    r-recommended="${R_APT_VERSION}"
else
  echo "Exact R 4.5.3 apt packages not available; installing current R from CRAN repo."
  sudo apt install -y --no-install-recommends r-base r-base-dev r-recommended
fi

echo
echo "======================================================================"
echo "5. Exact R packages used by your notebook"
echo "======================================================================"

Rscript <<'RS'
options(repos = c(CRAN = "https://cloud.r-project.org"))

user_lib <- Sys.getenv("R_LIBS_USER")
dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(user_lib, .libPaths()))

if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes")
}

install_exact <- function(pkg, version) {
  current <- tryCatch(as.character(packageVersion(pkg)), error = function(e) NA_character_)

  if (!is.na(current) && identical(current, version)) {
    message(pkg, " already installed at ", version)
    return(invisible(TRUE))
  }

  message("Installing ", pkg, " ", version)
  remotes::install_version(
    package = pkg,
    version = version,
    repos = "https://cloud.r-project.org",
    dependencies = c("Depends", "Imports", "LinkingTo"),
    upgrade = "never",
    quiet = FALSE
  )
}

wanted <- c(
  readr     = "2.2.0",
  dplyr     = "1.2.1",
  ggplot2   = "4.0.3",
  patchwork = "1.3.2",
  scales    = "1.4.0",
  tidyr     = "1.3.2",
  IRkernel  = "1.3.2"
)

for (pkg in names(wanted)) {
  install_exact(pkg, wanted[[pkg]])
}

IRkernel::installspec(user = TRUE, name = "ir", displayname = "R 4.5.3")

cat("\nInstalled R packages:\n")
ip <- installed.packages()
print(ip[intersect(names(wanted), rownames(ip)), c("Package", "Version", "LibPath")], row.names = FALSE)
RS

echo
echo "======================================================================"
echo "6. Python .venv + Jupyter stack"
echo "======================================================================"

python3 -m venv .venv

# shellcheck source=/dev/null
source .venv/bin/activate

python -m pip install --upgrade pip==26.0.1 setuptools==68.1.2 wheel==0.42.0 || {
  echo "Exact pip/setuptools/wheel versions unavailable; installing latest bootstrap tooling."
  python -m pip install --upgrade pip setuptools wheel
}

python -m pip install --upgrade \
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

python -m ipykernel install --user \
  --name openmls-benchmark \
  --display-name "Python (OpenMLS benchmark)"

echo
echo "======================================================================"
echo "7. Repo dependency sanity checks"
echo "======================================================================"

if [ ! -d "openmls-main/openmls" ] \
   || [ ! -d "openmls-main/openmls_rust_crypto" ] \
   || [ ! -d "openmls-main/basic_credential" ]; then
  echo "WARNING: local OpenMLS patch directories are missing."
  echo "Cargo.toml expects:"
  echo "  openmls-main/openmls"
  echo "  openmls-main/openmls_rust_crypto"
  echo "  openmls-main/basic_credential"
  echo "If this repo uses submodules, run:"
  echo "  git submodule update --init --recursive"
else
  cargo fetch --locked
fi

echo
echo "======================================================================"
echo "8. Final versions"
echo "======================================================================"

echo
echo "--- OS ---"
cat /etc/os-release | sed -n '1,8p'

echo
echo "--- Rust ---"
rustc -Vv
cargo -V
rustup show

echo
echo "--- Docker ---"
docker version || sudo docker version
docker compose version || sudo docker compose version

echo
echo "--- Python / Jupyter ---"
python --version
python -m pip --version
jupyter --version
jupyter kernelspec list

echo
echo "--- R ---"
R --version
Rscript -e 'sessionInfo()'

echo
echo "--- R benchmark packages ---"
Rscript -e '
pkgs <- c("readr", "dplyr", "ggplot2", "patchwork", "scales", "tidyr", "IRkernel")
ip <- installed.packages()
print(ip[intersect(pkgs, rownames(ip)), c("Package", "Version", "LibPath")], row.names = FALSE)
'

echo
echo "======================================================================"
echo "DONE"
echo "======================================================================"
echo
echo "Docker group membership may not be active in this shell yet."
echo "Run this once, or log out and back in:"
echo
echo "  newgrp docker"
echo
echo "Then, from this repo:"
echo
echo "  source .venv/bin/activate"
echo "  python scripts/run_compose_benchmark.py --workers 10 --build-images --force-cleanup-mls-ports --relay-port 4400"
echo
