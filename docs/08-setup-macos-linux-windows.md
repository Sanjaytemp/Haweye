# 08 — Setting up your machine (macOS, Linux, Windows)

*Everything that must be true **before** `make bootstrap` works, per OS, with the
gotchas that are specific to each. If you already have Docker Desktop running and
`make check` passes, skip this file.*

## What the project needs from your machine

| requirement | why | minimum |
|---|---|---|
| Docker + the compose **plugin** (`docker compose`, not `docker-compose`) | all 15 services | Docker 24+, compose v2.20+ |
| **12 GB free RAM for Docker**, 4 CPU | Spark + Kafka + MinIO + Airflow + Postgres + Redis at once | 8 GB works if you lower `SPARK_WORKER_MEMORY` (see below) |
| 25 GB free **disk** for images/volumes | images are ~6 GB; each `make nuke` re-downloads if you prune | 20 GB |
| `make` | the entry point for everything | GNU make 4+ |
| `git` | cloning, committing | any recent |
| Python 3.10+ on the host | only for `make lint`/`make test` (the venv bootstrap creates `.venv`) | 3.10–3.12 |
| Free ports | see the table | — |

Ports (all overridable in `.env`, and `make bootstrap` checks them for you):

| service | host port | `.env` var |
|---|---|---|
| Kafka (external listener) | 9094 | `KAFKA_HOST_PORT` |
| MinIO API / console | 9000 / 9001 | `MINIO_API_PORT` / `MINIO_CONSOLE_PORT` |
| Postgres | 5432 | `POSTGRES_PORT` |
| Redis | 6379 | `REDIS_PORT` |
| Spark UI (master) / Spark master | 8080 / 7077 | `SPARK_UI_PORT` / `SPARK_MASTER_PORT` |
| Spark job UI (while a job runs) | 4040 | `SPARK_JOB_UI_PORT` |
| Serving API | 8000 | `API_PORT` |
| Airflow | 8085 | `AIRFLOW_PORT` |
| NiFi | 8090 | `NIFI_PORT` |
| CDC Postgres / Connect | 5433 / 8083 | `CDC_PG_PORT` / `CDC_CONNECT_PORT` |
| MLflow | 5000 | `MLFLOW_PORT` |

Two that clash most often: **5432** (a Postgres you installed months ago) and
**8000/5000** (another project's dev server). Change them in `.env`; don't stop
your database.

---

## macOS (Apple silicon or Intel)

```bash
# 1. Docker Desktop (or OrbStack, which is lighter and faster on ARM)
brew install --cask docker          # or: brew install --cask orbstack
open -a Docker                       # wait for the whale to say "running"

# 2. make + git ship with Xcode's command line tools
xcode-select --install               # if `make` says "command not found"

# 3. python for the host-side tests (optional; bootstrap can also use python3 from brew)
brew install python@3.11
```

**Docker Desktop → Settings → Resources**: give it 8+ CPU, 12 GB RAM, 100 GB disk.
The #1 cause of "spark-worker keeps restarting" on a Mac is a 4 GB VM limit.

Gotchas:

* **`localhost` works** on macOS for published ports (Docker's VM forwards them) —
  unlike WSL2, where it sometimes doesn't. So `psql -h localhost -p 5432` works.
* **File I/O is slow across the mount boundary.** We bind-mount `./jobs` into the
  Spark container: fine (read-only, small). Never put the *data* on a bind mount;
  the compose file uses named volumes for exactly this reason.
* **`host.docker.internal`** resolves out of the box on Docker Desktop — that's
  what the `extra_hosts: *host-gateway` anchor in the compose file is for (a job
  that must reach a database on your Mac).
* **zsh + `$$`**: `make gen-stream` uses `$$` escapes internally; you don't type
  them. If you copy a compose `command:` into your shell, replace `$$` with `$`.
* Apple silicon: `confluentinc/cp-server` and `bitnami/spark` run `linux/arm64`
  images natively. If a pull says "no matching manifest", add
  `DOCKER_DEFAULT_PLATFORM=linux/amd64` to `.env` — it works through Rosetta, ~30%
  slower.

## Linux

```bash
# Docker Engine + compose plugin (Ubuntu/Debian)
sudo apt-get update && sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker "$USER"          # log out/in afterwards
newgrp docker                            # or: re-login; then test without sudo
docker run --rm hello world
apt-get install -y make git python3-venv python3-pip
```

Gotchas:

* **`docker: permission denied` after `usermod`** — you didn't start a new login
  shell. `newgrp docker` for the current one.
* **A firewall or SELinux/AppArmor** can block the published 9094 listener. Test
  from inside: `docker compose exec spark-master bash -c 'echo > /dev/tcp/kafka/29092 && echo ok'`.
* **Swap.** Spark + Kafka will use it; `free -h` before you start, or reduce
  `SPARK_WORKER_MEMORY=1g`, `KAFKA_HEAP_OPTS=-Xmx512m -Xms512m` in `.env`.
* **`inotify` limits** for Airflow's DAG-rotation logs on big repos:
  `echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf; sudo sysctl --system`.
* **Rootless Docker** works but the `host-gateway` extra host differs; use
  `host.docker.internal` (provided automatically by rootless's slirp4netns).

## Windows

Two supported routes; pick one and stay on it.

### A. WSL2 (recommended)

```powershell
# PowerShell as Admin
wsl --install -d Ubuntu-24.04
```

Then inside Ubuntu follow the **Linux** section (Docker Engine *inside WSL*, or
Docker Desktop with the WSL integration toggle for that distro). Open the project
**inside the WSL filesystem** (`~/dev/Haweye`), *not* `/mnt/c/Users/…`:

* `/mnt/c/...` is 5–20× slower for the many-small-files patterns Spark/Iceberg use,
  and file-watchers/permissions (exec bit on `run_job.sh`) break there.
* If you must live on `/mnt/c`, run `git config core.fileMode false` and expect
  slow first builds.

From Windows you then reach everything at `http://localhost:8085` etc. (WSL2
forwards localhost for listening ports in current builds). If a port doesn't
forward: `netsh interface portproxy add v4tov4 listenport=9094 listenaddress=0.0.0.0 connectport=9094 connectaddress=(wsl hostname -I)`.

Line endings — do this once, before cloning:

```powershell
git config --global core.autocrlf input     # LF in the repo, LF in WSL
```

And if you ever see `bash\r: No such file or directory` or `: command not found`
from our `.sh` files, it's CRLF; fix the file: `sed -i 's/\r$//' <file>` (the repo
ships `.gitattributes` with `* text=auto eol=lf` for `*.sh`/`Makefile` so a normal
clone is fine).

### B. Docker Desktop + PowerShell (no WSL in the loop)

```powershell
winget install Docker.DockerDesktop
winget install Git.Git
winget install GnuWin32.Make        # or use `choco install make`
```

Then either (a) run the `docker compose` commands directly (documented inside each
Makefile target — copy them), or (b) use **Git Bash** for `make`, which is what
the team here uses.

Windows-specific gotchas:

* `make` isn't in PowerShell by default → `choco install make` / use Git Bash /
  run the compose commands directly.
* **Memory**: Docker Desktop's VM defaults are small. Settings → Resources → 12 GB.
* **Paths with spaces or `~`**: `CURDIR`-based bind mounts in the Makefile break
  under MSYS path translation. If `make seed-dims` complains about
  `/c/Users/...`, run it from Git Bash (not PowerShell).
* **Antivirus/Defender** real-time scanning of `node_modules`-sized folders slows
  the first image build a lot; exclude your repo folder and `Docker Desktop`.
* **`docker compose exec -T`** (used by `run_job.sh`) needs a working TTY-less
  mode — fine in PowerShell 7 and Git Bash; in cmd.exe prefer
  `docker compose exec spark-master spark-submit ...` directly.

---

## The venv (host-side tooling only)

`make bootstrap` creates `.venv` and installs `requirements-dev.txt` (pytest,
ruff, sqlglot, pyyaml, fastapi, pyspark for the spark-marked tests). If you prefer
to do it by hand:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt      # Windows: .venv\Scripts\pip
make lint test
```

Why a venv at all? Linux/macOS system python is "externally managed" (PEP 668) and
`pip install` there fails with a message that looks like a bug in the repo. It
isn't. The venv is the fix, and `make bootstrap` does it for you.

## Verify the machine, then the project

```bash
docker info >/dev/null && echo docker-ok
docker compose version && make --version | head -1 && python3 -V && git --version
git clone https://github.com/Sanjaytemp/Haweye.git && cd Haweye
make bootstrap && make up && make check
```

`make check` exiting 0 means the machine is not the problem any more: from there,
every mystery is in `docs/06-runbook.md`.

## If you are on a corporate laptop

| blocker | workaround |
|---|---|
| proxy / MITM TLS for `docker pull` and `pip` | `~/.docker/config.json` `proxies`, and `pip config set global.index-url <your mirror>`; the repo pins jars from `repo1.maven.org` via `MAVEN_BASE_URL` in `.env` → point it at your Artifactory |
| image pulls blocked entirely | build once on an allowed machine: `docker save haweye/spark-iceberg:3.5.1 -o spark.tar`, then `docker load -i spark.tar` |
| no admin rights (Docker Desktop needs them once) | rootless Podman + `podman compose`, or Codespaces/Gitpod (see the devcontainer note in `CONTRIBUTING.md`) |
| disk quotas | `make nuke` frees volumes; `docker system prune -a` frees images (it also deletes the built ones — you'll rebuild in 4 min) |
