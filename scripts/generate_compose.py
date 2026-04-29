#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Generate a docker-compose file plus worker lists for many MLS workers."
        )
    )
    p.add_argument("--workers", type=int, required=True, help="Number of worker services to generate")
    p.add_argument("--run-id", default="compose-generated-001", help="Default run id baked into the compose env")
    p.add_argument("--scenario", default="http-staircase-compose", help="Default scenario baked into the compose env")
    p.add_argument("--output-dir", default="benchmark_output", help="Host results directory")
    p.add_argument("--compose-out", default="docker-compose.generated.yml", help="Generated compose file path")
    p.add_argument("--workers-out", default="workers.txt", help="Generated internal worker list path")
    p.add_argument("--workers-host-out", default="workers.host.txt", help="Generated host worker list path")
    p.add_argument("--project-name", default="mls-benchmark", help="Compose project name")
    p.add_argument("--base-worker-port", type=int, default=8081, help="First published host port for workers")
    p.add_argument("--ds-port", type=int, default=3000, help="Published DS port")
    p.add_argument("--relay-port", type=int, default=4000, help="Published relay port")
    return p


def worker_id(i: int) -> str:
    return f"{i:05d}"


def service_name(i: int) -> str:
    return f"worker-{worker_id(i)}"


def validate_args(args: argparse.Namespace) -> None:
    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")
    if not (1 <= args.base_worker_port <= 65535):
        raise SystemExit("--base-worker-port must be between 1 and 65535")
    if not (1 <= args.ds_port <= 65535):
        raise SystemExit("--ds-port must be between 1 and 65535")
    if not (1 <= args.relay_port <= 65535):
        raise SystemExit("--relay-port must be between 1 and 65535")

    last_port = args.base_worker_port + args.workers - 1
    if last_port > 65535:
        raise SystemExit(
            f"Worker host ports would exceed 65535: last port would be {last_port}"
        )


def generate_compose_text(args: argparse.Namespace) -> str:
    lines: list[str] = []

    lines.append(f'name: {args.project_name}')
    lines.append("")
    lines.append("x-worker-common: &worker-common")
    lines.append("  image: mls-worker")
    lines.append("  environment:")
    lines.append(f"    OPENMLS_PROFILE_RUN_ID: {args.run_id}")
    lines.append(f"    OPENMLS_PROFILE_SCENARIO: {args.scenario}")
    lines.append("  depends_on:")
    lines.append("    - ds")
    lines.append("    - relay")
    lines.append("  volumes:")
    lines.append(f"    - ./{args.output_dir}:/results")
    lines.append("")
    lines.append("services:")
    lines.append("  ds:")
    lines.append("    image: mls-ds")
    lines.append(f'    command: ["--listen-addr", "0.0.0.0:{args.ds_port}"]')
    lines.append("    ports:")
    lines.append(f'      - "{args.ds_port}:{args.ds_port}"')
    lines.append("")
    lines.append("  relay:")
    lines.append("    image: mls-relay")
    lines.append(f'    command: ["--listen-addr", "0.0.0.0:{args.relay_port}"]')
    lines.append("    ports:")
    lines.append(f'      - "{args.relay_port}:{args.relay_port}"')

    for i in range(1, args.workers + 1):
        wid = worker_id(i)
        svc = service_name(i)
        host_port = args.base_worker_port + i - 1

        lines.append("")
        lines.append(f"  {svc}:")
        lines.append("    <<: *worker-common")
        lines.append("    command:")
        lines.append('      - "--name"')
        lines.append(f'      - "{wid}"')
        lines.append('      - "--ds-url"')
        lines.append(f'      - "http://ds:{args.ds_port}"')
        lines.append('      - "--relay-url"')
        lines.append(f'      - "http://relay:{args.relay_port}"')
        lines.append('      - "--listen-addr"')
        lines.append('      - "0.0.0.0:8080"')
        lines.append("    environment:")
        lines.append(f"      OPENMLS_PROFILE_RUN_ID: {args.run_id}")
        lines.append(f"      OPENMLS_PROFILE_SCENARIO: {args.scenario}")
        lines.append(f"      OPENMLS_PROFILE_PATH: /results/{args.run_id}/client-{wid}.jsonl")
        lines.append("    ports:")
        lines.append(f'      - "{host_port}:8080"')

    return "\n".join(lines) + "\n"


def generate_workers_internal(args: argparse.Namespace) -> str:
    lines = []
    for i in range(1, args.workers + 1):
        wid = worker_id(i)
        svc = service_name(i)
        lines.append(f"{wid}=http://{svc}:8080")
    return "\n".join(lines) + "\n"


def generate_workers_host(args: argparse.Namespace) -> str:
    lines = []
    for i in range(1, args.workers + 1):
        wid = worker_id(i)
        host_port = args.base_worker_port + i - 1
        lines.append(f"{wid}=http://127.0.0.1:{host_port}")
    return "\n".join(lines) + "\n"


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args)

    compose_out = Path(args.compose_out)
    workers_out = Path(args.workers_out)
    workers_host_out = Path(args.workers_host_out)

    write_text(compose_out, generate_compose_text(args))
    write_text(workers_out, generate_workers_internal(args))
    write_text(workers_host_out, generate_workers_host(args))

    print(f"Wrote {compose_out}")
    print(f"Wrote {workers_out}")
    print(f"Wrote {workers_host_out}")
    print("")
    print("What you generated:")
    print(f"- Compose file with {args.workers} workers")
    print("- workers.txt for an in-network runner")
    print("- workers.host.txt for the current host-runner workflow")


if __name__ == "__main__":
    main()