#!/usr/bin/env python3
"""
Tests for the external devices module.

Tests run_id validation, config parsing, and layout entry generation.
ADB/SSH backend tests require actual devices and are skipped in CI.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

import external_devices
from external_devices import (
    validate_run_id,
    load_devices_config,
    DeviceConfig,
    build_external_device_layout_entry,
)


def test_validate_run_id_accepts_valid():
    valid_ids = ["run-001", "test.123", "bench_2026", "a", "0", "A.B_C"]
    for rid in valid_ids:
        validate_run_id(rid)
    print("PASS: validate_run_id accepts valid IDs")


def test_validate_run_id_rejects_empty():
    try:
        validate_run_id("")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass
    print("PASS: validate_run_id rejects empty string")


def test_validate_run_id_rejects_path_traversal():
    bad_ids = ["/", ".", "..", "foo/bar", "../../etc"]
    for rid in bad_ids:
        try:
            validate_run_id(rid)
            assert False, f"Should have raised ValueError for '{rid}'"
        except ValueError:
            pass
    print("PASS: validate_run_id rejects path traversal characters")


def test_validate_run_id_rejects_special_chars():
    bad_ids = ["hello world", "foo$bar", "foo|bar", "foo;bar"]
    for rid in bad_ids:
        try:
            validate_run_id(rid)
            assert False, f"Should have raised ValueError for '{rid}'"
        except ValueError:
            pass
    print("PASS: validate_run_id rejects special characters")


def test_load_devices_config(tmp_path: Path):
    yaml_content = """
devices:
  - id: test-device-01
    enabled: true
    kind: test_board
    connection:
      type: adb
      serial: "test-serial-123"
    transport:
      type: usb_rndis
      device_ip: "172.32.0.93"
      host_ip: "172.32.0.98"
      worker_port: 8080
    target:
      arch: "armv7l"
      rust_target: "armv7-unknown-linux-musleabihf"
      binary: "target/armv7-unknown-linux-musleabihf/minsize/worker"
    worker:
      id: "pico-plus-00001"
      mode: "singleton"
      remote_binary: "/worker"
      remote_results_root: "/results/signal"
      remote_tmp: "/tmp/signal-benchmark"
    metadata:
      execution_backend: "real_device"
      device_kind: "test_board"
      transport: "usb_rndis"
      access_backend: "adb"
"""
    config_file = tmp_path / "devices.yaml"
    config_file.write_text(yaml_content, encoding="utf-8")

    configs = load_devices_config(config_file)
    assert len(configs) == 1, f"Expected 1 device config, got {len(configs)}"

    dev = configs[0]
    assert dev.id == "test-device-01"
    assert dev.enabled is True
    assert dev.kind == "test_board"
    assert dev.connection["type"] == "adb"
    assert dev.transport["device_ip"] == "172.32.0.93"
    assert dev.target["arch"] == "armv7l"
    assert dev.worker["id"] == "pico-plus-00001"
    assert dev.metadata["execution_backend"] == "real_device"
    print("PASS: load_devices_config parses YAML correctly")


def test_build_external_device_layout_entry():
    config = DeviceConfig(
        id="test-board-01",
        enabled=True,
        kind="test_board",
        connection={"type": "adb", "serial": "test-serial"},
        transport={"type": "usb_rndis", "device_ip": "10.0.0.1", "host_ip": "10.0.0.2", "worker_port": 8080},
        target={"arch": "armv7l", "rust_target": "armv7-unknown-linux-musleabihf", "binary": "/worker"},
        worker={"id": "ext-00001", "mode": "singleton", "remote_binary": "/worker",
                "remote_results_root": "/results/signal", "remote_tmp": "/tmp"},
        metadata={"execution_backend": "real_device", "device_kind": "test_board",
                  "transport": "usb_rndis", "access_backend": "adb"},
    )

    client_entry, phys_entry, worker_line = build_external_device_layout_entry(
        config,
        transport_ip="10.0.0.1",
        host_ip="10.0.0.2",
        worker_port=8080,
        ds_port=3000,
        relay_port=4000,
        run_id="test-run-001",
    )

    # Check worker file line
    assert worker_line == "ext-00001=http://10.0.0.1:8080", f"Unexpected worker line: {worker_line}"

    # Check client entry
    assert client_entry["client_id"] == "ext-00001"
    assert client_entry["execution_backend"] == "real_device"
    assert client_entry["device_kind"] == "test_board"
    assert client_entry["transport"] == "usb_rndis"
    assert client_entry["access_backend"] == "adb"
    assert client_entry["arch"] == "armv7l"
    assert client_entry["rust_target"] == "armv7-unknown-linux-musleabihf"

    # Check physical worker entry
    assert phys_entry["physical_worker_id"] == "test-board-01"
    assert phys_entry["execution_backend"] == "real_device"
    assert phys_entry["device_kind"] == "test_board"

    print("PASS: build_external_device_layout_entry generates correct metadata")


def test_config_defaults():
    """Test that missing optional fields get sensible defaults."""
    config = DeviceConfig(
        id="minimal-device",
        enabled=True,
        kind="",
        connection={"type": "adb"},
        transport={},
        target={},
        worker={"id": "min-00001", "mode": "singleton", "remote_binary": "/worker",
                "remote_results_root": "/results/signal", "remote_tmp": "/tmp"},
    )

    client_entry, phys_entry, worker_line = build_external_device_layout_entry(
        config,
        transport_ip="172.32.0.93",
        host_ip="172.32.0.98",
        worker_port=8080,
        ds_port=3000,
        relay_port=4000,
        run_id="test-run",
    )

    assert worker_line == "min-00001=http://172.32.0.93:8080"
    assert client_entry["execution_backend"] == "real_device"
    assert client_entry["device_kind"] == ""
    assert client_entry["arch"] == "armv7l", f"Expected 'armv7l', got '{client_entry['arch']}'"
    assert client_entry["rust_target"] == ""
    print("PASS: config defaults work correctly")


def main() -> int:
    import shutil

    tmp_dir = Path(tempfile.mkdtemp(prefix="test_external_devices_"))
    try:
        tests = [
            ("validate_run_id accepts valid", lambda: test_validate_run_id_accepts_valid()),
            ("validate_run_id rejects empty", lambda: test_validate_run_id_rejects_empty()),
            ("validate_run_id rejects path traversal", lambda: test_validate_run_id_rejects_path_traversal()),
            ("validate_run_id rejects special chars", lambda: test_validate_run_id_rejects_special_chars()),
            ("load_devices_config", lambda: test_load_devices_config(tmp_dir)),
            ("build_external_device_layout_entry", lambda: test_build_external_device_layout_entry()),
            ("config defaults", lambda: test_config_defaults()),
        ]

        passed = 0
        failed = 0

        for name, fn in tests:
            try:
                fn()
                passed += 1
            except AssertionError as e:
                print(f"FAIL: {name}: {e}")
                failed += 1
            except Exception as e:
                print(f"ERROR: {name}: {type(e).__name__}: {e}")
                failed += 1

        print(f"\n{passed} passed, {failed} failed")
        return 1 if failed > 0 else 0
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
