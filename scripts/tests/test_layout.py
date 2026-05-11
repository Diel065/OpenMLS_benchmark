#!/usr/bin/env python3
"""
Tests for the hybrid layout calculation in generate_compose.py.

These tests verify the layout formula matches the specification in MANIFEST.md.
"""
from __future__ import annotations

import math
import sys
import json
import tempfile
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

import generate_compose
from generate_compose import (
    compute_hybrid_layout,
    select_singleton_ids,
    worker_id,
    build_hybrid_layout,
    build_legacy_layout,
)


class FakeArgs:
    def __init__(
        self,
        workers: int,
        singleton_min_count: int = 16,
        singleton_fraction: float = 0.125,
        packed_clients_per_container: int = 16,
        singleton_selection_seed: int = 1,
        singleton_selection_strategy: str = "stratified-random",
    ):
        self.workers = workers
        self.singleton_min_count = singleton_min_count
        self.singleton_fraction = singleton_fraction
        self.packed_clients_per_container = packed_clients_per_container
        self.singleton_selection_seed = singleton_selection_seed
        self.singleton_selection_strategy = singleton_selection_strategy
        self.worker_layout_mode = "hybrid"
        self.base_worker_port = 8081
        self.run_id = "test-run"
        self.output_dir = "benchmark_output"


def test_layout_16_workers():
    result = compute_hybrid_layout(16, 16, 0.125, 16)
    assert result["singleton_count"] == 16, f"Expected 16 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 0, f"Expected 0 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 0, f"Expected 0 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 16, f"Expected 16 physical, got {result['physical_worker_count']}"
    print("PASS: 16 workers → 16 singleton, 0 packed, 16 physical")


def test_layout_64_workers():
    result = compute_hybrid_layout(64, 16, 0.125, 16)
    assert result["singleton_count"] == 16, f"Expected 16 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 48, f"Expected 48 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 3, f"Expected 3 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 19, f"Expected 19 physical, got {result['physical_worker_count']}"
    print("PASS: 64 workers → 16 singleton, 48 packed, 3 containers, 19 physical")


def test_layout_128_workers():
    result = compute_hybrid_layout(128, 16, 0.125, 16)
    assert result["singleton_count"] == 16, f"Expected 16 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 112, f"Expected 112 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 7, f"Expected 7 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 23, f"Expected 23 physical, got {result['physical_worker_count']}"
    print("PASS: 128 workers → 16 singleton, 112 packed, 7 containers, 23 physical")


def test_layout_256_workers():
    result = compute_hybrid_layout(256, 16, 0.125, 16)
    assert result["singleton_count"] == 32, f"Expected 32 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 224, f"Expected 224 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 14, f"Expected 14 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 46, f"Expected 46 physical, got {result['physical_worker_count']}"
    print("PASS: 256 workers → 32 singleton, 224 packed, 14 containers, 46 physical")


def test_layout_512_workers():
    result = compute_hybrid_layout(512, 16, 0.125, 16)
    assert result["singleton_count"] == 64, f"Expected 64 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 448, f"Expected 448 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 28, f"Expected 28 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 92, f"Expected 92 physical, got {result['physical_worker_count']}"
    print("PASS: 512 workers → 64 singleton, 448 packed, 28 containers, 92 physical")


def test_layout_1024_workers():
    result = compute_hybrid_layout(1024, 16, 0.125, 16)
    assert result["singleton_count"] == 128, f"Expected 128 singletons, got {result['singleton_count']}"
    assert result["packed_client_count"] == 896, f"Expected 896 packed, got {result['packed_client_count']}"
    assert result["packed_container_count"] == 56, f"Expected 56 packed containers, got {result['packed_container_count']}"
    assert result["physical_worker_count"] == 184, f"Expected 184 physical, got {result['physical_worker_count']}"
    print("PASS: 1024 workers → 128 singleton, 896 packed, 56 containers, 184 physical")


def test_singleton_selection_includes_00001():
    for total in [16, 32, 64, 128, 256, 512, 1024]:
        layout = compute_hybrid_layout(total, 16, 0.125, 16)
        s_count = layout["singleton_count"]
        ids = select_singleton_ids(total, s_count, seed=1, strategy="stratified-random")
        assert "00001" in ids, f"00001 must always be singleton for total={total}"

        ids_evenly = select_singleton_ids(total, s_count, seed=1, strategy="evenly-spaced")
        assert "00001" in ids_evenly, f"00001 must always be singleton (evenly-spaced) for total={total}"
    print("PASS: singleton selection always includes 00001")


def test_singleton_selection_deterministic():
    for total in [64, 256, 1024]:
        layout = compute_hybrid_layout(total, 16, 0.125, 16)
        s_count = layout["singleton_count"]

        ids_a = select_singleton_ids(total, s_count, seed=42, strategy="stratified-random")
        ids_b = select_singleton_ids(total, s_count, seed=42, strategy="stratified-random")
        assert ids_a == ids_b, f"Selection must be deterministic for seed=42, total={total}"
    print("PASS: singleton selection is deterministic")


def test_singleton_selection_count_matches():
    for total in [1, 8, 16, 32, 64, 128, 256, 512, 1024]:
        layout = compute_hybrid_layout(total, 16, 0.125, 16)
        s_count = layout["singleton_count"]
        ids = select_singleton_ids(total, s_count, seed=1, strategy="stratified-random")
        assert len(ids) == s_count, f"Expected {s_count} singleton IDs, got {len(ids)} for total={total}"
    print("PASS: singleton selection count matches layout")


def test_all_clients_covered():
    for total in [32, 64, 128, 256, 1024]:
        layout = compute_hybrid_layout(total, 16, 0.125, 16)
        s_count = layout["singleton_count"]
        singleton_ids = set(select_singleton_ids(total, s_count, seed=1, strategy="stratified-random"))

        all_ids = set(worker_id(i) for i in range(1, total + 1))
        packed_ids = all_ids - singleton_ids

        assert len(singleton_ids) + len(packed_ids) == total, f"Not all clients covered for total={total}"
        assert len(singleton_ids & packed_ids) == 0, f"Overlap between singleton and packed for total={total}"
    print("PASS: all clients are covered (singleton or packed)")


def test_hybrid_layout_build():
    for total in [32, 64, 128, 1024]:
        args = FakeArgs(total)
        layout = compute_hybrid_layout(total, args.singleton_min_count, args.singleton_fraction, args.packed_clients_per_container)
        singleton_ids = select_singleton_ids(total, layout["singleton_count"], args.singleton_selection_seed, args.singleton_selection_strategy)

        all_ids = [worker_id(i) for i in range(1, total + 1)]
        singleton_set = set(singleton_ids)
        packed_ids = [cid for cid in all_ids if cid not in singleton_set]

        clients, physical_workers = build_hybrid_layout(args, singleton_ids, packed_ids, layout)

        assert len(clients) == total, f"Expected {total} client entries, got {len(clients)}"
        assert len(physical_workers) == layout["physical_worker_count"], (
            f"Expected {layout['physical_worker_count']} physical workers, got {len(physical_workers)}"
        )

        singleton_clients = [c for c in clients if c.container_mode == "singleton"]
        packed_clients = [c for c in clients if c.container_mode == "packed"]
        assert len(singleton_clients) == layout["singleton_count"]
        assert len(packed_clients) == layout["packed_client_count"]

        all_profile_enabled = [c for c in clients if c.profile_enabled]
        assert len(all_profile_enabled) == layout["singleton_count"], "Only singletons should have profile_enabled=true"
    print("PASS: hybrid layout build produces correct structure")


def test_legacy_layout_build():
    for total in [1, 8, 16]:
        args = FakeArgs(total)
        args.worker_layout_mode = "one-container-per-client"
        clients, physical_workers = build_legacy_layout(args)

        assert len(clients) == total
        assert len(physical_workers) == total

        for c in clients:
            assert c.container_mode == "singleton"
            assert c.profile_enabled is True
    print("PASS: legacy layout build produces correct structure")


def main() -> int:
    tests = [
        test_layout_16_workers,
        test_layout_64_workers,
        test_layout_128_workers,
        test_layout_256_workers,
        test_layout_512_workers,
        test_layout_1024_workers,
        test_singleton_selection_includes_00001,
        test_singleton_selection_deterministic,
        test_singleton_selection_count_matches,
        test_all_clients_covered,
        test_hybrid_layout_build,
        test_legacy_layout_build,
    ]

    passed = 0
    failed = 0

    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except AssertionError as e:
            print(f"FAIL: {test_fn.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"ERROR: {test_fn.__name__}: {e}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
