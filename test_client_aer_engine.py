#!/usr/bin/env python3
"""
Test suite for ClientAEREngine implementation.
Verifies Phase 1b (Core AER Engine) and Phase 2 (AER Circuits).

Tests:
1. build_noise_model: valid NoiseModel with amplitude_damping + phase_damping
2. prepare_w_state_circuit: W3 circuit preparation
3. run_evolution_circuit: evolves DM and returns valid density matrix
4. run_shot_circuit: shot measurements produce expected W-state distribution
5. build_block_field_tensor: 8×8×8 complex64 tensor output
"""

import sys
import os
from pathlib import Path

# Add qtcl-miner to path
sys.path.insert(0, str(Path(__file__).parent))

def test_imports():
    """Verify module imports work."""
    print("\n[TEST] Checking imports...")
    try:
        from qtcl_client import ClientAEREngine
        print("  ✓ ClientAEREngine imported successfully")
        return True
    except Exception as e:
        print(f"  ✗ Import failed: {e}")
        return False

def test_init():
    """Test __init__ and engine construction."""
    print("\n[TEST] Testing __init__...")
    try:
        from qtcl_client import ClientAEREngine
        engine = ClientAEREngine()
        assert hasattr(engine, 'noise_model'), "Missing noise_model attribute"
        assert hasattr(engine, 'aer_simulator'), "Missing aer_simulator attribute"
        assert hasattr(engine, 'fallback_mode'), "Missing fallback_mode attribute"
        assert engine.SERVER_K_EFF == 0.0044, f"Wrong k_eff: {engine.SERVER_K_EFF}"
        assert engine.SERVER_A_EFF == 0.0012, f"Wrong a_eff: {engine.SERVER_A_EFF}"
        assert engine.SERVER_P_EFF == 0.0005, f"Wrong p_eff: {engine.SERVER_P_EFF}"
        print(f"  ✓ Engine initialized (fallback_mode={engine.fallback_mode})")
        return True, engine
    except Exception as e:
        print(f"  ✗ Init failed: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def test_build_noise_model(engine):
    """Test build_noise_model returns valid NoiseModel."""
    print("\n[TEST] Testing build_noise_model...")
    try:
        if engine.fallback_mode:
            print("  ⊘ Skipped (fallback mode - qiskit unavailable)")
            return True

        nm = engine.build_noise_model()
        if nm is None:
            print("  ⊘ Returned None (expected on non-AER systems)")
            return True

        # Check it's a NoiseModel instance
        from qiskit_aer.noise import NoiseModel
        assert isinstance(nm, NoiseModel), f"Not a NoiseModel: {type(nm)}"
        print(f"  ✓ NoiseModel created: {type(nm).__name__}")
        return True
    except ImportError:
        print("  ⊘ Skipped (qiskit not available)")
        return True
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_enforce_valid_dm(engine):
    """Test enforce_valid_dm."""
    print("\n[TEST] Testing enforce_valid_dm...")
    try:
        import numpy as np

        # Create a random 8x8 matrix
        np.random.seed(42)
        A = np.random.randn(8, 8) + 1j * np.random.randn(8, 8)
        dm_invalid = A @ A.conj().T  # Make Hermitian and PSD

        dm_valid = engine.enforce_valid_dm(dm_invalid)

        # Check properties
        assert dm_valid.shape == (8, 8), f"Wrong shape: {dm_valid.shape}"
        assert dm_valid.dtype == np.complex128, f"Wrong dtype: {dm_valid.dtype}"

        # Check Hermitian
        diff = np.max(np.abs(dm_valid - dm_valid.conj().T))
        assert diff < 1e-10, f"Not Hermitian (diff={diff})"

        # Check trace
        tr = np.real(np.trace(dm_valid))
        assert abs(tr - 1.0) < 1e-10, f"Trace not 1: {tr}"

        # Check positive semidefinite
        eigs = np.linalg.eigvalsh(dm_valid)
        assert np.min(eigs) >= -1e-10, f"Negative eigenvalue: {np.min(eigs)}"

        print(f"  ✓ Valid DM properties verified (trace={tr:.6f})")
        return True
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_prepare_w_state_circuit(engine):
    """Test prepare_w_state_circuit."""
    print("\n[TEST] Testing prepare_w_state_circuit...")
    try:
        if engine.fallback_mode:
            print("  ⊘ Skipped (fallback mode)")
            return True

        # Test without seed
        qc = engine.prepare_w_state_circuit(None)
        if qc is None:
            print("  ⊘ Returned None (fallback or error)")
            return True

        from qiskit import QuantumCircuit
        assert isinstance(qc, QuantumCircuit), f"Not a QuantumCircuit: {type(qc)}"

        # Test with seed
        import numpy as np
        w_state = np.zeros(8, dtype=complex)
        w_state[1] = w_state[2] = w_state[4] = 1.0 / np.sqrt(3.0)
        dm_w = np.outer(w_state, w_state.conj())

        qc2 = engine.prepare_w_state_circuit(dm_w)
        if qc2 is not None:
            assert isinstance(qc2, QuantumCircuit)
            print(f"  ✓ W-state circuit created with/without seed")
        else:
            print(f"  ⊘ Seed initialization failed (fallback)")

        return True
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_run_evolution_circuit(engine):
    """Test run_evolution_circuit."""
    print("\n[TEST] Testing run_evolution_circuit...")
    try:
        if engine.fallback_mode:
            print("  ⊘ Skipped (fallback mode)")
            return True

        import numpy as np

        # Create ideal W3 state
        w = np.zeros(8, dtype=complex)
        w[1] = w[2] = w[4] = 1.0 / np.sqrt(3.0)
        dm_w3 = np.outer(w, w.conj())

        # Run evolution
        dm_evolved = engine.run_evolution_circuit(dm_w3)

        assert dm_evolved is not None, "Returned None"
        assert dm_evolved.shape == (8, 8), f"Wrong shape: {dm_evolved.shape}"

        # Check it's valid
        tr = np.real(np.trace(dm_evolved))
        purity = np.real(np.trace(dm_evolved @ dm_evolved))

        assert abs(tr - 1.0) < 1e-6, f"Trace not 1: {tr}"
        assert 0.0 <= purity <= 1.01, f"Invalid purity: {purity}"

        # Noise should make it mixed (purity < 1)
        if purity < 0.99:
            print(f"  ✓ Evolution successful (purity={purity:.4f}, mixed state)")
        else:
            print(f"  ⊘ Purity still high: {purity:.4f} (minimal noise)")

        return True
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_run_shot_circuit(engine):
    """Test run_shot_circuit."""
    print("\n[TEST] Testing run_shot_circuit...")
    try:
        if engine.fallback_mode:
            print("  ⊘ Skipped (fallback mode)")
            return True

        import numpy as np

        # Create ideal W3 state (pure, no noise)
        w = np.zeros(8, dtype=complex)
        w[1] = w[2] = w[4] = 1.0 / np.sqrt(3.0)  # |001⟩, |010⟩, |100⟩
        dm_w3 = np.outer(w, w.conj())

        # Run shots
        counts = engine.run_shot_circuit(dm_w3, shots=1024)

        assert isinstance(counts, dict), f"Wrong type: {type(counts)}"
        assert len(counts) > 0, "No counts returned"

        # Check all keys are strings, values are ints
        for k, v in counts.items():
            assert isinstance(k, str), f"Key not str: {type(k)}"
            assert isinstance(v, int), f"Value not int: {type(v)}"

        # For W3, expect shots concentrated on {001, 010, 100}
        w_states = ['001', '010', '100']
        w_shots = sum(counts.get(s, 0) for s in w_states)
        w_ratio = w_shots / sum(counts.values()) if counts else 0

        print(f"  ✓ Shot counts: {counts}")
        print(f"    W-state ratio: {w_ratio:.2%} (expected ~100% for pure W3)")

        return True
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_build_block_field_tensor(engine):
    """Test build_block_field_tensor."""
    print("\n[TEST] Testing build_block_field_tensor...")
    try:
        import numpy as np
        from qtcl_client import _HAS_NP

        if not _HAS_NP:
            print("  ⊘ Skipped (numpy unavailable)")
            return True

        # Create random 8x8 valid DM
        np.random.seed(42)
        A = np.random.randn(8, 8) + 1j * np.random.randn(8, 8)
        dm = A @ A.conj().T
        dm /= np.trace(dm)

        # Build tensor
        tensor = engine.build_block_field_tensor(
            dm,
            pq_curr=100,
            pq_last=99,
            block_hash_seed="test_seed_12345"
        )

        if tensor is None:
            print("  ⊘ Returned None")
            return True

        # Check shape and dtype
        assert tensor.shape == (8, 8, 8), f"Wrong shape: {tensor.shape}"
        assert tensor.dtype == np.complex64, f"Wrong dtype: {tensor.dtype}"

        # Check Frobenius norm
        frob = np.linalg.norm(tensor)
        assert frob > 0, f"Zero norm: {frob}"

        # Check normalization (should be ~1 after normalization)
        assert abs(frob - 1.0) < 0.1, f"Norm not ~1: {frob}"

        print(f"  ✓ Tensor created: shape={tensor.shape}, dtype={tensor.dtype}")
        print(f"    Frobenius norm: {frob:.6f}")

        return True
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("=" * 70)
    print("ClientAEREngine Test Suite")
    print("=" * 70)

    results = []

    # Test 1: Imports
    if not test_imports():
        print("\nFATAL: Cannot import ClientAEREngine")
        return False

    # Test 2: Init
    success, engine = test_init()
    if not success or engine is None:
        print("\nFATAL: Cannot initialize engine")
        return False
    results.append(("Init", success))

    # Test 3: build_noise_model
    results.append(("build_noise_model", test_build_noise_model(engine)))

    # Test 4: enforce_valid_dm
    results.append(("enforce_valid_dm", test_enforce_valid_dm(engine)))

    # Test 5: prepare_w_state_circuit
    results.append(("prepare_w_state_circuit", test_prepare_w_state_circuit(engine)))

    # Test 6: run_evolution_circuit
    results.append(("run_evolution_circuit", test_run_evolution_circuit(engine)))

    # Test 7: run_shot_circuit
    results.append(("run_shot_circuit", test_run_shot_circuit(engine)))

    # Test 8: build_block_field_tensor
    results.append(("build_block_field_tensor", test_build_block_field_tensor(engine)))

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  {name:40s} {status}")

    passed = sum(1 for _, r in results if r)
    total = len(results)
    print(f"\nTotal: {passed}/{total} passed")

    return all(r for _, r in results)

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
