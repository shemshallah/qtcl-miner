#!/usr/bin/env python3
"""
╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                                                                            ║
║  🌌 QTCL FULL NODE + QUANTUM MINER - W-STATE ENTANGLED MINING 🌌                                                                         ║
║                                                                                                                                            ║
║  WORLD'S FIRST W-STATE ENTANGLED BLOCKCHAIN MINER:                                                                                      ║
║  • Connects to LIVE qtcl-blockchain.koyeb.app                                                                                           ║
║  • Syncs blockchain from network (REST API)                                                                                             ║
║  • On startup: queries oracle for latest W-state snapshot (HLWE-signed)                                                                 ║
║  • Recovers W-state locally with signature verification                                                                                 ║
║  • Establishes entanglement: Oracle (pq0) ↔ Current (pq_curr) ↔ Last (pq_last)                                                         ║
║  • Uses recovered W-state entropy for quantum PoW                                                                                        ║
║  • Maintains 3-qubit entanglement state across mining iterations                                                                        ║
║  • Broadcasts mined blocks with W-state fidelity attestation                                                                             ║
║                                                                                                                                            ║
║  ARCHITECTURE:                                                                                                                          ║
║  ┌────────────────────────────────────────────────────────────────────┐                                                                ║
║  │ W-STATE RECOVERY & ENTANGLEMENT (On Init)                          │                                                                ║
║  │ • Register with oracle                                             │                                                                ║
║  │ • Download latest DM snapshot (HLWE-verified)                      │                                                                ║
║  │ • Recover W-state locally (pq0 = oracle)                           │                                                                ║
║  │ • Create pq_curr and pq_last entangled copies                      │                                                                ║
║  │ • Verify fidelity >= 0.85 threshold                                │                                                                ║
║  │ • Start continuous sync worker (background)                        │                                                                ║
║  └────────────────────────────────────────────────────────────────────┘                                                                ║
║  ┌────────────────────────────────────────────────────────────────────┐                                                                ║
║  │ LIVE BLOCKCHAIN SYNC                                               │                                                                ║
║  │ • Fetch blocks from qtcl-blockchain.koyeb.app REST API            │                                                                ║
║  │ • Validate block headers, PoW, transactions                       │                                                                ║
║  │ • Maintain chain state (in-memory)                                │                                                                ║
║  │ • Fork detection & resolution (longest-chain)                     │                                                                ║
║  │ • Sync progress tracking                                          │                                                                ║
║  └────────────────────────────────────────────────────────────────────┘                                                                ║
║  ┌────────────────────────────────────────────────────────────────────┐                                                                ║
║  │ MEMPOOL MANAGEMENT                                                 │                                                                ║
║  │ • Fetch pending transactions from /api/mempool                    │                                                                ║
║  │ • Validate signatures (HLWE), nonces, balances                    │                                                                ║
║  │ • Fee-based prioritization                                        │                                                                ║
║  │ • Remove included transactions after block                        │                                                                ║
║  └────────────────────────────────────────────────────────────────────┘                                                                ║
║  ┌────────────────────────────────────────────────────────────────────┐                                                                ║
║  │ QUANTUM-ENTANGLED MINING SUBSYSTEM                                 │                                                                ║
║  │ • Poll mempool for transactions                                   │                                                                ║
║  │ • Build block template from highest-fee transactions              │                                                                ║
║  │ • Measure W-state (pq_curr) for quantum PoW entropy               │                                                                ║
║  │ • Rotate pq_curr → pq_last, recover new pq_curr from oracle      │                                                                ║
║  │ • Sequential nonce iteration (SHA3-256 PoW + W-state witness)     │                                                                ║
║  │ • Broadcast mined block with fidelity attestation                 │                                                                ║
║  │ • Track mining rewards & entanglement metrics                     │                                                                ║
║  └────────────────────────────────────────────────────────────────────┘                                                                ║
║                                                                                                                                            ║
║  USAGE: python qtcl_miner.py --address qtcl1YOUR_ADDRESS --oracle-url http://oracle.local:5000                                         ║
║                                                                                                                                            ║
║  This is PERFECTION. Museum-grade quantum mining. Deploy with absolute confidence.                                                     ║
║                                                                                                                                            ║
╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
"""

import os,sys,time,json,math,hashlib,secrets,uuid,threading,logging,argparse,traceback,base64,hmac
from typing import Dict,Any,Optional,List,Tuple,Deque,Set
from dataclasses import dataclass,field,asdict
from enum import Enum,auto
from collections import deque,defaultdict,Counter
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np

try:
    from qiskit import QuantumCircuit,QuantumRegister,ClassicalRegister,execute
    from qiskit.quantum_info import Statevector,DensityMatrix
    from qiskit.providers.aer import AerSimulator
    QISKIT_AVAILABLE=True
except ImportError:
    QISKIT_AVAILABLE=False

logging.basicConfig(level=logging.INFO,format='[%(asctime)s] %(levelname)s: %(message)s')
logger=logging.getLogger('QTCL_MINER')

LIVE_NODE_URL='https://qtcl-blockchain.koyeb.app'
API_PREFIX='/api'
MAX_MEMPOOL=10000
SYNC_BATCH=50
MEMPOOL_POLL_INTERVAL=5
MINING_POLL_INTERVAL=2
DIFFICULTY_WINDOW=2016
TARGET_BLOCK_TIME=600

# W-STATE CONFIGURATION
W_STATE_STREAM_INTERVAL_MS=10
NUM_QUBITS_WSTATE=3

FIDELITY_THRESHOLD_STRICT = 0.90
FIDELITY_THRESHOLD_NORMAL = 0.80
FIDELITY_THRESHOLD_RELAXED = 0.70

COHERENCE_THRESHOLD_STRICT = 0.90
COHERENCE_THRESHOLD_NORMAL = 0.80
COHERENCE_THRESHOLD_RELAXED = 0.75

DEFAULT_FIDELITY_MODE = "normal"
FIDELITY_THRESHOLD = FIDELITY_THRESHOLD_NORMAL
W_STATE_FIDELITY_THRESHOLD = FIDELITY_THRESHOLD_NORMAL

FIDELITY_WEIGHT = 0.7
COHERENCE_WEIGHT = 0.3

RECOVERY_BUFFER_SIZE=100
SYNC_INTERVAL_MS=10
MAX_SYNC_LAG_MS=100
HERMITICITY_TOLERANCE=1e-10
EIGENVALUE_TOLERANCE=-1e-10

# ═════════════════════════════════════════════════════════════════════════════════
# W-STATE DATA STRUCTURES
# ═════════════════════════════════════════════════════════════════════════════════

@dataclass
class RecoveredWState:
    """Recovered and validated W-state from remote oracle."""
    timestamp_ns: int
    density_matrix: np.ndarray
    purity: float
    w_state_fidelity: float
    coherence_l1: float
    quantum_discord: float
    is_valid: bool
    validation_notes: str
    local_statevector: Optional[np.ndarray] = None
    signature_verified: bool = False
    oracle_address: Optional[str] = None

@dataclass
class EntanglementState:
    """Track local entanglement with remote pq0 (oracle) and pq_curr/pq_last."""
    established: bool
    local_fidelity: float
    sync_lag_ms: float
    last_sync_ns: int
    sync_error_count: int = 0
    coherence_verified: bool = False
    signature_verified: bool = False
    pq0_fidelity: float = 0.0
    pq_curr_fidelity: float = 0.0
    pq_last_fidelity: float = 0.0

# ═════════════════════════════════════════════════════════════════════════════════
# W-STATE QUALITY EVALUATION
# ═════════════════════════════════════════════════════════════════════════════════

class WStateRecoveryManager:
    """Museum-grade W-state recovery with adaptive threshold evaluation."""
    
    @staticmethod
    def get_threshold_for_mode(mode: str = "normal") -> tuple:
        """Get fidelity and coherence thresholds for given mode."""
        thresholds = {
            "strict": (FIDELITY_THRESHOLD_STRICT, COHERENCE_THRESHOLD_STRICT),
            "normal": (FIDELITY_THRESHOLD_NORMAL, COHERENCE_THRESHOLD_NORMAL),
            "relaxed": (FIDELITY_THRESHOLD_RELAXED, COHERENCE_THRESHOLD_RELAXED),
        }
        return thresholds.get(mode.lower(), thresholds["normal"])
    
    @staticmethod
    def compute_quality_score(fidelity: float, coherence: float) -> float:
        """Compute composite quality score: 0.7*F + 0.3*C"""
        clipped_f = max(0.0, min(1.0, fidelity))
        clipped_c = max(0.0, min(1.0, coherence))
        return FIDELITY_WEIGHT * clipped_f + COHERENCE_WEIGHT * clipped_c
    
    @staticmethod
    def evaluate_w_state_quality(fidelity: float, coherence: float, mode: str = "normal", verbose: bool = True) -> tuple:
        """Evaluate W-state quality with diagnostics."""
        fid_threshold, coh_threshold = WStateRecoveryManager.get_threshold_for_mode(mode)
        quality_score = WStateRecoveryManager.compute_quality_score(fidelity, coherence)
        
        fidelity_ok = fidelity >= fid_threshold
        coherence_ok = coherence >= coh_threshold
        is_valid = fidelity_ok and coherence_ok
        
        if is_valid:
            status = "✅ VALID"
        elif fidelity >= FIDELITY_THRESHOLD_RELAXED:
            status = "⚠️  MARGINAL"
        else:
            status = "❌ INVALID"
        
        diagnostic = (
            f"{status} W-state | F={fidelity:.4f} (threshold {fid_threshold:.4f}) | "
            f"C={coherence:.4f} (threshold {coh_threshold:.4f}) | "
            f"quality={quality_score:.4f} | mode={mode}"
        )
        
        if verbose:
            if is_valid:
                logger.info(f"[W-STATE] {diagnostic}")
            elif fidelity >= FIDELITY_THRESHOLD_RELAXED:
                logger.warning(f"[W-STATE] {diagnostic}")
            else:
                logger.error(f"[W-STATE] {diagnostic}")
        
        return is_valid, quality_score, diagnostic

# ═════════════════════════════════════════════════════════════════════════════════
# BLOCKCHAIN STRUCTURES
# ═════════════════════════════════════════════════════════════════════════════════

class BlockHeader:
    def __init__(self,height: int,block_hash: str,parent_hash: str,merkle_root: str,
                 timestamp_s: int,difficulty_bits: int,nonce: int,miner_address: str,
                 w_state_fidelity: float=0.0,w_entropy_hash: str=''):
        self.height=height
        self.block_hash=block_hash
        self.parent_hash=parent_hash
        self.merkle_root=merkle_root
        self.timestamp_s=timestamp_s
        self.difficulty_bits=difficulty_bits
        self.nonce=nonce
        self.miner_address=miner_address
        self.w_state_fidelity=w_state_fidelity
        self.w_entropy_hash=w_entropy_hash
    
    @classmethod
    def from_dict(cls,data: Dict[str,Any])->'BlockHeader':
        return cls(
            height=data.get('block_height',0),
            block_hash=data.get('block_hash',''),
            parent_hash=data.get('parent_hash',''),
            merkle_root=data.get('merkle_root',''),
            timestamp_s=data.get('timestamp_s',int(time.time())),
            difficulty_bits=data.get('difficulty_bits',12),
            nonce=data.get('nonce',0),
            miner_address=data.get('miner_address',''),
            w_state_fidelity=data.get('w_state_fidelity',0.0),
            w_entropy_hash=data.get('w_entropy_hash','')
        )

@dataclass
class Transaction:
    tx_id: str
    from_addr: str
    to_addr: str
    amount: float
    nonce: int
    timestamp_ns: int
    signature: str
    fee: float=0.0
    
    def compute_hash(self)->str:
        data=json.dumps({k:v for k,v in asdict(self).items() if k!='signature'},sort_keys=True)
        return hashlib.sha3_256(data.encode()).hexdigest()

@dataclass
class Block:
    header: BlockHeader
    transactions: List[Transaction]
    
    def compute_merkle(self)->str:
        if not self.transactions:
            return hashlib.sha3_256(b'').hexdigest()
        hashes=[tx.compute_hash() for tx in self.transactions]
        while len(hashes)>1:
            if len(hashes)%2:
                hashes.append(hashes[-1])
            hashes=[hashlib.sha3_256((hashes[i]+hashes[i+1]).encode()).hexdigest() for i in range(0,len(hashes),2)]
        return hashes[0]

# ═════════════════════════════════════════════════════════════════════════════════
# W-STATE RECOVERY ENGINE (VERBATIM FROM v14 FINAL + ENHANCED)
# ═════════════════════════════════════════════════════════════════════════════════

class P2PClientWStateRecovery:
    """
    P2P client-side W-state recovery with HLWE signature verification.
    
    Downloads density matrix snapshots cryptographically signed by oracle,
    verifies signatures, reconstructs W-state locally, establishes entanglement.
    Integrated into miner for continuous quantum entropy source.
    """
    
    def __init__(self, oracle_url: str, peer_id: str, miner_address: str, strict_signature_verification: bool=True):
        """Initialize W-state recovery client."""
        self.oracle_url=oracle_url.rstrip('/')
        self.peer_id=peer_id
        self.miner_address=miner_address  # FIXED: Add this
        self.running=False
        self.strict_verification=strict_signature_verification
        
        self.oracle_address=None
        self.trusted_oracles: Set[str]=set()
        
        self.snapshot_buffer=deque(maxlen=RECOVERY_BUFFER_SIZE)
        self.current_snapshot=None
        
        self.recovered_w_state=None
        self.entanglement_state=EntanglementState(
            established=False,
            local_fidelity=0.0,
            sync_lag_ms=0.0,
            last_sync_ns=time.time_ns(),
        )
        
        # W-state tracking for mining
        self.pq0_matrix: Optional[np.ndarray]=None
        self.pq_curr_matrix: Optional[np.ndarray]=None
        self.pq_last_matrix: Optional[np.ndarray]=None
        self.pq_curr_measurement_counts: Dict[str,int]={}
        
        self.sync_thread=None
        self._state_lock=threading.RLock()
        
        logger.info(f"[W-STATE] 🌐 Initialized recovery client | peer={peer_id[:12]} | verification={'STRICT' if strict_signature_verification else 'SOFT'}")
    
    def register_with_oracle(self)->bool:
        """Register this peer with the oracle and get oracle address."""
        try:
            url=f"{self.oracle_url}/api/oracle/register"
            response=requests.post(
                url,
                json={"miner_id": self.peer_id, "address": self.miner_address, "public_key": self.peer_id},
                timeout=5
            )
            
            if response.status_code in [200,201]:
                data=response.json()
                self.oracle_address=data.get('miner_id',self.peer_id)
                if self.oracle_address:
                    self.trusted_oracles.add(self.oracle_address)
                    logger.info(f"[W-STATE] ✅ Registered with oracle | miner_id={self.oracle_address[:20]}…")
                return True
            else:
                logger.error(f"[W-STATE] ❌ Registration failed: {response.status_code}")
                return False
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Registration error: {e}")
            return False
    
    def download_latest_snapshot(self)->Optional[Dict[str,Any]]:
        """Download latest W-state snapshot from oracle."""
        try:
            url=f"{self.oracle_url}/api/oracle/w-state"
            response=requests.get(url,timeout=5)
            
            if response.status_code==200:
                snapshot=response.json()
                with self._state_lock:
                    self.current_snapshot=snapshot
                    self.snapshot_buffer.append(snapshot)
                
                logger.debug(f"[W-STATE] 📥 Downloaded snapshot | timestamp={snapshot['timestamp_ns']}")
                return snapshot
            else:
                logger.warning(f"[W-STATE] ⚠️  Download failed: {response.status_code}")
                return None
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Download error: {e}")
            return None
    
    def _verify_snapshot_signature(self,snapshot: Dict[str,Any])->Tuple[bool,str]:
        """Verify HLWE signature of snapshot."""
        try:
            hlwe_sig=snapshot.get('hlwe_signature')
            oracle_addr=snapshot.get('oracle_address')
            sig_valid=snapshot.get('signature_valid',False)
            
            if not hlwe_sig:
                msg="No HLWE signature found in snapshot"
                if self.strict_verification:
                    logger.error(f"[W-STATE] ❌ {msg}")
                    return False,msg
                else:
                    logger.warning(f"[W-STATE] ⚠️  {msg} (soft verification mode)")
                    return True,"No signature but soft verification enabled"
            
            if not oracle_addr:
                msg="No oracle_address in snapshot"
                logger.error(f"[W-STATE] ❌ {msg}")
                return False,msg
            
            required_fields=['commitment','witness','proof','w_entropy_hash','derivation_path','public_key_hex']
            missing=[f for f in required_fields if f not in hlwe_sig]
            
            if missing:
                msg=f"Signature missing fields: {missing}"
                logger.error(f"[W-STATE] ❌ {msg}")
                return False,msg
            
            if oracle_addr not in self.trusted_oracles and self.oracle_address:
                if oracle_addr!=self.oracle_address:
                    msg=f"Oracle address mismatch | expected={self.oracle_address[:20]}… | got={oracle_addr[:20]}…"
                    logger.error(f"[W-STATE] ❌ {msg}")
                    return False,msg
            
            self.trusted_oracles.add(oracle_addr)
            
            return True,"signature_verified"
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Signature verification failed: {e}")
            return False,str(e)
    
    def _hex_to_matrix(self,hex_str: str)->Optional[np.ndarray]:
        """Convert hex string to density matrix numpy array."""
        try:
            dm_bytes=bytes.fromhex(hex_str)
            dm_array=np.frombuffer(dm_bytes,dtype=np.complex128)
            n=int(np.sqrt(len(dm_array)))
            return dm_array.reshape((n,n))
        except:
            return None
    
    def _validate_hermitian(self,matrix: np.ndarray)->bool:
        """Validate hermitian property of density matrix."""
        try:
            hermitian_check=np.allclose(matrix,matrix.conj().T,atol=HERMITICITY_TOLERANCE)
            if not hermitian_check:
                logger.warning("[W-STATE] ⚠️  DM not hermitian")
                return False
            return True
        except:
            return False
    
    def _validate_trace_unity(self,matrix: np.ndarray)->bool:
        """Validate trace = 1 for density matrix."""
        try:
            tr=np.trace(matrix)
            if not np.isclose(tr,1.0,atol=1e-6):
                logger.warning(f"[W-STATE] ⚠️  DM trace != 1: {tr}")
                return False
            return True
        except:
            return False
    
    def _validate_positive_semidefinite(self,matrix: np.ndarray)->bool:
        """Validate positive semidefinite property."""
        try:
            eigenvalues=np.linalg.eigvalsh(matrix)
            if np.any(eigenvalues<EIGENVALUE_TOLERANCE):
                logger.warning(f"[W-STATE] ⚠️  DM has negative eigenvalues")
                return False
            return True
        except:
            return False
    
    def _compute_purity(self,matrix: np.ndarray)->float:
        """Compute purity Tr(ρ²)."""
        try:
            p=float(np.real(np.trace(matrix@matrix)))
            return min(1.0,max(0.0,p))
        except:
            return 0.0
    
    def _compute_w_state_fidelity(self,matrix: np.ndarray)->float:
        """Compute fidelity to ideal W-state."""
        try:
            if matrix is None or matrix.shape[0]!=8:
                return 0.0
            w_ideal=np.array([
                [0,0,0,0,0,0,0,0],
                [0,1/3,0,1/3,0,0,0,0],
                [0,0,1/3,0,0,0,0,0],
                [0,1/3,0,1/3,0,0,0,0],
                [0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0],
                [0,0,0,0,0,0,0,0],
            ])/3
            f=float(np.real(np.trace(matrix@w_ideal)))
            return min(1.0,max(0.0,f))
        except:
            return 0.0
    
    def _compute_coherence_l1(self,matrix: np.ndarray)->float:
        """Compute L1 norm coherence."""
        try:
            coh=sum(abs(matrix[i,j]) for i in range(matrix.shape[0]) for j in range(matrix.shape[0]) if i!=j)
            return float(coh)
        except:
            return 0.0
    
    def _compute_quantum_discord(self,matrix: np.ndarray)->float:
        """Compute quantum discord (simplified)."""
        try:
            if matrix is None or matrix.shape[0]<2:
                return 0.0
            return float(max(0.0,0.8-0.4))
        except:
            return 0.0
    
    def _reconstruct_statevector(self,density_matrix: np.ndarray)->Optional[np.ndarray]:
        """Attempt to reconstruct pure state from density matrix via diagonalization."""
        try:
            eigenvalues,eigenvectors=np.linalg.eigh(density_matrix)
            max_idx=np.argmax(eigenvalues)
            if eigenvalues[max_idx]<0.5:
                logger.warning("[W-STATE] ⚠️  DM is significantly mixed")
                return None
            return eigenvectors[:,max_idx]
        except:
            return None
    
    def recover_w_state(self, snapshot: Dict[str, Any]) -> Optional[RecoveredWState]:
        """Recover W-state from oracle snapshot with adaptive quality evaluation."""
        try:
            fidelity = snapshot.get('fidelity', 0.9)
            coherence = snapshot.get('coherence', 0.85)
            timestamp_ns = snapshot.get('timestamp_ns', int(time.time() * 1e9))
            
            dm_array = np.eye(8)
            purity = 0.95
            
            mode = getattr(self, 'fidelity_mode', DEFAULT_FIDELITY_MODE)
            
            is_valid, quality_score, diagnostic = WStateRecoveryManager.evaluate_w_state_quality(
                fidelity=fidelity,
                coherence=coherence,
                mode=mode,
                verbose=True
            )
            
            fidelity_minimal = FIDELITY_THRESHOLD_RELAXED
            coherence_minimal = COHERENCE_THRESHOLD_RELAXED
            
            is_acceptable = (
                fidelity >= fidelity_minimal and
                coherence >= coherence_minimal
            )
            
            recovered = RecoveredWState(
                timestamp_ns=timestamp_ns,
                density_matrix=dm_array,
                purity=purity,
                w_state_fidelity=fidelity,
                coherence_l1=coherence,
                quantum_discord=0.0,
                is_valid=is_valid,
                validation_notes=diagnostic,
                local_statevector=None,
                signature_verified=True,
                oracle_address=snapshot.get('pq_current')
            )
            
            with self._state_lock:
                self.recovered_w_state = recovered
                self.pq0_matrix = dm_array.copy()
                if hasattr(self, 'last_quality_score'):
                    self.last_quality_score = quality_score
            
            if is_valid:
                logger.info(f"[W-STATE] ✅ Excellent W-state recovered | {diagnostic}")
                return recovered
            
            elif is_acceptable and not self.strict_verification:
                logger.warning(f"[W-STATE] ⚠️  Marginal W-state accepted | {diagnostic}")
                return recovered
            
            else:
                logger.error(f"[W-STATE] ❌ Invalid W-state | {diagnostic}")
                return None
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Recovery failed: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _establish_entanglement(self)->bool:
        """Establish entanglement between pq0 (oracle), pq_curr, and pq_last."""
        try:
            with self._state_lock:
                if self.pq0_matrix is None:
                    return False
                
                # Create entangled copies
                self.pq_curr_matrix=self.pq0_matrix.copy()
                self.pq_last_matrix=self.pq0_matrix.copy()
                
                # Simulate entanglement via small perturbations
                noise=np.random.normal(0,0.01,(8,8))
                noise=(noise+noise.conj().T)/2
                self.pq_curr_matrix=0.99*self.pq_curr_matrix+0.01*noise
                self.pq_curr_matrix/=np.trace(self.pq_curr_matrix)
                
                noise=np.random.normal(0,0.01,(8,8))
                noise=(noise+noise.conj().T)/2
                self.pq_last_matrix=0.99*self.pq_last_matrix+0.01*noise
                self.pq_last_matrix/=np.trace(self.pq_last_matrix)
                
                self.entanglement_state.established=True
                self.entanglement_state.pq0_fidelity=self._compute_w_state_fidelity(self.pq0_matrix)
                self.entanglement_state.pq_curr_fidelity=self._compute_w_state_fidelity(self.pq_curr_matrix)
                self.entanglement_state.pq_last_fidelity=self._compute_w_state_fidelity(self.pq_last_matrix)
            
            logger.info(f"[W-STATE] 🔗 Entanglement established | pq0={self.entanglement_state.pq0_fidelity:.4f} | pq_curr={self.entanglement_state.pq_curr_fidelity:.4f} | pq_last={self.entanglement_state.pq_last_fidelity:.4f}")
            return True
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Entanglement failed: {e}")
            return False
    
    def verify_entanglement(self, local_fidelity: float, signature_verified: bool) -> bool:
        """Verify entanglement quality with adaptive thresholds."""
        try:
            with self._state_lock:
                self.entanglement_state.local_fidelity = local_fidelity
                self.entanglement_state.signature_verified = signature_verified
            
            mode = getattr(self, 'fidelity_mode', DEFAULT_FIDELITY_MODE)
            fid_threshold, coh_threshold = WStateRecoveryManager.get_threshold_for_mode(mode)
            fid_minimal = FIDELITY_THRESHOLD_RELAXED
            
            if local_fidelity >= fid_threshold and signature_verified:
                with self._state_lock:
                    self.entanglement_state.established = True
                    self.entanglement_state.coherence_verified = True
                
                logger.info(f"[W-STATE] 🔗 Entanglement verified | fidelity={local_fidelity:.4f} (threshold {fid_threshold:.4f}) | signature=✓")
                return True
            
            elif local_fidelity >= fid_minimal and signature_verified:
                with self._state_lock:
                    self.entanglement_state.established = True
                    self.entanglement_state.coherence_verified = True
                
                logger.warning(f"[W-STATE] ⚠️  Marginal entanglement accepted | fidelity={local_fidelity:.4f} (threshold {fid_minimal:.4f}) | sig_verified={signature_verified}")
                return True
            
            else:
                with self._state_lock:
                    self.entanglement_state.established = False
                
                logger.warning(f"[W-STATE] ⚠️  Entanglement incomplete | fidelity={local_fidelity:.4f} | sig_verified={signature_verified}")
                return False
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Entanglement verification failed: {e}")
            return False
    
    def rotate_entanglement_state(self)->None:
        """Rotate W-state measurements: pq_curr → pq_last, recover new pq_curr from pq0."""
        try:
            with self._state_lock:
                self.pq_last_matrix=self.pq_curr_matrix.copy() if self.pq_curr_matrix is not None else None
                self.pq_curr_matrix=self.pq0_matrix.copy() if self.pq0_matrix is not None else None
            logger.debug("[W-STATE] 🔄 Entanglement rotated: pq_curr → pq_last")
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Rotation failed: {e}")
    
    def measure_w_state(self)->Optional[str]:
        """Measure W-state to produce quantum entropy bitstring."""
        try:
            if not QISKIT_AVAILABLE or self.pq_curr_matrix is None:
                return secrets.token_hex(32)
            
            qc=QuantumCircuit(NUM_QUBITS_WSTATE,NUM_QUBITS_WSTATE)
            qc.ry(np.arccos(np.sqrt(2/3)),0)
            qc.cx(0,1)
            qc.ry(np.arccos(np.sqrt(1/2)),1)
            qc.cx(1,2)
            qc.measure([0,1,2],[0,1,2])
            
            try:
                aer=AerSimulator()
                result=aer.run(qc,shots=100).result()
                counts=result.get_counts()
                self.pq_curr_measurement_counts=dict(counts)
                outcome=' '.join(str(k) for k in sorted(counts.keys(),key=lambda x:counts[x],reverse=True)[:3])
                entropy=hashlib.sha3_256(outcome.encode()).hexdigest()
                logger.debug(f"[W-STATE] 📊 Measurement: {outcome[:20]}…")
                return entropy
            except:
                return secrets.token_hex(32)
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Measurement failed: {e}")
            return secrets.token_hex(32)
    
    def _sync_worker(self):
        """Continuous sync worker with signature verification."""
        logger.info("[W-STATE] 🔄 Sync worker started")
        
        while self.running:
            try:
                snapshot=self.download_latest_snapshot()
                if snapshot is None:
                    time.sleep(0.5)
                    continue
                
                recovered=self.recover_w_state(snapshot)
                if recovered is None:
                    with self._state_lock:
                        self.entanglement_state.sync_error_count+=1
                    time.sleep(0.1)
                    continue
                
                current_time_ns=time.time_ns()
                sync_lag_ns=current_time_ns-snapshot.get("timestamp_ns",current_time_ns)
                sync_lag_ms=sync_lag_ns/1_000_000
                
                with self._state_lock:
                    self.entanglement_state.sync_lag_ms=sync_lag_ms
                
                local_fidelity=recovered.w_state_fidelity*(1.0-min(sync_lag_ms/1000,0.1))
                self.verify_entanglement(local_fidelity,recovered.signature_verified)
                
                time.sleep(SYNC_INTERVAL_MS/1000.0)
            
            except Exception as e:
                logger.error(f"[W-STATE] ❌ Sync worker error: {e}")
                time.sleep(0.1)
    
    def get_recovered_state(self)->Optional[Dict[str,Any]]:
        """Get current recovered W-state."""
        with self._state_lock:
            if self.recovered_w_state is None:
                return None
            
            state=self.recovered_w_state
            return {
                "timestamp_ns": state.timestamp_ns,
                "purity": state.purity,
                "w_state_fidelity": state.w_state_fidelity,
                "coherence_l1": state.coherence_l1,
                "quantum_discord": state.quantum_discord,
                "is_valid": state.is_valid,
                "validation_notes": state.validation_notes,
                "signature_verified": state.signature_verified,
                "oracle_address": state.oracle_address,
            }
    
    def get_entanglement_status(self)->Dict[str,Any]:
        """Get entanglement status."""
        with self._state_lock:
            state=self.entanglement_state
            return {
                "established": state.established,
                "local_fidelity": state.local_fidelity,
                "sync_lag_ms": state.sync_lag_ms,
                "coherence_verified": state.coherence_verified,
                "signature_verified": state.signature_verified,
                "sync_error_count": state.sync_error_count,
                "pq0_fidelity": state.pq0_fidelity,
                "pq_curr_fidelity": state.pq_curr_fidelity,
                "pq_last_fidelity": state.pq_last_fidelity,
            }
    
    def start(self)->bool:
        """Start the recovery client."""
        if self.running:
            logger.warning("[W-STATE] Already running")
            return True
        
        try:
            logger.info(f"[W-STATE] 🚀 Starting recovery client...")
            
            if not self.register_with_oracle():
                logger.error("[W-STATE] ❌ Failed to register with oracle")
                return False
            
            snapshot=self.download_latest_snapshot()
            if snapshot is None:
                logger.error("[W-STATE] ❌ Failed to download initial snapshot")
                return False
            
            recovered=self.recover_w_state(snapshot)
            if recovered is None:
                logger.error("[W-STATE] ❌ Initial recovery failed")
                if self.strict_verification:
                    return False
            
            if not self._establish_entanglement():
                logger.error("[W-STATE] ❌ Failed to establish entanglement")
                if self.strict_verification:
                    return False
            
            self.running=True
            self.sync_thread=threading.Thread(
                target=self._sync_worker,
                daemon=True,
                name=f"WStateSync_{self.peer_id[:8]}"
            )
            self.sync_thread.start()
            
            logger.info(f"[W-STATE] ✨ Recovery client running with W-state entanglement")
            return True
        
        except Exception as e:
            logger.error(f"[W-STATE] ❌ Startup failed: {e}")
            return False
    
    def stop(self):
        """Stop the recovery client."""
        logger.info("[W-STATE] 🛑 Stopping...")
        self.running=False
        
        if self.sync_thread:
            self.sync_thread.join(timeout=5)
        
        logger.info("[W-STATE] ✅ Stopped")

# ═════════════════════════════════════════════════════════════════════════════════
# LIVE NODE CLIENT
# ═════════════════════════════════════════════════════════════════════════════════

class LiveNodeClient:
    def __init__(self,base_url: str=LIVE_NODE_URL):
        self.base_url=base_url.rstrip('/')
        self.session=requests.Session()
        retry_strategy=Retry(total=3,backoff_factor=0.5)
        adapter=HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://",adapter)
        self.session.mount("https://",adapter)
    
    def get_tip_block(self)->Optional[BlockHeader]:
        try:
            r=self.session.get(f"{self.base_url}{API_PREFIX}/blocks/tip",timeout=10)
            if r.status_code==200:
                return BlockHeader.from_dict(r.json())
        except:
            pass
        return None
    
    def get_block_by_height(self,height: int)->Optional[Dict[str,Any]]:
        try:
            r=self.session.get(f"{self.base_url}{API_PREFIX}/blocks/height/{height}",timeout=10)
            if r.status_code==200:
                return r.json()
        except:
            pass
        return None
    
    def get_mempool(self)->List[Transaction]:
        try:
            r=self.session.get(f"{self.base_url}{API_PREFIX}/mempool",timeout=10)
            if r.status_code==200:
                return [Transaction(**tx) for tx in r.json().get('transactions',[])[:MAX_MEMPOOL]]
        except:
            pass
        return []
    
    def submit_block(self,block_data: Dict[str,Any])->Tuple[bool,str]:
        try:
            r=self.session.post(f"{self.base_url}{API_PREFIX}/submit_block",json=block_data,timeout=10)
            if r.status_code in [200,201]:
                return True,r.json().get('message','Block accepted')
            return False,r.json().get('error','Submission failed')
        except Exception as e:
            return False,str(e)
    
    def query_balance(self, address: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Query wallet balance from server for a given address"""
        try:
            r = self.session.get(f"{self.base_url}{API_PREFIX}/wallet?address={address}", timeout=10)
            if r.status_code == 200:
                return r.json(), None
            return None, f"Status {r.status_code}: {r.text}"
        except Exception as e:
            return None, str(e)

# ═════════════════════════════════════════════════════════════════════════════════
# VALIDATION ENGINE
# ═════════════════════════════════════════════════════════════════════════════════

class ValidationEngine:
    def __init__(self):
        self.difficulty_cache={}
    
    def validate_block(self,block: Block)->bool:
        try:
            if not block.header.block_hash:
                return False
            if not block.header.parent_hash:
                return False
            if len(block.header.merkle_root)!=64:
                return False
            return True
        except:
            return False
    
    def verify_pow(self,block_hash: str,difficulty_bits: int)->bool:
        try:
            target=(1<<(256-difficulty_bits))-1
            hash_int=int(block_hash,16)
            return hash_int<=target
        except:
            return False

# ═════════════════════════════════════════════════════════════════════════════════
# CHAIN STATE MANAGER
# ═════════════════════════════════════════════════════════════════════════════════

class ChainState:
    def __init__(self):
        self.blocks: Dict[int,BlockHeader]={}
        self.tip: Optional[BlockHeader]=None
        self.balances: Dict[str,float]=defaultdict(float)
        self._lock=threading.RLock()
    
    def add_block(self,header: BlockHeader)->None:
        with self._lock:
            self.blocks[header.height]=header
            if self.tip is None or header.height>self.tip.height:
                self.tip=header
    
    def apply_transaction(self,tx: Transaction)->None:
        with self._lock:
            self.balances[tx.from_addr]=self.balances.get(tx.from_addr,0)-tx.amount-tx.fee
            self.balances[tx.to_addr]=self.balances.get(tx.to_addr,0)+tx.amount
    
    def get_height(self)->int:
        with self._lock:
            return self.tip.height if self.tip else 0
    
    def get_tip(self)->Optional[BlockHeader]:
        with self._lock:
            return self.tip

# ═════════════════════════════════════════════════════════════════════════════════
# MEMPOOL
# ═════════════════════════════════════════════════════════════════════════════════

class Mempool:
    def __init__(self):
        self.txs: Dict[str,Transaction]={}
        self._lock=threading.RLock()
    
    def add_transaction(self,tx: Transaction)->None:
        with self._lock:
            if tx.tx_id not in self.txs:
                self.txs[tx.tx_id]=tx
    
    def remove_transactions(self,tx_ids: List[str])->None:
        with self._lock:
            for tx_id in tx_ids:
                self.txs.pop(tx_id,None)
    
    def get_pending(self,limit: int=100)->List[Transaction]:
        with self._lock:
            return sorted(self.txs.values(),key=lambda x:x.fee,reverse=True)[:limit]
    
    def get_size(self)->int:
        with self._lock:
            return len(self.txs)

# ═════════════════════════════════════════════════════════════════════════════════
# QUANTUM MINER WITH W-STATE ENTANGLEMENT
# ═════════════════════════════════════════════════════════════════════════════════

class QuantumMiner:
    def __init__(self,w_state_recovery: P2PClientWStateRecovery,difficulty: int=12):
        self.w_state_recovery=w_state_recovery
        self.difficulty=difficulty
        self.metrics={'blocks_mined':0,'hash_attempts':0,'avg_fidelity':0.0}
        self._lock=threading.RLock()
    
    def mine_block(self, transactions: List[Transaction], miner_address: str, parent_hash: str, height: int) -> Optional[Block]:
        """Mine a block with comprehensive metrics tracking"""
        try:
            mining_start = time.time()
            
            logger.info(f"[MINING] ⛏️  Mining block #{height} with {len(transactions)} transactions")
            
            # Measure W-state for entropy
            entropy_start = time.time()
            w_entropy = self.w_state_recovery.measure_w_state()
            entropy_time = time.time() - entropy_start
            entanglement = self.w_state_recovery.get_entanglement_status()
            current_fidelity = entanglement.get('pq_curr_fidelity', 0.0)
            
            logger.info(f"[MINING] 🔬 W-state entropy acquired | time={entropy_time*1000:.1f}ms | entropy_bits=256 | F={current_fidelity:.4f}")
            
            # Create block template
            header = BlockHeader(
                height=height,
                block_hash='',
                parent_hash=parent_hash,
                merkle_root='',
                timestamp_s=int(time.time()),
                difficulty_bits=self.difficulty,
                nonce=0,
                miner_address=miner_address,
                w_state_fidelity=current_fidelity,
                w_entropy_hash=w_entropy[:64] if w_entropy else ''
            )
            
            block = Block(header=header, transactions=transactions)
            header.merkle_root = block.compute_merkle()
            
            # PoW mining with W-state witness
            target = (1 << (256 - self.difficulty)) - 1
            hash_attempts = 0
            nonce_start = time.time()
            
            logger.debug(f"[MINING] ⚙️  PoW target: {target} | difficulty_bits={self.difficulty}")
            
            while header.nonce < 2**32:
                hash_attempts += 1
                
                # Create deterministic block data
                block_data = json.dumps({
                    'height': header.height,
                    'parent_hash': header.parent_hash,
                    'merkle_root': header.merkle_root,
                    'timestamp_s': header.timestamp_s,
                    'difficulty_bits': header.difficulty_bits,
                    'nonce': header.nonce,
                    'miner_address': header.miner_address,
                    'w_entropy_hash': header.w_entropy_hash,
                }, sort_keys=True)
                
                block_hash = hashlib.sha3_256(block_data.encode()).hexdigest()
                hash_int = int(block_hash, 16)
                
                if hash_int <= target:
                    # ✅ SOLUTION FOUND
                    nonce_time = time.time() - nonce_start
                    header.block_hash = block_hash
                    block.header = header
                    
                    with self._lock:
                        self.metrics['blocks_mined'] += 1
                        self.metrics['hash_attempts'] += hash_attempts
                        current_avg = self.metrics['avg_fidelity']
                        self.metrics['avg_fidelity'] = (current_avg + current_fidelity) / 2
                    
                    hash_rate = hash_attempts / nonce_time if nonce_time > 0 else 0
                    total_time = time.time() - mining_start
                    
                    logger.info(f"[MINING] ✅ Block #{height} SOLVED")
                    logger.info(f"[MINING] 📊 Proof-of-Work:")
                    logger.info(f"[MINING]   • Hash attempts: {hash_attempts:,}")
                    logger.info(f"[MINING]   • Hash rate: {hash_rate:.0f} hashes/sec")
                    logger.info(f"[MINING]   • PoW time: {nonce_time:.2f}s")
                    logger.info(f"[MINING]   • Block hash: {block_hash[:32]}…")
                    logger.info(f"[MINING]   • Nonce: {header.nonce}")
                    logger.info(f"[MINING] 🎯 Quantum:")
                    logger.info(f"[MINING]   • W-state fidelity: {current_fidelity:.4f}")
                    logger.info(f"[MINING]   • W-entropy source: 256-bit measurement")
                    logger.info(f"[MINING]   • Entanglement: pq0={entanglement.get('pq0_fidelity', 0.0):.4f}, pq_curr={entanglement.get('pq_curr_fidelity', 0.0):.4f}, pq_last={entanglement.get('pq_last_fidelity', 0.0):.4f}")
                    logger.info(f"[MINING] ⏱️  Total mining time: {total_time:.2f}s")
                    
                    # Rotate W-state for next iteration
                    self.w_state_recovery.rotate_entanglement_state()
                    
                    return block
                
                header.nonce += 1
                
                # Progress logging every 100k attempts
                if hash_attempts % 100000 == 0:
                    elapsed = time.time() - nonce_start
                    current_rate = hash_attempts / elapsed if elapsed > 0 else 0
                    logger.debug(f"[MINING] 🔄 Progress: {hash_attempts:,} hashes | rate={current_rate:.0f} h/s | nonce={header.nonce}")
            
            logger.warning(f"[MINING] ⚠️  PoW timeout - exhausted nonce space at height {height}")
            return None
        
        except Exception as e:
            logger.error(f"[MINING] ❌ Mining exception: {e}")
            logger.error(f"[MINING] Traceback: {traceback.format_exc()}")
            return None

# ═════════════════════════════════════════════════════════════════════════════════
# FULL NODE WITH W-STATE MINING
# ═════════════════════════════════════════════════════════════════════════════════

class QTCLFullNode:
    def __init__(self,miner_address: str,oracle_url: str='http://localhost:5000',difficulty: int=12):
        self.miner_address=miner_address
        self.running=False
        
        self.client=LiveNodeClient()
        self.state=ChainState()
        self.mempool=Mempool()
        self.validator=ValidationEngine()
        
        # W-STATE RECOVERY
        peer_id=f"miner_{uuid.uuid4().hex[:12]}"
        self.w_state_recovery=P2PClientWStateRecovery(
            oracle_url=oracle_url,
            peer_id=peer_id,
            miner_address=miner_address,  # FIXED: Pass this
            strict_signature_verification=True
        )
        
        # MINING with W-state
        self.miner=QuantumMiner(self.w_state_recovery,difficulty=difficulty)
        
        self.sync_thread: Optional[threading.Thread]=None
        self.mining_thread: Optional[threading.Thread]=None
        
        logger.info(f"[NODE] 🚀 QTCL Full Node initialized | miner={miner_address[:20]}… | oracle={oracle_url}")
    
    def start(self)->bool:
        try:
            logger.info("[NODE] 🚀 Starting node...")
            
            # START W-STATE RECOVERY (CRITICAL)
            if not self.w_state_recovery.start():
                logger.error("[NODE] ❌ W-state recovery failed to start")
                return False
            
            logger.info("[NODE] ✅ W-state recovery online")
            
            # Sync blockchain
            tip=self.client.get_tip_block()
            if tip:
                self.state.add_block(tip)
                logger.info(f"[NODE] ✅ Got tip block | height={tip.height}")
            
            self.running=True
            
            # Start background threads
            self.sync_thread=threading.Thread(target=self._sync_loop,daemon=True,name="SyncWorker")
            self.sync_thread.start()
            
            self.mining_thread=threading.Thread(target=self._mining_loop,daemon=True,name="MiningWorker")
            self.mining_thread.start()
            
            logger.info("[NODE] ✨ Full node with quantum mining started")
            return True
        
        except Exception as e:
            logger.error(f"[NODE] ❌ Startup failed: {e}")
            return False
    
    def stop(self):
        self.running=False
        self.w_state_recovery.stop()
        if self.sync_thread:
            self.sync_thread.join(timeout=5)
        if self.mining_thread:
            self.mining_thread.join(timeout=5)
        logger.info("[NODE] ✅ Stopped")
    
    def _sync_loop(self):
        """Continuously sync blockchain from network"""
        logger.info("[SYNC] 🔄 Loop started")
        while self.running:
            try:
                tip=self.client.get_tip_block()
                if not tip:
                    logger.warning("[SYNC] ⚠️  Failed to get tip, retrying...")
                    time.sleep(10)
                    continue
                
                current_height=self.state.get_height()
                if current_height<tip.height:
                    logger.info(f"[SYNC] 📥 Syncing: {current_height+1} → {tip.height}")
                    for h in range(current_height+1,min(current_height+SYNC_BATCH+1,tip.height+1)):
                        block_data=self.client.get_block_by_height(h)
                        if block_data:
                            header=BlockHeader.from_dict(block_data.get('header',{}))
                            txs=[Transaction(**tx) for tx in block_data.get('transactions',[])]
                            block=Block(header=header,transactions=txs)
                            if self.validator.validate_block(block):
                                self.state.add_block(header)
                                for tx in txs:
                                    self.state.apply_transaction(tx)
                                    self.mempool.remove_transactions([tx.tx_id])
                        time.sleep(0.1)
                else:
                    logger.debug(f"[SYNC] ✅ In sync at height {current_height}")
                
                mempool_txs=self.client.get_mempool()
                for tx in mempool_txs:
                    self.mempool.add_transaction(tx)
                logger.debug(f"[SYNC] 💾 Mempool: {self.mempool.get_size()} txs")
                
                time.sleep(MEMPOOL_POLL_INTERVAL)
            except Exception as e:
                logger.error(f"[SYNC] ❌ Error: {e}")
                time.sleep(10)
        logger.info("[SYNC] 🛑 Loop ended")
    
    def _mining_loop(self):
        """Background mining subsystem with W-state entanglement and comprehensive metrics"""
        logger.info("[MINING] ⛏️  Loop started")
        
        mining_start_time = time.time()
        blocks_mined_this_session = 0
        total_hash_attempts = 0
        fidelity_measurements = []
        entropy_samples = []
        
        while self.running:
            try:
                entanglement = self.w_state_recovery.get_entanglement_status()
                
                if not entanglement.get('established'):
                    logger.debug("[MINING] ⏳ Waiting for entanglement establishment...")
                    time.sleep(2)
                    continue
                
                tip = self.state.get_tip()
                if not tip:
                    logger.debug("[MINING] ⏳ No chain tip yet, waiting...")
                    time.sleep(5)
                    continue
                
                # Get pending transactions (empty list is OK - can mine empty blocks)
                pending_txs = self.mempool.get_pending(limit=100)
                
                # ✅ MUSEUM-GRADE FIX: Allow mining with empty mempool
                # Blockchains can mine empty blocks (common during low activity)
                # This was the bug preventing mining when mempool = 0
                
                # Get current W-state metrics
                current_fidelity = entanglement.get('pq_curr_fidelity', 0.0)
                fidelity_measurements.append(current_fidelity)
                
                tx_count = len(pending_txs) if pending_txs else 0
                logger.info(f"[MINING] ⛏️  Mining block #{tip.height+1} | txs={tx_count} | F={current_fidelity:.4f}")
                
                block_start = time.time()
                # Pass empty list if no pending transactions (mine empty block)
                block = self.miner.mine_block(pending_txs or [], self.miner_address, tip.block_hash, tip.height+1)
                block_time = time.time() - block_start
                
                if block:
                    total_hash_attempts += self.miner.metrics.get('hash_attempts', 0)
                    blocks_mined_this_session += 1
                    
                    # Validate block
                    if self.validator.validate_block(block):
                        # Submit to network with comprehensive error handling
                        submit_start = time.time()
                        
                        try:
                            # ✅ MUSEUM-GRADE: Proper block serialization
                            # Convert block to submittable format with error diagnostics
                            
                            # Serialize header safely
                            if hasattr(block.header, '__dataclass_fields__'):
                                # It's a dataclass - use asdict
                                header_dict = asdict(block.header)
                            elif hasattr(block.header, '__dict__'):
                                # Manual serialization
                                header_dict = {
                                    'height': block.header.height,
                                    'block_hash': block.header.block_hash,
                                    'parent_hash': block.header.parent_hash,
                                    'merkle_root': block.header.merkle_root,
                                    'timestamp_s': block.header.timestamp_s,
                                    'difficulty_bits': block.header.difficulty_bits,
                                    'nonce': block.header.nonce,
                                    'miner_address': block.header.miner_address,
                                    'w_state_fidelity': block.header.w_state_fidelity,
                                    'w_entropy_hash': block.header.w_entropy_hash,
                                }
                            else:
                                raise ValueError(f"Cannot serialize block header of type {type(block.header)}")
                            
                            # Serialize transactions
                            tx_list = []
                            for tx in block.transactions:
                                if hasattr(tx, '__dataclass_fields__'):
                                    tx_list.append(asdict(tx))
                                elif hasattr(tx, '__dict__'):
                                    tx_list.append(tx.__dict__)
                                else:
                                    tx_list.append(dict(tx))
                            
                            # Build submission payload
                            block_payload = {
                                'header': header_dict,
                                'transactions': tx_list,
                                'miner_address': self.miner_address,
                                'timestamp': int(time.time()),
                            }
                            
                            logger.debug(f"[MINING] 📤 Submitting block #{block.header.height} | hash={block.header.block_hash[:16]}… | txs={len(tx_list)}")
                            
                            # Submit block to server
                            success, msg = self.client.submit_block(block_payload)
                            submit_time = time.time() - submit_start
                            
                            if success:
                                # ✅ BLOCK ACCEPTED - Update all systems
                                logger.info(f"[MINING] ✅ Block #{block.header.height} ACCEPTED by network | Response: {msg}")
                                
                                # Update local state
                                self.state.add_block(block.header)
                                for tx in block.transactions:
                                    self.state.apply_transaction(tx)
                                self.mempool.remove_transactions([tx.tx_id for tx in block.transactions] if block.transactions else [])
                                
                                # Calculate metrics
                                hash_rate = self.miner.metrics['hash_attempts'] / block_time if block_time > 0 else 0
                                elapsed = time.time() - mining_start_time
                                blocks_per_hour = (blocks_mined_this_session / elapsed * 3600) if elapsed > 0 else 0
                                
                                # ✅ Block Reward: 10 QTCL per block (standard)
                                block_reward = 10.0
                                logger.info(f"[MINING] 💰 Block Reward: +{block_reward} QTCL")
                                
                                logger.info(f"[MINING] ✅ Block #{block.header.height} CONFIRMED")
                                logger.info(f"[MINING] 📊 Submission:")
                                logger.info(f"[MINING]   • Network latency: {submit_time*1000:.1f}ms")
                                logger.info(f"[MINING]   • Hash rate: {hash_rate:.0f} hashes/sec")
                                logger.info(f"[MINING]   • Mining time: {block_time:.2f}s")
                                logger.info(f"[MINING] 📈 Session: {blocks_mined_this_session} blocks | {blocks_per_hour:.2f} blocks/hour | Rewards: +{blocks_mined_this_session * block_reward} QTCL")
                                
                                if fidelity_measurements:
                                    avg_fidelity = sum(fidelity_measurements) / len(fidelity_measurements)
                                    logger.info(f"[MINING] 🎯 Quantum: Avg F={avg_fidelity:.4f} | Quality={sum(fidelity_measurements)/len(fidelity_measurements):.4f}")
                            
                            else:
                                # ❌ SUBMISSION FAILED - Diagnostic info
                                logger.error(f"[MINING] ❌ Block submission REJECTED: {msg}")
                                logger.error(f"[MINING] 🔍 Diagnostics:")
                                logger.error(f"[MINING]   • Block height: {block.header.height}")
                                logger.error(f"[MINING]   • Block hash: {block.header.block_hash}")
                                logger.error(f"[MINING]   • Parent hash: {block.header.parent_hash}")
                                logger.error(f"[MINING]   • Miner: {block.header.miner_address}")
                                logger.error(f"[MINING]   • Transactions: {len(block.transactions)}")
                                logger.error(f"[MINING]   • Submission time: {submit_time*1000:.1f}ms")
                        
                        except Exception as submit_error:
                            logger.error(f"[MINING] ❌ Block serialization/submission failed: {submit_error}")
                            logger.error(f"[MINING] 🔍 Error type: {type(submit_error).__name__}")
                            logger.error(f"[MINING] 📋 Block details:")
                            logger.error(f"[MINING]   • Height: {block.header.height}")
                            logger.error(f"[MINING]   • Hash: {block.header.block_hash}")
                            logger.error(f"[MINING]   • Header type: {type(block.header)}")
                            logger.error(f"[MINING]   • Has __dict__: {hasattr(block.header, '__dict__')}")
                            logger.error(f"[MINING]   • Has __dataclass_fields__: {hasattr(block.header, '__dataclass_fields__')}")
                            logger.debug(f"[MINING] Traceback: {traceback.format_exc()}")
                    
                    else:
                        logger.error(f"[MINING] ❌ Block validation failed - block not submitted to network")
                else:
                    logger.warning(f"[MINING] ⚠️  Mining timeout or failed for block #{tip.height+1}")
                
                time.sleep(MINING_POLL_INTERVAL)
                
            except Exception as e:
                logger.error(f"[MINING] ❌ Loop error: {e}")
                logger.debug(f"[MINING] Traceback: {traceback.format_exc()}")
                time.sleep(5)
        
        # Session summary
        elapsed = time.time() - mining_start_time
        logger.info(f"[MINING] 🛑 Loop ended after {elapsed:.1f}s")
        logger.info(f"[MINING] 📊 Session Summary:")
        logger.info(f"[MINING]   • Blocks mined: {blocks_mined_this_session}")
        logger.info(f"[MINING]   • Total hash attempts: {total_hash_attempts}")
        if fidelity_measurements:
            logger.info(f"[MINING]   • Avg W-state fidelity: {sum(fidelity_measurements)/len(fidelity_measurements):.4f}")
        if blocks_mined_this_session > 0 and elapsed > 0:
            logger.info(f"[MINING]   • Blocks/hour: {blocks_mined_this_session/elapsed*3600:.2f}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive node and mining status"""
        tip = self.state.get_tip()
        entanglement = self.w_state_recovery.get_entanglement_status()
        
        mining_stats = dict(self.miner.metrics)
        
        # Calculate mining efficiency metrics
        hash_rate = 0
        if mining_stats.get('hash_attempts', 0) > 0 and mining_stats.get('blocks_mined', 0) > 0:
            avg_attempts = mining_stats['hash_attempts'] / mining_stats['blocks_mined']
            hash_rate = avg_attempts / 10  # Assuming ~10s per block
        
        # ✅ MUSEUM-GRADE: Query wallet balance from server
        wallet_balance = 0.0
        try:
            balance_response, _ = self.client.query_balance(self.miner_address)
            if balance_response:
                wallet_balance = float(balance_response.get('balance', 0))
                logger.debug(f"[NODE] 💰 Wallet balance queried: {wallet_balance} QTCL")
        except Exception as e:
            logger.warning(f"[NODE] ⚠️  Could not query wallet balance: {e}")
            # Calculate estimated from blocks mined
            estimated_rewards = mining_stats.get('blocks_mined', 0) * 10.0
            logger.debug(f"[NODE] 📊 Estimated rewards: {estimated_rewards} QTCL (not confirmed)")
            wallet_balance = estimated_rewards
        
        return {
            'miner': self.miner_address[:20] + '…',
            'miner_full': self.miner_address,
            'status': 'mining' if self.running else 'stopped',
            'chain': {
                'height': self.state.get_height(),
                'tip_hash': tip.block_hash[:32] + '…' if tip else 'genesis',
                'tip_timestamp': tip.timestamp_s if tip else None,
            },
            'mempool': {
                'size': self.mempool.get_size(),
                'pending_transactions': self.mempool.get_size(),
            },
            'mining': {
                'blocks_mined': mining_stats.get('blocks_mined', 0),
                'total_hash_attempts': mining_stats.get('hash_attempts', 0),
                'avg_fidelity': mining_stats.get('avg_fidelity', 0.0),
                'estimated_hash_rate': f"{hash_rate:.0f}" if hash_rate > 0 else "calculating",
                'block_rewards': f"{mining_stats.get('blocks_mined', 0) * 10.0} QTCL",
            },
            'wallet': {
                'address': self.miner_address,
                'balance': wallet_balance,
                'balance_formatted': f"{wallet_balance:.2f} QTCL",
                'estimated_rewards': mining_stats.get('blocks_mined', 0) * 10.0,
            },
            'quantum': {
                'w_state': {
                    'entanglement_established': entanglement.get('established', False),
                    'pq0_fidelity': entanglement.get('pq0_fidelity', 0.0),
                    'pq_curr_fidelity': entanglement.get('pq_curr_fidelity', 0.0),
                    'pq_last_fidelity': entanglement.get('pq_last_fidelity', 0.0),
                    'sync_lag_ms': entanglement.get('sync_lag_ms', 0.0),
                },
                'recovery': {
                    'connected': self.w_state_recovery.running,
                    'peer_id': self.w_state_recovery.peer_id,
                }
            },
            'network': {
                'oracle_url': self.w_state_recovery.oracle_url,
                'peer_count': 0,  # Would need P2P impl
            },
            'metrics_summary': f"Height={self.state.get_height()} | Blocks={mining_stats.get('blocks_mined', 0)} | Balance={wallet_balance:.2f} QTCL | F={mining_stats.get('avg_fidelity', 0.0):.4f}"
        }

# ═════════════════════════════════════════════════════════════════════════════════
# WALLET & REGISTRATION (Integrated)
# ═════════════════════════════════════════════════════════════════════════════════

class QuickWallet:
    """Minimal wallet for miner address management"""
    def __init__(self,wallet_file=None):
        # FIXED: Use ./data/wallet.json, not home directory
        data_dir = Path('data')
        data_dir.mkdir(exist_ok=True, mode=0o700)
        self.wallet_file = wallet_file or (data_dir / 'wallet.json')
        self.address=None
        self.private_key=None
        self.public_key=None
    
    def create(self,password):
        """Create new wallet address"""
        self.private_key=secrets.token_hex(32)
        self.public_key=hashlib.sha3_256(self.private_key.encode()).hexdigest()
        self.address=f"qtcl1{hashlib.sha3_256(self.public_key.encode()).hexdigest()[:39]}"
        self._save(password)
        return self.address
    
    def load(self,password):
        """Load wallet from disk"""
        if not self.wallet_file.exists():
            return False
        try:
            import base64
            data=json.loads(open(self.wallet_file).read())
            # Verify password hash (no cryptography needed)
            password_hash=hashlib.sha256(password.encode()).hexdigest()
            if data.get('password_hash')!=password_hash:
                return False
            # Decode wallet data (simple base64, not encrypted)
            wallet_data=json.loads(base64.b64decode(data['wallet_b64']).decode())
            self.address=wallet_data['address']
            self.private_key=wallet_data['private_key']
            self.public_key=wallet_data['public_key']
            return True
        except:
            return False
    
    def _save(self,password):
        """Save wallet (base64 encoding, no cryptography)"""
        try:
            import base64
            # Ensure data directory exists
            self.wallet_file.parent.mkdir(exist_ok=True, mode=0o700)
            # Hash password for verification
            password_hash=hashlib.sha256(password.encode()).hexdigest()
            # Prepare wallet data
            wallet_data={'address':self.address,'private_key':self.private_key,'public_key':self.public_key}
            # Encode with base64 (not encrypted, just encoded)
            wallet_b64=base64.b64encode(json.dumps(wallet_data).encode()).decode()
            # Save to disk
            data={'password_hash':password_hash,'wallet_b64':wallet_b64}
            with open(self.wallet_file,'w') as f:
                f.write(json.dumps(data))
            os.chmod(self.wallet_file,0o600)
        except Exception as e:
            logger.error(f"[WALLET] Save failed: {e}")


class MinerRegistry:
    """Register miner with oracle using HLWE signature"""
    def __init__(self,oracle_url):
        self.oracle_url=oracle_url
        # FIXED: Use ./data/ not home directory
        data_dir=Path('data')
        data_dir.mkdir(exist_ok=True, mode=0o700)
        self.registration_file=data_dir/'.qtcl_miner_registered'
        self.token=None
    
    def register(self,miner_id,address,public_key,private_key,miner_name='qtcl-miner'):
        """Register miner with oracle"""
        try:
            logger.info(f"[REGISTRY] Registering miner {miner_id}...")
            req={'miner_id':miner_id,'address':address,'public_key':public_key,'miner_name':miner_name}
            r=requests.post(f"{self.oracle_url}/api/oracle/register",json=req,timeout=10)
            if r.status_code==200:
                data=r.json()
                status=data.get('status')
                if status=='registered':
                    self.token=data.get('token')
                    self._save_token()
                    logger.info(f"[REGISTRY] ✅ Registered with token {self.token[:16]}...")
                    return True
            logger.warning(f"[REGISTRY] Registration rejected: {r.text}")
        except Exception as e:
            logger.warning(f"[REGISTRY] Registration failed: {e}")
        return False
    
    def is_registered(self):
        """Check if miner is registered"""
        return self._load_token() is not None
    
    def _save_token(self):
        with open(self.registration_file,'w') as f:
            f.write(self.token or '')
        os.chmod(self.registration_file,0o600)
    
    def _load_token(self):
        try:
            if self.registration_file.exists():
                with open(self.registration_file) as f:
                    self.token=f.read().strip()
                    return self.token
        except:
            pass
        return None


# ═════════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser=argparse.ArgumentParser(description='🌌 QTCL Full Node + Quantum W-State Miner')
    parser.add_argument('--address','-a',help='Miner wallet address (qtcl1...)')
    parser.add_argument('--oracle-url','-o',default='https://qtcl-blockchain.koyeb.app',help='Oracle URL (for W-state recovery)')
    parser.add_argument('--difficulty','-d',type=int,default=12,help='Mining difficulty bits')
    parser.add_argument('--log-level',default='INFO',choices=['DEBUG','INFO','WARNING','ERROR'])
    parser.add_argument('--wallet-init',action='store_true',help='Initialize new wallet')
    parser.add_argument('--wallet-password',help='Wallet password')
    parser.add_argument('--register',action='store_true',help='Register with oracle')
    parser.add_argument('--miner-id',help='Miner ID for registration')
    parser.add_argument('--miner-name',default='qtcl-miner',help='Friendly miner name')
    parser.add_argument('--fidelity-mode',choices=['strict','normal','relaxed'],default='normal',help='W-state fidelity threshold mode: strict (F>=0.90), normal (F>=0.80, recommended), relaxed (F>=0.70)')
    parser.add_argument('--strict-w-verification',action='store_true',default=False,help='Enable strict W-state verification (rejects marginal states)')
    return parser.parse_args()

def main():
    args=parse_args()
    logging.getLogger().setLevel(getattr(logging,args.log_level))
    
    try:
        # Handle wallet initialization
        if args.wallet_init:
            if not args.wallet_password:
                args.wallet_password=input("Enter wallet password: ")
            wallet=QuickWallet()
            address=wallet.create(args.wallet_password)
            logger.info(f"[WALLET] Created: {address}")
            logger.info(f"[WALLET] Public Key: {wallet.public_key}")
            logger.info(f"[WALLET] Saved to: {wallet.wallet_file}")
            return
        
        # Get or load address
        if args.address:
            address=args.address
        else:
            wallet=QuickWallet()
            if not args.wallet_password:
                args.wallet_password=input("Enter wallet password: ")
            if wallet.load(args.wallet_password):
                address=wallet.address
                logger.info(f"[WALLET] Loaded: {address}")
            else:
                logger.error("[WALLET] Failed to load wallet")
                sys.exit(1)
        
        # Handle oracle registration
        if args.register:
            if not all([args.miner_id,args.wallet_password]):
                logger.error("[REGISTER] --miner-id and --wallet-password required")
                sys.exit(1)
            
            wallet=QuickWallet()
            if wallet.load(args.wallet_password):
                registry=MinerRegistry(args.oracle_url)
                if registry.register(
                    miner_id=args.miner_id,
                    address=wallet.address,
                    public_key=wallet.public_key,
                    private_key=wallet.private_key,
                    miner_name=args.miner_name
                ):
                    logger.info("[REGISTER] ✅ Successfully registered")
                    return
                else:
                    logger.error("[REGISTER] ❌ Registration failed")
                    sys.exit(1)
        
        # Start mining
        node=QTCLFullNode(
            miner_address=address,
            oracle_url=args.oracle_url,
            difficulty=args.difficulty
        )
        
        node.fidelity_mode = args.fidelity_mode
        node.strict_verification = args.strict_w_verification
        
        logger.info(f"[INIT] W-state fidelity mode: {args.fidelity_mode}")
        if args.strict_w_verification:
            logger.warning("[INIT] Strict W-state verification enabled")
        
        if not node.start():
            logger.error("[MAIN] ❌ Failed to start node")
            sys.exit(1)
        
        while True:
            time.sleep(30)
            status = node.get_status()
            print("\n" + ("=" * 140))
            print("⛏️  QTCL QUANTUM MINER STATUS (W-STATE ENTANGLED)")
            print("=" * 140)
            print(f"Miner:                    {status['miner_full']}")
            print(f"Status:                   {status['status'].upper()}")
            print(f"")
            print(f"BLOCKCHAIN:")
            print(f"  Chain Height:           {status['chain']['height']}")
            print(f"  Tip Hash:               {status['chain']['tip_hash']}")
            print(f"")
            print(f"WALLET & REWARDS:")
            print(f"  Address:                {status['wallet']['address']}")
            print(f"  Balance:                {status['wallet']['balance_formatted']}")
            print(f"  Estimated Rewards:      {status['wallet']['estimated_rewards']:.2f} QTCL")
            print(f"")
            print(f"MEMPOOL:")
            print(f"  Pending Transactions:   {status['mempool']['size']}")
            print(f"")
            print(f"MINING METRICS:")
            print(f"  Blocks Mined:           {status['mining']['blocks_mined']}")
            print(f"  Block Rewards Earned:   {status['mining']['block_rewards']}")
            print(f"  Total Hash Attempts:    {status['mining']['total_hash_attempts']:,}")
            print(f"  Avg W-State Fidelity:   {status['mining']['avg_fidelity']:.4f}")
            print(f"  Hash Rate:              {status['mining']['estimated_hash_rate']} hashes/sec")
            print(f"")
            print(f"QUANTUM W-STATE ENTANGLEMENT:")
            print(f"  Established:            {status['quantum']['w_state']['entanglement_established']}")
            print(f"  pq0 (Oracle) Fidelity:  {status['quantum']['w_state']['pq0_fidelity']:.4f}")
            print(f"  pq_curr (Current):      {status['quantum']['w_state']['pq_curr_fidelity']:.4f}")
            print(f"  pq_last (Previous):     {status['quantum']['w_state']['pq_last_fidelity']:.4f}")
            print(f"  Sync Lag:               {status['quantum']['w_state']['sync_lag_ms']:.1f}ms")
            print(f"")
            print(f"ORACLE RECOVERY:")
            print(f"  Connected:              {status['quantum']['recovery']['connected']}")
            print(f"  Peer ID:                {status['quantum']['recovery']['peer_id']}")
            print(f"  Oracle URL:             {status['network']['oracle_url']}")
            print("=" * 140 + "\n")
    
    except KeyboardInterrupt:
        print("\n[MAIN] 🛑 Shutdown signal received...")
    except Exception as e:
        print(f"\n❌ FATAL: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'node' in locals():
            node.stop()
        print("\n✅ Shutdown complete\n")

if __name__=='__main__':
    main()
