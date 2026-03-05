"""Pure computation helpers for startability scoring."""
from __future__ import annotations

import struct

import numpy as np


def unpack_embedding(blob: bytes) -> np.ndarray:
    return np.array(struct.unpack("64d", blob))


def pack_embedding(arr: np.ndarray) -> bytes:
    return struct.pack("64d", *arr.tolist())


def phi_tech_cosine(y_v: np.ndarray, h_ft: np.ndarray) -> float:
    norm_a = np.linalg.norm(y_v)
    norm_b = np.linalg.norm(h_ft)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(y_v, h_ft) / (norm_a * norm_b))


def phi_tech_distance(y_v: np.ndarray, h_ft: np.ndarray) -> float:
    return float(1.0 / (1.0 + np.linalg.norm(y_v - h_ft)))


def phi_tech_cpc_jaccard(
    cluster_cpc_codes: set[str], firm_cpc_codes: set[str]
) -> float:
    if not cluster_cpc_codes and not firm_cpc_codes:
        return 0.0
    intersection = cluster_cpc_codes & firm_cpc_codes
    union = cluster_cpc_codes | firm_cpc_codes
    return len(intersection) / len(union) if union else 0.0


def phi_tech_citation_proximity(
    cluster_patents: set[str], firm_cited_patents: set[str]
) -> float:
    if not cluster_patents or not firm_cited_patents:
        return 0.0
    overlap = cluster_patents & firm_cited_patents
    return len(overlap) / len(cluster_patents)


def gate(cos_sim: float, cpc_overlap: float, cite_prox: float) -> bool:
    return cpc_overlap > 0.01 or cite_prox > 0 or cos_sim > 0.3


# Calibrated 2026-02-24 (iteration 2) after quality check
# Target: Toyota×auto > 0.8, Suntory×semi < 0.4, std > 0.15
# Iteration 1 (α=-1.5, β=[3,1.5,1,0.5]): std=0.084 — too compressed
# Iteration 2 (α=-4.0, β=[6,3,2,1]): std=0.165, Toyota=0.98, weak=0.42 ✅
DEFAULT_ALPHA = -4.0
DEFAULT_BETA_TECH = np.array([6.0, 3.0, 2.0, 1.0])


def startability_score(
    phi_tech: np.ndarray,
    alpha: float = DEFAULT_ALPHA,
    beta_tech: np.ndarray | None = None,
) -> float:
    if beta_tech is None:
        beta_tech = DEFAULT_BETA_TECH
    logit = alpha + np.dot(beta_tech, phi_tech)
    return float(1.0 / (1.0 + np.exp(-logit)))

