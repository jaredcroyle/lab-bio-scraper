"""
Layer 3: Enhancement system

This layer adds provenance tracking, quality scoring, versioning,
and ML-ready formatting to normalized metadata.
"""

from .enhancer import MetadataEnhancer
from .quality import QualityScorer
from .provenance import ProvenanceTracker
from .ml_formatter import MLFormatter

__all__ = ["MetadataEnhancer", "QualityScorer", "ProvenanceTracker", "MLFormatter"]
