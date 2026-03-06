"""
Lab Bio Scraper - A comprehensive biological data scraper and normalizer

This package provides connectors and normalizers for major biological data repositories:
- Genomics: NCBI SRA, GEO, ENA
- Proteins: UniProt, PDB, AlphaFold DB
- Single-cell: CELLxGENE, Human Cell Atlas
- Proteomics: PRIDE, MassIVE, MetaboLights
- Biodiversity: GBIF

Architecture:
Layer 1: Official repository ingestion (APIs, FTP, bulk downloads)
Layer 2: Metadata normalization into unified schema
Layer 3: Provenance, versioning, quality scoring, ML formatting
Layer 4: Supplementary scraping for niche sources
"""

__version__ = "0.1.0"
__author__ = "Lab Bio Scraper Team"

from .core.base import BaseConnector, DataRecord
from .core.schema import UnifiedMetadata
from .layer1.connectors import NCBIConnector, GEOConnector, ENAConnector
from .layer1.connectors import UniProtConnector, PDBConnector, AlphaFoldConnector

__all__ = [
    "BaseConnector",
    "DataRecord", 
    "UnifiedMetadata",
    "NCBIConnector",
    "GEOConnector", 
    "ENAConnector",
    "UniProtConnector",
    "PDBConnector",
    "AlphaFoldConnector",
]
