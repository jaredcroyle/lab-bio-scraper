"""
Base classes and interfaces for biological data connectors
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Iterator
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

class DataSource(Enum):
    """Enumeration of supported data sources"""
    NCBI_SRA = "ncbi_sra"
    GEO = "geo"
    ENA = "ena"
    UNIPROT = "uniprot"
    PDB = "pdb"
    ALPHAFOLD = "alphafold"
    PRIDE = "pride"
    MASSIVE = "massive"
    METABOLIGHTS = "metabolights"
    CELLXGENE = "cellxgene"
    HUMAN_CELL_ATLAS = "human_cell_atlas"
    GBIF = "gbif"

class DataType(Enum):
    """Enumeration of biological data types"""
    GENOMIC_SEQUENCE = "genomic_sequence"
    TRANSCRIPTOMIC = "transcriptomic"
    PROTEOMIC = "proteomic"
    METABOLOMIC = "metabolomic"
    STRUCTURAL = "structural"
    SINGLE_CELL = "single_cell"
    BIODIVERSITY = "biodiversity"

@dataclass
class DataRecord:
    """Unified data record structure"""
    id: str
    source: DataSource
    data_type: DataType
    title: str
    description: Optional[str]
    metadata: Dict[str, Any]
    download_urls: List[str]
    file_size: Optional[int]
    checksum: Optional[str]
    accession: Optional[str]
    version: Optional[str]
    date_created: Optional[datetime]
    date_modified: Optional[datetime]
    provenance: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            "id": self.id,
            "source": self.source.value,
            "data_type": self.data_type.value,
            "title": self.title,
            "description": self.description,
            "metadata": self.metadata,
            "download_urls": self.download_urls,
            "file_size": self.file_size,
            "checksum": self.checksum,
            "accession": self.accession,
            "version": self.version,
            "date_created": self.date_created.isoformat() if self.date_created else None,
            "date_modified": self.date_modified.isoformat() if self.date_modified else None,
            "provenance": self.provenance,
        }

class BaseConnector(ABC):
    """Abstract base class for all data connectors"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.base_url = self.config.get("base_url", self.get_default_base_url())
        self.api_key = self.config.get("api_key")
        self.rate_limit = self.config.get("rate_limit", 1.0)  # requests per second
        
    @abstractmethod
    def get_default_base_url(self) -> str:
        """Return the default base URL for this connector"""
        pass
    
    @abstractmethod
    def search(self, query: str, limit: int = 100) -> Iterator[DataRecord]:
        """Search for data records matching the query"""
        pass
    
    @abstractmethod
    def get_record(self, accession: str) -> Optional[DataRecord]:
        """Get a specific data record by accession"""
        pass
    
    @abstractmethod
    def get_metadata(self, accession: str) -> Dict[str, Any]:
        """Get detailed metadata for a record"""
        pass
    
    @abstractmethod
    def download_urls(self, accession: str) -> List[str]:
        """Get download URLs for a record"""
        pass
    
    def validate_accession(self, accession: str) -> bool:
        """Validate if accession format is correct for this source"""
        return True  # Override in subclasses
    
    def get_rate_limit_delay(self) -> float:
        """Get delay between requests based on rate limit"""
        return 1.0 / self.rate_limit if self.rate_limit > 0 else 0
