"""
Unified metadata schema for biological data records
"""

from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum

class Organism(BaseModel):
    """Organism information"""
    scientific_name: str
    common_name: Optional[str] = None
    taxonomy_id: Optional[int] = None
    strain: Optional[str] = None
    
class Sample(BaseModel):
    """Sample information"""
    id: str
    organism: Optional[Organism] = None
    tissue: Optional[str] = None
    cell_type: Optional[str] = None
    disease: Optional[str] = None
    experimental_condition: Optional[str] = None
    age: Optional[str] = None
    sex: Optional[str] = None
    
class Technology(BaseModel):
    """Experimental technology information"""
    platform: str
    instrument: Optional[str] = None
    library_strategy: Optional[str] = None
    library_source: Optional[str] = None
    library_selection: Optional[str] = None
    
class Processing(BaseModel):
    """Data processing information"""
    pipeline: Optional[str] = None
    software: Optional[List[str]] = None
    version: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    
class QualityMetrics(BaseModel):
    """Data quality metrics"""
    completeness_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    accuracy_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    consistency_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    overall_quality: Optional[str] = None  # high, medium, low
    
class Citation(BaseModel):
    """Citation information"""
    title: str
    authors: List[str]
    journal: Optional[str] = None
    year: Optional[int] = None
    doi: Optional[str] = None
    pmid: Optional[str] = None
    
class File(BaseModel):
    """File information"""
    filename: str
    filesize: Optional[int] = None
    filetype: str
    checksum: Optional[str] = None
    checksum_type: Optional[str] = None
    download_url: str
    
class UnifiedMetadata(BaseModel):
    """Unified metadata schema for all biological data types"""
    
    # Core identifiers
    accession: str
    title: str
    description: Optional[str] = None
    
    # Classification
    data_type: str  # genomic_sequence, transcriptomic, proteomic, etc.
    sub_type: Optional[str] = None  # rna_seq, atac_seq, etc.
    
    # Sample and organism information
    samples: List[Sample] = []
    organisms: List[Organism] = []
    
    # Experimental information
    technology: Optional[Technology] = None
    
    # Processing and quality
    processing: Optional[Processing] = None
    quality_metrics: Optional[QualityMetrics] = None
    
    # File information
    files: List[File] = []
    
    # Provenance and versioning
    source: str  # ncbi_sra, geo, uniprot, etc.
    source_version: Optional[str] = None
    date_created: Optional[datetime] = None
    date_modified: Optional[datetime] = None
    
    # Citations and references
    citations: List[Citation] = []
    references: List[str] = []  # URLs, DOIs, etc.
    
    # Additional metadata
    raw_metadata: Dict[str, Any] = {}  # Original source metadata
    tags: List[str] = []
    keywords: List[str] = []
    
    # Validation and status
    is_validated: bool = False
    validation_errors: List[str] = []
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }
    
    def add_sample(self, sample: Sample) -> None:
        """Add a sample to the metadata"""
        self.samples.append(sample)
    
    def add_organism(self, organism: Organism) -> None:
        """Add an organism to the metadata"""
        self.organisms.append(organism)
    
    def add_file(self, file: File) -> None:
        """Add a file to the metadata"""
        self.files.append(file)
    
    def add_citation(self, citation: Citation) -> None:
        """Add a citation to the metadata"""
        self.citations.append(citation)
    
    def get_total_filesize(self) -> int:
        """Get total size of all files"""
        return sum(f.filesize or 0 for f in self.files)
    
    def get_filetypes(self) -> List[str]:
        """Get unique file types"""
        return list(set(f.filetype for f in self.files))
    
    def has_filetype(self, filetype: str) -> bool:
        """Check if metadata contains a specific file type"""
        return any(f.filetype == filetype for f in self.files)
