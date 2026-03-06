"""
Metadata normalizer for converting source-specific metadata to unified schema
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import re
from urllib.parse import urlparse

from ..core.base import DataRecord, DataSource, DataType
from ..core.schema import UnifiedMetadata, Organism, Sample, Technology, File, Citation, Processing, QualityMetrics
from .cleaners import TextCleaner, DateNormalizer, TaxonomyNormalizer

class MetadataNormalizer:
    """Normalizes metadata from different sources into unified schema"""
    
    def __init__(self):
        self.text_cleaner = TextCleaner()
        self.date_normalizer = DateNormalizer()
        self.taxonomy_normalizer = TaxonomyNormalizer()
    
    def normalize_record(self, record: DataRecord) -> UnifiedMetadata:
        """Normalize a data record into unified metadata"""
        normalized = UnifiedMetadata(
            accession=record.accession,
            title=self.text_cleaner.clean(record.title),
            description=self.text_cleaner.clean(record.description),
            source=record.source.value,
            source_version=record.version,
            date_created=record.date_created,
            date_modified=record.date_modified,
            raw_metadata=record.metadata
        )
        
        # Source-specific normalization
        if record.source == DataSource.NCBI_SRA:
            self._normalize_ncbi_record(record, normalized)
        elif record.source == DataSource.GEO:
            self._normalize_geo_record(record, normalized)
        elif record.source == DataSource.ENA:
            self._normalize_ena_record(record, normalized)
        elif record.source == DataSource.UNIPROT:
            self._normalize_uniprot_record(record, normalized)
        elif record.source == DataSource.PDB:
            self._normalize_pdb_record(record, normalized)
        elif record.source == DataSource.ALPHAFOLD:
            self._normalize_alphafold_record(record, normalized)
        
        # Normalize files
        for url in record.download_urls:
            file_info = self._extract_file_info(url, record)
            if file_info:
                normalized.add_file(file_info)
        
        # Set data type
        normalized.data_type = record.data_type.value
        
        return normalized
    
    def _normalize_ncbi_record(self, record: DataRecord, normalized: UnifiedMetadata):
        """Normalize NCBI/SRA record"""
        metadata = record.metadata
        
        # Extract sample information
        if "xml" in metadata:
            # Parse XML for sample data
            import xml.etree.ElementTree as ET
            try:
                root = ET.fromstring(metadata["xml"])
                
                # Extract organism info
                for organism_elem in root.findall(".//ORGANISM"):
                    sci_name = organism_elem.find("SCIENTIFIC_NAME")
                    if sci_name is not None:
                        organism = Organism(scientific_name=sci_name.text)
                        normalized.add_organism(organism)
                
                # Extract sample information
                for sample_elem in root.findall(".//SAMPLE"):
                    sample_id = sample_elem.get("accession", "")
                    title_elem = sample_elem.find("TITLE")
                    title = title_elem.text if title_elem is not None else ""
                    
                    sample = Sample(id=sample_id)
                    if title:
                        sample.experimental_condition = title
                    normalized.add_sample(sample)
                
                # Extract technology info
                for lib_elem in root.findall(".//LIBRARY_DESCRIPTOR"):
                    strategy = lib_elem.find("LIBRARY_STRATEGY")
                    source = lib_elem.find("LIBRARY_SOURCE")
                    selection = lib_elem.find("LIBRARY_SELECTION")
                    
                    if strategy is not None:
                        technology = Technology(
                            platform="Illumina",  # Default assumption
                            library_strategy=strategy.text,
                            library_source=source.text if source is not None else None,
                            library_selection=selection.text if selection is not None else None
                        )
                        normalized.technology = technology
                        
            except ET.ParseError:
                pass
    
    def _normalize_geo_record(self, record: DataRecord, normalized: UnifiedMetadata):
        """Normalize GEO record"""
        metadata = record.metadata
        
        # Extract sample information from GEO fields
        if "sample" in metadata:
            sample_data = metadata["sample"]
            if isinstance(sample_data, list) and len(sample_data) > 0:
                sample = Sample(id=record.accession)
                
                # Extract organism
                if "organism" in metadata:
                    organism_data = metadata["organism"]
                    if isinstance(organism_data, list) and len(organism_data) > 0:
                        organism = Organism(scientific_name=organism_data[0])
                        normalized.add_organism(organism)
                        sample.organism = organism
                
                normalized.add_sample(sample)
        
        # Set data type based on GEO series type
        if "series_type" in metadata:
            series_type = metadata["series_type"]
            if isinstance(series_type, list) and len(series_type) > 0:
                if "Expression profiling" in series_type[0]:
                    normalized.data_type = "transcriptomic"
                elif "Epigenomics" in series_type[0]:
                    normalized.sub_type = "epigenomic"
    
    def _normalize_ena_record(self, record: DataRecord, normalized: UnifiedMetadata):
        """Normalize ENA record"""
        metadata = record.metadata
        
        # Extract organism
        scientific_name = metadata.get("scientific_name")
        if scientific_name:
            organism = Organism(scientific_name=scientific_name)
            normalized.add_organism(organism)
        
        # Extract sample information
        sample_accession = metadata.get("sample_accession")
        if sample_accession:
            sample = Sample(id=sample_accession)
            sample.organism = organism if scientific_name else None
            normalized.add_sample(sample)
        
        # Extract technology information
        instrument_model = metadata.get("instrument_model")
        library_strategy = metadata.get("library_strategy")
        
        if instrument_model or library_strategy:
            technology = Technology(
                platform=instrument_model or "Unknown",
                instrument=instrument_model,
                library_strategy=library_strategy
            )
            normalized.technology = technology
    
    def _normalize_uniprot_record(self, record: DataRecord, normalized: UnifiedMetadata):
        """Normalize UniProt record"""
        metadata = record.metadata
        
        # Extract organism information
        organisms = metadata.get("organisms", [])
        for org_data in organisms:
            scientific_name = org_data.get("scientificName")
            taxonomy_id = org_data.get("taxonId")
            
            if scientific_name:
                organism = Organism(
                    scientific_name=scientific_name,
                    taxonomy_id=int(taxonomy_id) if taxonomy_id else None
                )
                normalized.add_organism(organism)
        
        # Extract protein information as sample
        protein_name = metadata.get("proteinDescription", {}).get("recommendedName", {}).get("fullName", {}).get("value", "")
        length = metadata.get("sequence", {}).get("length", 0)
        
        sample = Sample(
            id=record.accession,
            experimental_condition=f"Protein length: {length} aa"
        )
        normalized.add_sample(sample)
        
        # Set data type
        normalized.data_type = "proteomic"
        normalized.sub_type = "protein_sequence"
        
        # Add citations
        citations = metadata.get("citations", [])
        for citation in citations:
            authors = [author.get("name", "") for author in citation.get("authors", [])]
            citation_obj = Citation(
                title=citation.get("title", ""),
                authors=authors,
                journal=citation.get("journal", {}).get("name"),
                year=citation.get("publicationDate", {}).get("year"),
                doi=citation.get("doi")
            )
            normalized.add_citation(citation_obj)
    
    def _normalize_pdb_record(self, record: DataRecord, normalized: UnifiedMetadata):
        """Normalize PDB record"""
        metadata = record.metadata
        
        # Extract organism information
        for entity in metadata.get("entity", []):
            organism = entity.get("organism_scientific")
            if organism:
                org_obj = Organism(scientific_name=organism)
                normalized.add_organism(org_obj)
        
        # Set data type
        normalized.data_type = "structural"
        normalized.sub_type = "protein_structure"
        
        # Extract experimental method
        method = metadata.get("exptl", [{}])[0].get("method", "")
        if method:
            technology = Technology(platform=method)
            normalized.technology = technology
    
    def _normalize_alphafold_record(self, record: DataRecord, normalized: UnifiedMetadata):
        """Normalize AlphaFold record"""
        metadata = record.metadata
        
        # AlphaFold predictions are based on UniProt entries
        # Organism info would need to be fetched from UniProt
        
        # Set data type
        normalized.data_type = "structural"
        normalized.sub_type = "predicted_structure"
        
        # Add processing information
        processing = Processing(
            pipeline="AlphaFold2",
            software=["AlphaFold2"],
            version="v4.0"
        )
        normalized.processing = processing
    
    def _extract_file_info(self, url: str, record: DataRecord) -> Optional[File]:
        """Extract file information from URL"""
        parsed = urlparse(url)
        filename = parsed.path.split('/')[-1]
        
        if not filename:
            return None
        
        # Determine file type from extension
        filetype = "unknown"
        if filename.endswith('.fasta') or filename.endswith('.fa'):
            filetype = "fasta"
        elif filename.endswith('.fastq') or filename.endswith('.fq'):
            filetype = "fastq"
        elif filename.endswith('.sra'):
            filetype = "sra"
        elif filename.endswith('.cif'):
            filetype = "cif"
        elif filename.endswith('.pdb'):
            filetype = "pdb"
        elif filename.endswith('.xml'):
            filetype = "xml"
        elif filename.endswith('.json'):
            filetype = "json"
        elif filename.endswith('.txt'):
            filetype = "text"
        
        return File(
            filename=filename,
            filetype=filetype,
            download_url=url
        )
    
    def batch_normalize(self, records: List[DataRecord]) -> List[UnifiedMetadata]:
        """Normalize multiple records in batch"""
        return [self.normalize_record(record) for record in records]
