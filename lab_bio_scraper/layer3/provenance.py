"""
Provenance tracking system for biological data metadata
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import hashlib
import json

from ..core.schema import UnifiedMetadata

class ProvenanceTracker:
    """Tracks provenance and lineage of biological data metadata"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.enable_lineage_tracking = self.config.get("enable_lineage", True)
        self.enable_version_tracking = self.config.get("enable_version", True)
        
    def track(self, metadata: UnifiedMetadata) -> Dict[str, Any]:
        """Track provenance information for metadata"""
        provenance = {
            "source_repository": metadata.source,
            "source_accession": metadata.accession,
            "source_version": metadata.source_version,
            "retrieved_at": datetime.utcnow().isoformat(),
            "retriever": "lab-bio-scraper v0.1.0",
            "processing_chain": self._get_processing_chain(metadata),
            "data_lineage": self._get_data_lineage(metadata) if self.enable_lineage_tracking else None,
            "checksum": self._calculate_checksum(metadata),
            "transformations": self._get_transformations(metadata),
            "quality_assessments": self._get_quality_assessments(metadata)
        }
        
        return provenance
    
    def _get_processing_chain(self, metadata: UnifiedMetadata) -> List[Dict[str, Any]]:
        """Get processing chain information"""
        chain = [
            {
                "step": "ingestion",
                "source": metadata.source,
                "timestamp": datetime.utcnow().isoformat(),
                "description": f"Data ingested from {metadata.source}"
            },
            {
                "step": "normalization", 
                "timestamp": datetime.utcnow().isoformat(),
                "description": "Metadata normalized to unified schema"
            }
        ]
        
        # Add processing steps if available
        if metadata.processing:
            chain.append({
                "step": "data_processing",
                "pipeline": metadata.processing.pipeline,
                "software": metadata.processing.software,
                "timestamp": datetime.utcnow().isoformat(),
                "description": "Original data processing information"
            })
        
        return chain
    
    def _get_data_lineage(self, metadata: UnifiedMetadata) -> Dict[str, Any]:
        """Get data lineage information"""
        lineage = {
            "original_source": metadata.source,
            "original_accession": metadata.accession,
            "derivatives": [],
            "related_records": [],
            "processing_history": []
        }
        
        # Check for related accessions in raw metadata
        raw_meta = metadata.raw_metadata
        
        # Look for related records
        if "BioSample" in str(raw_meta):
            lineage["related_records"].append("biosample")
        
        if "BioProject" in str(raw_meta):
            lineage["related_records"].append("bioproject")
        
        if "Publication" in str(raw_meta):
            lineage["related_records"].append("publication")
        
        return lineage
    
    def _calculate_checksum(self, metadata: UnifiedMetadata) -> str:
        """Calculate checksum for metadata integrity"""
        # Create canonical representation
        canonical = {
            "accession": metadata.accession,
            "source": metadata.source,
            "title": metadata.title,
            "data_type": metadata.data_type
        }
        
        # Add key metadata fields
        if metadata.organisms:
            canonical["organisms"] = [org.dict() for org in metadata.organisms]
        
        if metadata.samples:
            canonical["samples"] = [sample.dict() for sample in metadata.samples]
        
        # Calculate checksum
        content = json.dumps(canonical, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(content.encode()).hexdigest()
    
    def _get_transformations(self, metadata: UnifiedMetadata) -> List[Dict[str, Any]]:
        """Get list of transformations applied"""
        transformations = []
        
        # Check if text cleaning was applied
        if metadata.title != metadata.raw_metadata.get("original_title", metadata.title):
            transformations.append({
                "type": "text_cleaning",
                "field": "title",
                "description": "Text cleaned and normalized"
            })
        
        # Check if dates were normalized
        if metadata.raw_metadata.get("original_date_format"):
            transformations.append({
                "type": "date_normalization",
                "field": "date",
                "description": "Date format normalized to ISO"
            })
        
        # Check if taxonomy was normalized
        if metadata.raw_metadata.get("taxonomy_normalized"):
            transformations.append({
                "type": "taxonomy_normalization",
                "field": "organism",
                "description": "Organism names normalized to scientific format"
            })
        
        return transformations
    
    def _get_quality_assessments(self, metadata: UnifiedMetadata) -> List[Dict[str, Any]]:
        """Get quality assessment history"""
        assessments = []
        
        # Add current quality assessment
        if metadata.quality_metrics:
            assessments.append({
                "timestamp": datetime.utcnow().isoformat(),
                "scorer": "lab-bio-scraper v0.1.0",
                "metrics": {
                    "completeness": metadata.quality_metrics.completeness_score,
                    "consistency": metadata.quality_metrics.consistency_score,
                    "accuracy": metadata.quality_metrics.accuracy_score,
                    "overall": metadata.quality_metrics.overall_quality
                }
            })
        
        return assessments
    
    def track_version(self, metadata: UnifiedMetadata, previous_version: Optional[UnifiedMetadata] = None) -> Dict[str, Any]:
        """Track version changes"""
        if not self.enable_version_tracking:
            return {}
        
        version_info = {
            "current_version": metadata.source_version or "1.0",
            "version_timestamp": datetime.utcnow().isoformat(),
            "changes": []
        }
        
        if previous_version:
            changes = self._detect_changes(previous_version, metadata)
            version_info["changes"] = changes
            version_info["previous_version"] = previous_version.source_version
        
        return version_info
    
    def _detect_changes(self, old_metadata: UnifiedMetadata, new_metadata: UnifiedMetadata) -> List[Dict[str, Any]]:
        """Detect changes between metadata versions"""
        changes = []
        
        # Check for field changes
        fields_to_check = ["title", "description", "date_modified"]
        
        for field in fields_to_check:
            old_val = getattr(old_metadata, field)
            new_val = getattr(new_metadata, field)
            
            if old_val != new_val:
                changes.append({
                    "field": field,
                    "type": "field_change",
                    "old_value": old_val,
                    "new_value": new_val
                })
        
        # Check for file changes
        old_files = {f.filename: f for f in old_metadata.files}
        new_files = {f.filename: f for f in new_metadata.files}
        
        # Added files
        for filename in new_files:
            if filename not in old_files:
                changes.append({
                    "field": "files",
                    "type": "file_added",
                    "filename": filename
                })
        
        # Removed files
        for filename in old_files:
            if filename not in new_files:
                changes.append({
                    "field": "files", 
                    "type": "file_removed",
                    "filename": filename
                })
        
        return changes
    
    def create_provenance_graph(self, metadata_list: List[UnifiedMetadata]) -> Dict[str, Any]:
        """Create provenance graph for multiple records"""
        graph = {
            "nodes": [],
            "edges": [],
            "metadata": {
                "created_at": datetime.utcnow().isoformat(),
                "total_records": len(metadata_list)
            }
        }
        
        # Create nodes
        for i, metadata in enumerate(metadata_list):
            node = {
                "id": metadata.accession,
                "type": metadata.data_type,
                "source": metadata.source,
                "title": metadata.title,
                "index": i
            }
            graph["nodes"].append(node)
        
        # Create edges based on relationships
        for i, metadata in enumerate(metadata_list):
            # Look for related records
            for j, other_metadata in enumerate(metadata_list):
                if i != j:
                    relationship = self._detect_relationship(metadata, other_metadata)
                    if relationship:
                        edge = {
                            "source": metadata.accession,
                            "target": other_metadata.accession,
                            "relationship": relationship,
                            "type": "related"
                        }
                        graph["edges"].append(edge)
        
        return graph
    
    def _detect_relationship(self, metadata1: UnifiedMetadata, metadata2: UnifiedMetadata) -> Optional[str]:
        """Detect relationship between two metadata records"""
        # Same organism
        orgs1 = {org.scientific_name for org in metadata1.organisms}
        orgs2 = {org.scientific_name for org in metadata2.organisms}
        
        if orgs1.intersection(orgs2):
            return "same_organism"
        
        # Same data type
        if metadata1.data_type == metadata2.data_type:
            return "same_data_type"
        
        # Same source
        if metadata1.source == metadata2.source:
            return "same_source"
        
        return None
    
    def export_provenance(self, metadata: UnifiedMetadata, format: str = "json") -> str:
        """Export provenance information"""
        provenance = metadata.provenance
        
        if format == "json":
            return json.dumps(provenance, indent=2, default=str)
        elif format == "turtle":
            # Convert to RDF/Turtle format (simplified)
            return self._to_turtle(metadata, provenance)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _to_turtle(self, metadata: UnifiedMetadata, provenance: Dict[str, Any]) -> str:
        """Convert provenance to RDF/Turtle format"""
        lines = []
        
        # Prefixes
        lines.append("@prefix prov: <http://www.w3.org/ns/prov#> .")
        lines.append("@prefix dcterms: <http://purl.org/dc/terms/> .")
        lines.append("@prefix bio: <http://purl.org/bio/vocabulary/> .")
        lines.append("")
        
        # Main entity
        entity_uri = f"bio:{metadata.accession}"
        lines.append(f"{entity_uri} a prov:Entity ;")
        lines.append(f'    dcterms:identifier "{metadata.accession}" ;')
        lines.append(f'    dcterms:title "{metadata.title}" ;')
        lines.append(f'    bio:dataType "{metadata.data_type}" .')
        lines.append("")
        
        # Provenance information
        activity_uri = f"bio:{metadata.accession}_retrieval"
        lines.append(f"{activity_uri} a prov:Activity ;")
        lines.append(f'    prov:startedAtTime "{provenance.get('retrieved_at')}"^^xsd:dateTime ;')
        lines.append(f'    prov:used "{provenance.get('source_repository')}" .')
        lines.append("")
        
        return "\n".join(lines)
