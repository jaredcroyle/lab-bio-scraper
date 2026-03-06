"""
Metadata enhancement system for adding provenance, quality scoring, and ML formatting
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import hashlib
import json

from ..core.schema import UnifiedMetadata, QualityMetrics
from .quality import QualityScorer
from .provenance import ProvenanceTracker
from .ml_formatter import MLFormatter

class MetadataEnhancer:
    """Enhances normalized metadata with quality scores, provenance, and ML formatting"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.quality_scorer = QualityScorer(config.get("quality", {}))
        self.provenance_tracker = ProvenanceTracker(config.get("provenance", {}))
        self.ml_formatter = MLFormatter(config.get("ml_formatting", {}))
        
    def enhance(self, metadata: UnifiedMetadata) -> UnifiedMetadata:
        """Enhance metadata with quality scores, provenance, and ML formatting"""
        
        # Add quality metrics
        quality_metrics = self.quality_scorer.score_metadata(metadata)
        metadata.quality_metrics = quality_metrics
        
        # Add provenance information
        provenance = self.provenance_tracker.track(metadata)
        metadata.provenance.update(provenance)
        
        # Add ML-ready features
        ml_features = self.ml_formatter.extract_features(metadata)
        metadata.raw_metadata["ml_features"] = ml_features
        
        # Add enhancement timestamp
        metadata.raw_metadata["enhanced_at"] = datetime.utcnow().isoformat()
        metadata.raw_metadata["enhancer_version"] = "0.1.0"
        
        # Validate enhanced metadata
        self._validate_enhanced_metadata(metadata)
        
        return metadata
    
    def batch_enhance(self, metadata_list: List[UnifiedMetadata]) -> List[UnifiedMetadata]:
        """Enhance multiple metadata records in batch"""
        enhanced = []
        
        # First pass: individual enhancement
        for metadata in metadata_list:
            enhanced_metadata = self.enhance(metadata)
            enhanced.append(enhanced_metadata)
        
        # Second pass: cross-record quality assessment
        self._assess_cross_record_quality(enhanced)
        
        return enhanced
    
    def _assess_cross_record_quality(self, metadata_list: List[UnifiedMetadata]):
        """Assess quality relative to other records in the batch"""
        if len(metadata_list) < 2:
            return
        
        # Calculate quality percentiles
        completeness_scores = []
        for metadata in metadata_list:
            if metadata.quality_metrics and metadata.quality_metrics.completeness_score is not None:
                completeness_scores.append(metadata.quality_metrics.completeness_score)
        
        if completeness_scores:
            completeness_scores.sort()
            
            for metadata in metadata_list:
                if metadata.quality_metrics and metadata.quality_metrics.completeness_score is not None:
                    score = metadata.quality_metrics.completeness_score
                    percentile = (len([s for s in completeness_scores if s <= score]) / len(completeness_scores))
                    
                    # Update quality ranking
                    if not metadata.raw_metadata.get("quality_ranking"):
                        metadata.raw_metadata["quality_ranking"] = {}
                    metadata.raw_metadata["quality_ranking"]["completeness_percentile"] = percentile
    
    def _validate_enhanced_metadata(self, metadata: UnifiedMetadata):
        """Validate enhanced metadata"""
        errors = []
        
        # Check required fields
        if not metadata.accession:
            errors.append("Missing accession")
        
        if not metadata.title:
            errors.append("Missing title")
        
        if not metadata.source:
            errors.append("Missing source")
        
        if not metadata.data_type:
            errors.append("Missing data_type")
        
        # Check quality metrics
        if metadata.quality_metrics:
            if metadata.quality_metrics.completeness_score is not None:
                if not (0 <= metadata.quality_metrics.completeness_score <= 1):
                    errors.append("Completeness score out of range")
            
            if metadata.quality_metrics.accuracy_score is not None:
                if not (0 <= metadata.quality_metrics.accuracy_score <= 1):
                    errors.append("Accuracy score out of range")
        
        # Update validation status
        metadata.is_validated = len(errors) == 0
        metadata.validation_errors = errors
    
    def generate_summary_stats(self, metadata_list: List[UnifiedMetadata]) -> Dict[str, Any]:
        """Generate summary statistics for a batch of enhanced metadata"""
        stats = {
            "total_records": len(metadata_list),
            "sources": {},
            "data_types": {},
            "quality_distribution": {"high": 0, "medium": 0, "low": 0},
            "avg_completeness": 0.0,
            "file_types": {},
            "organism_counts": {},
            "date_range": {"earliest": None, "latest": None}
        }
        
        completeness_scores = []
        dates = []
        
        for metadata in metadata_list:
            # Source distribution
            source = metadata.source
            stats["sources"][source] = stats["sources"].get(source, 0) + 1
            
            # Data type distribution
            data_type = metadata.data_type
            stats["data_types"][data_type] = stats["data_types"].get(data_type, 0) + 1
            
            # Quality distribution
            if metadata.quality_metrics and metadata.quality_metrics.overall_quality:
                quality = metadata.quality_metrics.overall_quality
                stats["quality_distribution"][quality] += 1
            
            # Completeness scores
            if metadata.quality_metrics and metadata.quality_metrics.completeness_score is not None:
                completeness_scores.append(metadata.quality_metrics.completeness_score)
            
            # File types
            for file in metadata.files:
                filetype = file.filetype
                stats["file_types"][filetype] = stats["file_types"].get(filetype, 0) + 1
            
            # Organism counts
            for organism in metadata.organisms:
                org_name = organism.scientific_name
                stats["organism_counts"][org_name] = stats["organism_counts"].get(org_name, 0) + 1
            
            # Date range
            if metadata.date_created:
                dates.append(metadata.date_created)
        
        # Calculate averages
        if completeness_scores:
            stats["avg_completeness"] = sum(completeness_scores) / len(completeness_scores)
        
        if dates:
            stats["date_range"]["earliest"] = min(dates).isoformat()
            stats["date_range"]["latest"] = max(dates).isoformat()
        
        return stats
    
    def export_enhanced_metadata(self, metadata_list: List[UnifiedMetadata], 
                               output_format: str = "json") -> str:
        """Export enhanced metadata in specified format"""
        if output_format == "json":
            return json.dumps([md.dict() for md in metadata_list], indent=2, default=str)
        elif output_format == "csv":
            # Convert to pandas DataFrame and export to CSV
            import pandas as pd
            
            records = []
            for metadata in metadata_list:
                record = {
                    "accession": metadata.accession,
                    "title": metadata.title,
                    "source": metadata.source,
                    "data_type": metadata.data_type,
                    "date_created": metadata.date_created.isoformat() if metadata.date_created else None,
                    "completeness_score": metadata.quality_metrics.completeness_score if metadata.quality_metrics else None,
                    "overall_quality": metadata.quality_metrics.overall_quality if metadata.quality_metrics else None,
                    "total_filesize": metadata.get_total_filesize(),
                    "num_files": len(metadata.files),
                    "num_organisms": len(metadata.organisms),
                    "num_samples": len(metadata.samples),
                }
                records.append(record)
            
            df = pd.DataFrame(records)
            return df.to_csv(index=False)
        else:
            raise ValueError(f"Unsupported export format: {output_format}")
    
    def create_data_package(self, metadata_list: List[UnifiedMetadata], 
                          output_dir: str) -> Dict[str, str]:
        """Create a complete data package with metadata and files"""
        import os
        import json
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Export metadata
        metadata_file = os.path.join(output_dir, "metadata.json")
        with open(metadata_file, 'w') as f:
            json.dump([md.dict() for md in metadata_list], f, indent=2, default=str)
        
        # Create summary stats
        stats = self.generate_summary_stats(metadata_list)
        stats_file = os.path.join(output_dir, "summary_stats.json")
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
        
        # Create manifest
        manifest = {
            "created_at": datetime.utcnow().isoformat(),
            "enhancer_version": "0.1.0",
            "total_records": len(metadata_list),
            "files": {
                "metadata.json": "Enhanced metadata in JSON format",
                "summary_stats.json": "Summary statistics for the dataset"
            }
        }
        
        manifest_file = os.path.join(output_dir, "manifest.json")
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        return {
            "metadata_file": metadata_file,
            "stats_file": stats_file,
            "manifest_file": manifest_file
        }
