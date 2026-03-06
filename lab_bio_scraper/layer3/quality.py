"""
Quality scoring system for biological data metadata
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

from ..core.schema import UnifiedMetadata, QualityMetrics

class QualityScorer:
    """Scores metadata quality based on completeness, consistency, and accuracy"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.weights = self.config.get("weights", {
            "completeness": 0.4,
            "consistency": 0.3,
            "accuracy": 0.3
        })
        
        # Required fields for different data types
        self.required_fields = {
            "genomic_sequence": ["accession", "title", "source", "organisms", "technology"],
            "transcriptomic": ["accession", "title", "source", "organisms", "samples"],
            "proteomic": ["accession", "title", "source", "organisms"],
            "structural": ["accession", "title", "source", "organisms"],
            "single_cell": ["accession", "title", "source", "organisms", "samples"],
            "metabolomic": ["accession", "title", "source", "organisms"],
            "biodiversity": ["accession", "title", "source", "organisms"]
        }
    
    def score_metadata(self, metadata: UnifiedMetadata) -> QualityMetrics:
        """Score metadata quality"""
        completeness = self._score_completeness(metadata)
        consistency = self._score_consistency(metadata)
        accuracy = self._score_accuracy(metadata)
        
        # Calculate overall score
        overall_score = (
            completeness * self.weights["completeness"] +
            consistency * self.weights["consistency"] +
            accuracy * self.weights["accuracy"]
        )
        
        # Determine quality category
        overall_quality = self._categorize_quality(overall_score)
        
        return QualityMetrics(
            completeness_score=completeness,
            consistency_score=consistency,
            accuracy_score=accuracy,
            overall_quality=overall_quality
        )
    
    def _score_completeness(self, metadata: UnifiedMetadata) -> float:
        """Score metadata completeness"""
        required = self.required_fields.get(metadata.data_type, [])
        if not required:
            return 1.0  # No required fields defined
        
        present_fields = 0
        total_fields = len(required)
        
        for field in required:
            if self._field_is_present(metadata, field):
                present_fields += 1
        
        base_score = present_fields / total_fields
        
        # Bonus for additional valuable fields
        bonus_fields = ["description", "citations", "processing", "date_created"]
        bonus_score = 0
        for field in bonus_fields:
            if self._field_is_present(metadata, field):
                bonus_score += 0.05
        
        # Bonus for file information
        if metadata.files:
            file_bonus = min(len(metadata.files) * 0.02, 0.1)
            bonus_score += file_bonus
        
        return min(base_score + bonus_score, 1.0)
    
    def _score_consistency(self, metadata: UnifiedMetadata) -> float:
        """Score metadata consistency"""
        score = 1.0
        deductions = 0
        
        # Check date consistency
        if metadata.date_created and metadata.date_modified:
            if metadata.date_modified < metadata.date_created:
                deductions += 0.3
        
        # Check organism consistency
        if metadata.organisms and metadata.samples:
            sample_organisms = [s.organism for s in metadata.samples if s.organism]
            if sample_organisms:
                for sample_org in sample_organisms:
                    if sample_org not in metadata.organisms:
                        deductions += 0.1
                        break
        
        # Check file type consistency with data type
        expected_types = self._get_expected_file_types(metadata.data_type)
        if expected_types and metadata.files:
            file_types = set(f.filetype for f in metadata.files)
            expected_set = set(expected_types)
            
            if not file_types.intersection(expected_set):
                deductions += 0.2
        
        return max(0.0, score - deductions)
    
    def _score_accuracy(self, metadata: UnifiedMetadata) -> float:
        """Score metadata accuracy (heuristic-based)"""
        score = 1.0
        deductions = 0
        
        # Check accession format
        if not self._validate_accession_format(metadata.accession, metadata.source):
            deductions += 0.3
        
        # Check for reasonable dates
        if metadata.date_created:
            if metadata.date_created > datetime.utcnow():
                deductions += 0.2
            if metadata.date_created < datetime(1990, 1, 1):  # Before most biological databases
                deductions += 0.1
        
        # Check organism names
        for organism in metadata.organisms:
            if not self._validate_organism_name(organism.scientific_name):
                deductions += 0.1
                break
        
        # Check file sizes for reasonableness
        for file in metadata.files:
            if file.filesize and file.filesize > 10**12:  # > 1TB seems unreasonable for single file
                deductions += 0.05
                break
        
        return max(0.0, score - deductions)
    
    def _field_is_present(self, metadata: UnifiedMetadata, field: str) -> bool:
        """Check if a field is present and not empty"""
        if not hasattr(metadata, field):
            return False
        
        value = getattr(metadata, field)
        
        if value is None:
            return False
        
        if isinstance(value, str) and not value.strip():
            return False
        
        if isinstance(value, (list, dict)) and not value:
            return False
        
        return True
    
    def _get_expected_file_types(self, data_type: str) -> List[str]:
        """Get expected file types for a data type"""
        file_type_mapping = {
            "genomic_sequence": ["fastq", "sra", "fasta"],
            "transcriptomic": ["fastq", "sra", "txt", "csv"],
            "proteomic": ["fasta", "xml", "txt", "mzml"],
            "structural": ["pdb", "cif", "mmcif"],
            "single_cell": ["fastq", "h5", "mtx"],
            "metabolomic": ["mzml", "mzxml", "raw"],
            "biodiversity": ["csv", "json", "xml"]
        }
        
        return file_type_mapping.get(data_type, [])
    
    def _validate_accession_format(self, accession: str, source: str) -> bool:
        """Validate accession format for the source"""
        import re
        
        patterns = {
            "ncbi_sra": r'^(SRR|DRR|ERR)\d{6,}$',
            "geo": r'^(GSE|GSM|GPL)\d{3,}$',
            "ena": r'^(SRR|ERR|DRR)\d{6,}$',
            "uniprot": r'^[A-Z0-9]{6,}$',
            "pdb": r'^[0-9][A-Z0-9]{3}$',
            "alphafold": r'^[A-Z0-9]{6,}$',
            "pride": r'^PXD\d{6,}$',
            "massive": r'^MSV\d{6,}$',
            "metabolights": r'^MTBLS\d{4,}$',
            "cellxgene": r'^[A-Z0-9_-]+$',
            "human_cell_atlas": r'^[A-Z0-9_-]+$',
            "gbif": r'^\d+$'
        }
        
        pattern = patterns.get(source)
        if pattern:
            return bool(re.match(pattern, accession))
        
        return True  # No pattern defined, assume valid
    
    def _validate_organism_name(self, name: str) -> bool:
        """Validate organism name format"""
        # Should have at least two words (genus species)
        if " " not in name:
            return False
        
        parts = name.split()
        if len(parts) < 2:
            return False
        
        # First letter of genus should be uppercase
        if not parts[0][0].isupper():
            return False
        
        # Species should be lowercase
        if not parts[1].islower():
            return False
        
        return True
    
    def _categorize_quality(self, score: float) -> str:
        """Categorize quality score"""
        if score >= 0.8:
            return "high"
        elif score >= 0.6:
            return "medium"
        else:
            return "low"
    
    def get_quality_report(self, metadata: UnifiedMetadata) -> Dict[str, Any]:
        """Generate detailed quality report"""
        metrics = self.score_metadata(metadata)
        
        report = {
            "overall_score": {
                "completeness": metrics.completeness_score,
                "consistency": metrics.consistency_score,
                "accuracy": metrics.accuracy_score,
                "overall": metrics.overall_quality
            },
            "completeness_details": self._get_completeness_details(metadata),
            "consistency_details": self._get_consistency_details(metadata),
            "accuracy_details": self._get_accuracy_details(metadata),
            "recommendations": self._get_recommendations(metadata, metrics)
        }
        
        return report
    
    def _get_completeness_details(self, metadata: UnifiedMetadata) -> Dict[str, Any]:
        """Get detailed completeness analysis"""
        required = self.required_fields.get(metadata.data_type, [])
        missing = []
        present = []
        
        for field in required:
            if self._field_is_present(metadata, field):
                present.append(field)
            else:
                missing.append(field)
        
        return {
            "required_fields": required,
            "present_fields": present,
            "missing_fields": missing,
            "completeness_percentage": len(present) / len(required) * 100 if required else 100
        }
    
    def _get_consistency_details(self, metadata: UnifiedMetadata) -> Dict[str, Any]:
        """Get detailed consistency analysis"""
        issues = []
        
        # Check date consistency
        if metadata.date_created and metadata.date_modified:
            if metadata.date_modified < metadata.date_created:
                issues.append("Modification date precedes creation date")
        
        # Check organism consistency
        if metadata.organisms and metadata.samples:
            sample_organisms = [s.organism for s in metadata.samples if s.organism]
            for sample_org in sample_organisms:
                if sample_org not in metadata.organisms:
                    issues.append(f"Sample organism {sample_org} not in main organism list")
        
        return {
            "issues": issues,
            "consistency_score": 1.0 - len(issues) * 0.1
        }
    
    def _get_accuracy_details(self, metadata: UnifiedMetadata) -> Dict[str, Any]:
        """Get detailed accuracy analysis"""
        issues = []
        
        # Check accession format
        if not self._validate_accession_format(metadata.accession, metadata.source):
            issues.append("Invalid accession format")
        
        # Check dates
        if metadata.date_created and metadata.date_created > datetime.utcnow():
            issues.append("Creation date is in the future")
        
        return {
            "issues": issues,
            "accuracy_score": 1.0 - len(issues) * 0.1
        }
    
    def _get_recommendations(self, metadata: UnifiedMetadata, metrics: QualityMetrics) -> List[str]:
        """Get improvement recommendations"""
        recommendations = []
        
        if metrics.completeness_score < 0.8:
            required = self.required_fields.get(metadata.data_type, [])
            for field in required:
                if not self._field_is_present(metadata, field):
                    recommendations.append(f"Add {field} field")
        
        if not metadata.description:
            recommendations.append("Add description")
        
        if not metadata.citations:
            recommendations.append("Add citations/references")
        
        if not metadata.files:
            recommendations.append("Add file information")
        
        return recommendations
