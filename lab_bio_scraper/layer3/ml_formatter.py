"""
ML formatting system for preparing biological data for machine learning workflows
"""

from typing import Dict, List, Optional, Any, Union
import re
from datetime import datetime

from ..core.schema import UnifiedMetadata

class MLFormatter:
    """Formats metadata and data for ML-ready consumption"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.feature_types = self.config.get("feature_types", ["categorical", "numerical", "text"])
        self.include_text_features = self.config.get("include_text_features", True)
        self.max_text_length = self.config.get("max_text_length", 1000)
        
    def extract_features(self, metadata: UnifiedMetadata) -> Dict[str, Any]:
        """Extract ML-ready features from metadata"""
        features = {
            "categorical_features": self._extract_categorical_features(metadata),
            "numerical_features": self._extract_numerical_features(metadata),
            "text_features": self._extract_text_features(metadata) if self.include_text_features else {},
            "list_features": self._extract_list_features(metadata),
            "temporal_features": self._extract_temporal_features(metadata),
            "quality_features": self._extract_quality_features(metadata),
            "file_features": self._extract_file_features(metadata)
        }
        
        # Add metadata identifiers
        features["identifiers"] = {
            "accession": metadata.accession,
            "source": metadata.source,
            "data_type": metadata.data_type
        }
        
        return features
    
    def _extract_categorical_features(self, metadata: UnifiedMetadata) -> Dict[str, str]:
        """Extract categorical features"""
        features = {}
        
        # Basic categorical fields
        features["source"] = metadata.source
        features["data_type"] = metadata.data_type
        
        if metadata.sub_type:
            features["sub_type"] = metadata.sub_type
        
        # Organism information
        if metadata.organisms:
            # Primary organism (first one)
            primary_organism = metadata.organisms[0]
            features["primary_organism"] = primary_organism.scientific_name
            
            if primary_organism.common_name:
                features["primary_organism_common"] = primary_organism.common_name
            
            # Organism category (mammal, plant, bacteria, etc.)
            features["organism_category"] = self._categorize_organism(primary_organism.scientific_name)
        
        # Technology information
        if metadata.technology:
            tech = metadata.technology
            if tech.platform:
                features["platform"] = tech.platform
            if tech.library_strategy:
                features["library_strategy"] = tech.library_strategy
            if tech.library_source:
                features["library_source"] = tech.library_source
        
        # Sample information
        if metadata.samples:
            sample = metadata.samples[0]
            if sample.tissue:
                features["tissue_type"] = sample.tissue
            if sample.cell_type:
                features["cell_type"] = sample.cell_type
            if sample.disease:
                features["disease"] = sample.disease
            if sample.sex:
                features["sex"] = sample.sex
        
        return features
    
    def _extract_numerical_features(self, metadata: UnifiedMetadata) -> Dict[str, Union[int, float]]:
        """Extract numerical features"""
        features = {}
        
        # File statistics
        if metadata.files:
            features["num_files"] = len(metadata.files)
            features["total_filesize"] = metadata.get_total_filesize()
            
            # File size statistics
            file_sizes = [f.filesize for f in metadata.files if f.filesize]
            if file_sizes:
                features["avg_filesize"] = sum(file_sizes) / len(file_sizes)
                features["max_filesize"] = max(file_sizes)
                features["min_filesize"] = min(file_sizes)
            
            # File type counts
            file_types = metadata.get_filetypes()
            for filetype in file_types:
                count = len([f for f in metadata.files if f.filetype == filetype])
                features[f"num_{filetype}_files"] = count
        
        # Sample statistics
        if metadata.samples:
            features["num_samples"] = len(metadata.samples)
        
        # Organism statistics
        if metadata.organisms:
            features["num_organisms"] = len(metadata.organisms)
        
        # Citation statistics
        if metadata.citations:
            features["num_citations"] = len(metadata.citations)
            
            # Publication year statistics
            years = [c.year for c in metadata.citations if c.year]
            if years:
                features["earliest_publication_year"] = min(years)
                features["latest_publication_year"] = max(years)
        
        # Quality metrics
        if metadata.quality_metrics:
            qm = metadata.quality_metrics
            if qm.completeness_score is not None:
                features["completeness_score"] = qm.completeness_score
            if qm.consistency_score is not None:
                features["consistency_score"] = qm.consistency_score
            if qm.accuracy_score is not None:
                features["accuracy_score"] = qm.accuracy_score
            
            # Quality score as categorical
            if qm.overall_quality:
                features["quality_category"] = self._quality_to_numeric(qm.overall_quality)
        
        return features
    
    def _extract_text_features(self, metadata: UnifiedMetadata) -> Dict[str, str]:
        """Extract text features for NLP"""
        features = {}
        
        # Title and description
        if metadata.title:
            features["title"] = self._clean_text(metadata.title[:self.max_text_length])
        
        if metadata.description:
            features["description"] = self._clean_text(metadata.description[:self.max_text_length])
        
        # Combined text for full-text analysis
        text_parts = []
        
        if metadata.title:
            text_parts.append(metadata.title)
        
        if metadata.description:
            text_parts.append(metadata.description)
        
        # Add sample descriptions
        for sample in metadata.samples:
            if sample.experimental_condition:
                text_parts.append(sample.experimental_condition)
        
        # Add keywords and tags
        if metadata.keywords:
            text_parts.extend(metadata.keywords)
        
        if metadata.tags:
            text_parts.extend(metadata.tags)
        
        combined_text = " ".join(text_parts)
        if combined_text:
            features["combined_text"] = self._clean_text(combined_text[:self.max_text_length])
        
        # Extract entities from text
        if combined_text:
            features["extracted_entities"] = self._extract_entities(combined_text)
        
        return features
    
    def _extract_list_features(self, metadata: UnifiedMetadata) -> Dict[str, List[str]]:
        """Extract list-based features"""
        features = {}
        
        # Keywords and tags
        if metadata.keywords:
            features["keywords"] = metadata.keywords
        
        if metadata.tags:
            features["tags"] = metadata.tags
        
        # File types
        if metadata.files:
            features["file_types"] = metadata.get_filetypes()
        
        # Organism list
        if metadata.organisms:
            features["organisms"] = [org.scientific_name for org in metadata.organisms]
        
        # Tissue types
        tissues = set()
        for sample in metadata.samples:
            if sample.tissue:
                tissues.add(sample.tissue)
        if tissues:
            features["tissue_types"] = list(tissues)
        
        # Cell types
        cell_types = set()
        for sample in metadata.samples:
            if sample.cell_type:
                cell_types.add(sample.cell_type)
        if cell_types:
            features["cell_types"] = list(cell_types)
        
        return features
    
    def _extract_temporal_features(self, metadata: UnifiedMetadata) -> Dict[str, Union[int, str]]:
        """Extract temporal features"""
        features = {}
        
        if metadata.date_created:
            dt = metadata.date_created
            features["creation_year"] = dt.year
            features["creation_month"] = dt.month
            features["creation_day_of_year"] = dt.timetuple().tm_yday
            features["creation_decade"] = (dt.year // 10) * 10
        
        if metadata.date_modified:
            dt = metadata.date_modified
            features["modification_year"] = dt.year
            features["modification_month"] = dt.month
        
        # Time since creation (in days)
        if metadata.date_created:
            days_since_creation = (datetime.utcnow() - metadata.date_created).days
            features["days_since_creation"] = days_since_creation
            
            # Age category
            if days_since_creation < 365:
                features["data_age_category"] = "recent"
            elif days_since_creation < 365 * 5:
                features["data_age_category"] = "moderate"
            else:
                features["data_age_category"] = "old"
        
        return features
    
    def _extract_quality_features(self, metadata: UnifiedMetadata) -> Dict[str, Any]:
        """Extract quality-related features"""
        features = {}
        
        if metadata.quality_metrics:
            qm = metadata.quality_metrics
            
            # Individual scores
            if qm.completeness_score is not None:
                features["completeness_score"] = qm.completeness_score
                features["completeness_category"] = self._categorize_score(qm.completeness_score)
            
            if qm.consistency_score is not None:
                features["consistency_score"] = qm.consistency_score
                features["consistency_category"] = self._categorize_score(qm.consistency_score)
            
            if qm.accuracy_score is not None:
                features["accuracy_score"] = qm.accuracy_score
                features["accuracy_category"] = self._categorize_score(qm.accuracy_score)
            
            if qm.overall_quality:
                features["overall_quality"] = qm.overall_quality
        
        # Validation status
        features["is_validated"] = metadata.is_validated
        features["validation_error_count"] = len(metadata.validation_errors)
        
        return features
    
    def _extract_file_features(self, metadata: UnifiedMetadata) -> Dict[str, Any]:
        """Extract file-related features"""
        features = {}
        
        if not metadata.files:
            return features
        
        # File type distribution
        file_types = {}
        for file in metadata.files:
            filetype = file.filetype
            file_types[filetype] = file_types.get(filetype, 0) + 1
        
        features["file_type_distribution"] = file_types
        
        # File size statistics
        sizes = [f.filesize for f in metadata.files if f.filesize]
        if sizes:
            features["file_size_stats"] = {
                "count": len(sizes),
                "mean": sum(sizes) / len(sizes),
                "min": min(sizes),
                "max": max(sizes)
            }
        
        # Checksum availability
        has_checksum = any(f.checksum for f in metadata.files)
        features["has_checksums"] = has_checksum
        
        return features
    
    def _categorize_organism(self, organism_name: str) -> str:
        """Categorize organism type"""
        organism_lower = organism_name.lower()
        
        if any(keyword in organism_lower for keyword in ["homo sapiens", "mus musculus", "rattus norvegicus"]):
            return "mammal"
        elif any(keyword in organism_lower for keyword in ["arabidopsis", "zea mays", "oryza"]):
            return "plant"
        elif any(keyword in organism_lower for keyword in ["drosophila", "caenorhabditis"]):
            return "invertebrate"
        elif any(keyword in organism_lower for keyword in ["escherichia", "bacillus", "staphylococcus"]):
            return "bacteria"
        elif any(keyword in organism_lower for keyword in ["saccharomyces", "candida"]):
            return "yeast"
        elif any(keyword in organism_lower for keyword in ["influenza", "hiv", "coronavirus"]):
            return "virus"
        else:
            return "other"
    
    def _categorize_score(self, score: float) -> str:
        """Categorize quality score"""
        if score >= 0.8:
            return "high"
        elif score >= 0.6:
            return "medium"
        else:
            return "low"
    
    def _quality_to_numeric(self, quality: str) -> int:
        """Convert quality category to numeric"""
        mapping = {"high": 3, "medium": 2, "low": 1}
        return mapping.get(quality, 0)
    
    def _clean_text(self, text: str) -> str:
        """Clean text for ML processing"""
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s\.\,\!\?\-\(\)]', ' ', text)
        
        return text.strip()
    
    def _extract_entities(self, text: str) -> Dict[str, List[str]]:
        """Extract biological entities from text (simplified)"""
        entities = {
            "genes": [],
            "proteins": [],
            "diseases": [],
            "chemicals": []
        }
        
        # Simple pattern matching (in practice, would use NER models)
        # Gene/protein patterns (capitalized words)
        gene_pattern = r'\b[A-Z][A-Z0-9]{2,}\b'
        entities["genes"] = list(set(re.findall(gene_pattern, text)))
        
        # Disease patterns
        disease_keywords = ["cancer", "diabetes", "alzheimer", "parkinson", "leukemia"]
        for keyword in disease_keywords:
            if keyword.lower() in text.lower():
                entities["diseases"].append(keyword)
        
        return entities
    
    def format_for_training(self, features_list: List[Dict[str, Any]], 
                          target_format: str = "pandas") -> Union[Any, str]:
        """Format features for ML training"""
        if target_format == "pandas":
            return self._to_pandas_dataframe(features_list)
        elif target_format == "numpy":
            return self._to_numpy_arrays(features_list)
        elif target_format == "torch":
            return self._to_torch_tensors(features_list)
        elif target_format == "tensorflow":
            return self._to_tensorflow_dataset(features_list)
        else:
            raise ValueError(f"Unsupported format: {target_format}")
    
    def _to_pandas_dataframe(self, features_list: List[Dict[str, Any]]) -> Any:
        """Convert to pandas DataFrame"""
        import pandas as pd
        
        # Flatten features
        flattened_records = []
        
        for features in features_list:
            flat_record = {}
            
            # Flatten nested dictionaries
            for category, category_features in features.items():
                if isinstance(category_features, dict):
                    for key, value in category_features.items():
                        flat_key = f"{category}_{key}" if category != "identifiers" else key
                        flat_record[flat_key] = value
                else:
                    flat_record[category] = category_features
            
            flattened_records.append(flat_record)
        
        return pd.DataFrame(flattened_records)
    
    def _to_numpy_arrays(self, features_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Convert to numpy arrays"""
        import numpy as np
        
        # This is a simplified version - in practice would need proper encoding
        # of categorical variables and handling of variable-length features
        
        numerical_features = []
        for features in features_list:
            num_features = features.get("numerical_features", {})
            numerical_features.append(list(num_features.values()))
        
        return {
            "numerical": np.array(numerical_features, dtype=float),
            "categorical": [f.get("categorical_features", {}) for f in features_list],
            "text": [f.get("text_features", {}) for f in features_list]
        }
    
    def _to_torch_tensors(self, features_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Convert to PyTorch tensors"""
        try:
            import torch
            
            numpy_data = self._to_numpy_arrays(features_list)
            
            return {
                "numerical": torch.tensor(numpy_data["numerical"], dtype=torch.float),
                "categorical": numpy_data["categorical"],
                "text": numpy_data["text"]
            }
        except ImportError:
            raise ImportError("PyTorch not installed")
    
    def _to_tensorflow_dataset(self, features_list: List[Dict[str, Any]]) -> Any:
        """Convert to TensorFlow dataset"""
        try:
            import tensorflow as tf
            
            # Create dataset from features
            def generator():
                for features in features_list:
                    yield features
            
            return tf.data.Dataset.from_generator(generator, output_signature={
                "categorical_features": tf.TensorSpec(shape=(), dtype=tf.string),
                "numerical_features": tf.TensorSpec(shape=(None,), dtype=tf.float32),
                "text_features": tf.TensorSpec(shape=(), dtype=tf.string),
                "identifiers": tf.TensorSpec(shape=(), dtype=tf.string)
            })
        except ImportError:
            raise ImportError("TensorFlow not installed")
