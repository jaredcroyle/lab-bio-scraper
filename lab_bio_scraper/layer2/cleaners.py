"""
Data cleaning utilities for metadata normalization
"""

import re
from typing import Optional, Dict, Any
from datetime import datetime

class TextCleaner:
    """Cleans and normalizes text fields"""
    
    def __init__(self):
        # Patterns for cleaning
        self.whitespace_pattern = re.compile(r'\s+')
        self.html_pattern = re.compile(r'<[^>]+>')
        self.control_chars = re.compile(r'[\x00-\x1f\x7f-\x9f]')
        
    def clean(self, text: Optional[str]) -> Optional[str]:
        """Clean text by removing unwanted characters and normalizing whitespace"""
        if not text:
            return None
        
        # Remove HTML tags
        text = self.html_pattern.sub(' ', text)
        
        # Remove control characters
        text = self.control_chars.sub(' ', text)
        
        # Normalize whitespace
        text = self.whitespace_pattern.sub(' ', text)
        
        # Strip leading/trailing whitespace
        text = text.strip()
        
        return text if text else None
    
    def normalize_case(self, text: str, case_type: str = "title") -> str:
        """Normalize text case"""
        if case_type == "title":
            return text.title()
        elif case_type == "upper":
            return text.upper()
        elif case_type == "lower":
            return text.lower()
        return text
    
    def extract_keywords(self, text: str, min_length: int = 3) -> list:
        """Extract keywords from text"""
        # Simple keyword extraction - can be enhanced with NLP
        words = re.findall(r'\b[a-zA-Z]{' + str(min_length) + ',}\b', text.lower())
        return list(set(words))

class DateNormalizer:
    """Normalizes dates from various formats"""
    
    def __init__(self):
        self.date_formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y",
            "%m/%Y",
        ]
    
    def normalize(self, date_str: Optional[str]) -> Optional[datetime]:
        """Normalize date string to datetime object"""
        if not date_str:
            return None
        
        # Try each format
        for fmt in self.date_formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        # Try to extract year as fallback
        year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
        if year_match:
            year = int(year_match.group())
            return datetime(year, 1, 1)
        
        return None
    
    def to_iso_format(self, date_str: Optional[str]) -> Optional[str]:
        """Convert date string to ISO format"""
        dt = self.normalize(date_str)
        return dt.isoformat() if dt else None

class TaxonomyNormalizer:
    """Normalizes organism and taxonomy information"""
    
    def __init__(self):
        # Common organism name mappings
        self.organism_mappings = {
            "human": "Homo sapiens",
            "mouse": "Mus musculus", 
            "rat": "Rattus norvegicus",
            "fruit fly": "Drosophila melanogaster",
            "yeast": "Saccharomyces cerevisiae",
            "e. coli": "Escherichia coli",
            "arabidopsis": "Arabidopsis thaliana",
            "zebrafish": "Danio rerio",
        }
        
        # Taxonomy ID mappings (simplified)
        self.taxonomy_mappings = {
            "Homo sapiens": 9606,
            "Mus musculus": 10090,
            "Rattus norvegicus": 10116,
            "Drosophila melanogaster": 7227,
            "Saccharomyces cerevisiae": 4932,
            "Escherichia coli": 562,
            "Arabidopsis thaliana": 3702,
            "Danio rerio": 7955,
        }
    
    def normalize_organism_name(self, name: Optional[str]) -> Optional[str]:
        """Normalize organism name to scientific name"""
        if not name:
            return None
        
        name = name.strip().lower()
        
        # Check mappings
        if name in self.organism_mappings:
            return self.organism_mappings[name]
        
        # If already in scientific name format, capitalize properly
        if " " in name and not name.islower():
            parts = name.split()
            return " ".join(part.capitalize() for part in parts)
        
        return name.capitalize() if name else None
    
    def get_taxonomy_id(self, organism_name: Optional[str]) -> Optional[int]:
        """Get taxonomy ID for organism"""
        if not organism_name:
            return None
        
        normalized = self.normalize_organism_name(organism_name)
        return self.taxonomy_mappings.get(normalized)
    
    def extract_organism_from_text(self, text: str) -> Optional[str]:
        """Extract organism name from text"""
        text_lower = text.lower()
        
        # Check for common organism names
        for common_name, scientific_name in self.organism_mappings.items():
            if common_name in text_lower or scientific_name.lower() in text_lower:
                return scientific_name
        
        # Look for patterns like "Homo sapiens" or "Mus musculus"
        organism_pattern = r'\b[A-Z][a-z]+ [a-z]+\b'
        matches = re.findall(organism_pattern, text)
        
        for match in matches:
            if match in self.taxonomy_mappings:
                return match
        
        return None

class QualityMetricsNormalizer:
    """Normalizes quality metrics and scores"""
    
    def normalize_score(self, score: Any, min_val: float = 0.0, max_val: float = 1.0) -> Optional[float]:
        """Normalize score to 0-1 range"""
        if score is None:
            return None
        
        try:
            score_float = float(score)
            
            # If already in 0-1 range, return as-is
            if min_val <= score_float <= max_val:
                return score_float
            
            # If in different range, normalize
            if score_float > max_val:
                return min(score_float / 100.0, 1.0)  # Assume percentage
            
            return max(min(score_float, max_val), min_val)
            
        except (ValueError, TypeError):
            return None
    
    def categorize_quality(self, score: Optional[float]) -> Optional[str]:
        """Categorize quality score into high/medium/low"""
        if score is None:
            return None
        
        if score >= 0.8:
            return "high"
        elif score >= 0.6:
            return "medium"
        else:
            return "low"

class TechnologyNormalizer:
    """Normalizes technology and platform information"""
    
    def __init__(self):
        self.platform_mappings = {
            "illumina hiseq": "Illumina HiSeq",
            "illumina novaseq": "Illumina NovaSeq", 
            "illumina miseq": "Illumina MiSeq",
            "ion torrent": "Ion Torrent",
            "pacbio": "PacBio",
            "oxford nanopore": "Oxford Nanopore",
            "454": "Roche 454",
        }
        
        self.library_strategy_mappings = {
            "rna-seq": "RNA-Seq",
            "chip-seq": "ChIP-Seq",
            "atac-seq": "ATAC-Seq",
            "wgs": "WGS",
            "wxs": "WXS",
            "bisulfite-seq": "Bisulfite-Seq",
            "hic": "Hi-C",
        }
    
    def normalize_platform(self, platform: Optional[str]) -> Optional[str]:
        """Normalize platform name"""
        if not platform:
            return None
        
        platform_lower = platform.lower().strip()
        
        for key, value in self.platform_mappings.items():
            if key in platform_lower:
                return value
        
        return platform
    
    def normalize_library_strategy(self, strategy: Optional[str]) -> Optional[str]:
        """Normalize library strategy"""
        if not strategy:
            return None
        
        strategy_lower = strategy.lower().strip()
        
        for key, value in self.library_strategy_mappings.items():
            if key in strategy_lower:
                return value
        
        return strategy
