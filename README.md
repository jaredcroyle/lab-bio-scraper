hey, this is our webscraper for collecting bio-data for cleaning and annotation. the future is here - get ready for huggingface's and github's baby they left at a biotech lab ( ˘ ³˘)

# Lab Bio Scraper

A comprehensive biological data scraper and normalizer for major bioinformatics repositories, designed as a "Hugging Face for biological data."

## Overview

Lab Bio Scraper provides a unified interface to access, normalize, and enhance biological data from major repositories. The architecture follows a layered approach:

- **Layer 1**: Official repository ingestion (APIs, FTP, bulk downloads)
- **Layer 2**: Metadata normalization into unified schema  
- **Layer 3**: Provenance tracking, quality scoring, ML formatting
- **Layer 4**: Supplementary scraping for niche sources (future)

## Supported Data Sources

### Genomics / Sequencing
- **NCBI SRA** - Raw sequencing data
- **GEO** - Expression, epigenomics, bulk + single-cell functional genomics
- **ENA** - European counterpart to sequence archives

### Protein / Structure  
- **UniProt** - Protein sequence + functional annotation
- **RCSB PDB** - Experimentally determined 3D structures
- **AlphaFold DB** - Predicted structures at huge scale

### Transcriptomics / Single-cell
- **GEO** - Single-cell submissions
- **CELLxGENE** - Standardized, API-friendly access for ML-scale use
- **Human Cell Atlas** - Single-cell reference datasets

### Proteomics / Metabolomics
- **PRIDE / ProteomeXchange** - Main proteomics ecosystem
- **MassIVE** - Mass spectrometry workflows
- **MetaboLights** - Major metabolomics repository

### Biodiversity / Organismal
- **GBIF** - Central biodiversity source for organism occurrence

## Installation

```bash
pip install -e .
```

## Quick Start

### Command Line Interface

```bash
# Search NCBI SRA for RNA-seq data
lab-bio-scraper search ncbi "RNA-seq human" --limit 10 --normalize --enhance

# Get a specific record
lab-bio-scraper get uniprot P12345 --normalize --enhance --output P12345.json

# Batch process multiple accessions
lab-bio-scraper batch accessions.csv output_dir/ --normalize --enhance

# List available sources
lab-bio-scraper sources

# Validate accession format
lab-bio-scraper validate SRR123456
```

### Python API

```python
from lab_bio_scraper import NCBIConnector, MetadataNormalizer, MetadataEnhancer

# Initialize connector
connector = NCBIConnector()

# Search for records
records = list(connector.search("RNA-seq human", limit=10))

# Normalize metadata
normalizer = MetadataNormalizer()
normalized_records = [normalizer.normalize_record(record) for record in records]

# Enhance with quality scores and provenance
enhancer = MetadataEnhancer()
enhanced_records = enhancer.batch_enhance(normalized_records)

# Export results
enhancer.create_data_package(enhanced_records, "output_dir")
```

## Architecture

### Layer 1: Data Connectors

Base connector class with implementations for each repository:

```python
from lab_bio_scraper.core.base import BaseConnector

class NCBIConnector(BaseConnector):
    def search(self, query: str, limit: int = 100) -> Iterator[DataRecord]:
        # Search NCBI databases
    
    def get_record(self, accession: str) -> Optional[DataRecord]:
        # Get specific record
```

### Layer 2: Metadata Normalization

Convert source-specific metadata to unified schema:

```python
from lab_bio_scraper.layer2.normalizer import MetadataNormalizer

normalizer = MetadataNormalizer()
unified_metadata = normalizer.normalize_record(record)
```

### Layer 3: Enhancement System

Add quality scoring, provenance tracking, and ML formatting:

```python
from lab_bio_scraper.layer3.enhancer import MetadataEnhancer

enhancer = MetadataEnhancer()
enhanced_metadata = enhancer.enhance(unified_metadata)
```

## Data Sources Priority

Recommended order for building a comprehensive foundation:

1. **GEO + SRA + ENA** - Strongest genomics/transcriptomics coverage
2. **UniProt + PDB + AlphaFold** - Protein sequences and structures  
3. **PRIDE + MassIVE + MetaboLights** - Proteomics and metabolomics
4. **CELLxGENE + Human Cell Atlas** - Single-cell data
5. **GBIF** - Biodiversity and ecological context

## Quality Scoring

The system automatically scores metadata quality based on:

- **Completeness** (40%): Required fields present
- **Consistency** (30%): Internal consistency checks  
- **Accuracy** (30%): Format validation and reasonableness checks

Quality categories: High (≥0.8), Medium (≥0.6), Low (<0.6)

## ML-Ready Features

Enhanced metadata includes ML-ready features:

- **Categorical**: Source, data type, organism, platform
- **Numerical**: File counts, sizes, quality scores
- **Text**: Cleaned titles, descriptions, combined text
- **Temporal**: Creation dates, age categories
- **List**: Keywords, file types, tissues, cell types

Export formats: pandas DataFrame, numpy arrays, PyTorch tensors, TensorFlow datasets

## Configuration

```python
config = {
    "quality": {
        "weights": {
            "completeness": 0.4,
            "consistency": 0.3, 
            "accuracy": 0.3
        }
    },
    "ml_formatting": {
        "include_text_features": True,
        "max_text_length": 1000
    }
}

enhancer = MetadataEnhancer(config)
```

## Data Packages

Enhanced datasets are exported as complete packages:

```
output_dir/
├── metadata.json          # Enhanced metadata
├── summary_stats.json     # Dataset statistics  
├── manifest.json          # Package information
└── provenance/            # Provenance tracking
```

## Contributing

1. Add new connectors in `lab_bio_scraper/layer1/connectors.py`
2. Extend the unified schema in `lab_bio_scraper/core/schema.py`
3. Add quality metrics in `lab_bio_scraper/layer3/quality.py`
4. Update CLI commands in `lab_bio_scraper/cli.py`

## License

MIT License - see LICENSE file for details.

## Citation

If you use Lab Bio Scraper in your research, please cite:

```
Lab Bio Scraper: A Comprehensive Framework for Biological Data Integration
[Your citation here]
```