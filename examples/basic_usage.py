#!/usr/bin/env python3
"""
Basic usage examples for Lab Bio Scraper
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lab_bio_scraper import NCBIConnector, GEOConnector, UniProtConnector
from lab_bio_scraper.layer2.normalizer import MetadataNormalizer
from lab_bio_scraper.layer3.enhancer import MetadataEnhancer

def example_1_basic_search():
    """Example 1: Basic search and record retrieval"""
    print("=== Example 1: Basic NCBI SRA Search ===")
    
    # Initialize connector
    connector = NCBIConnector()
    
    # Search for RNA-seq data
    print("Searching for 'RNA-seq human' in NCBI SRA...")
    records = list(connector.search("RNA-seq human", limit=3))
    
    print(f"Found {len(records)} records")
    
    # Display first record
    if records:
        record = records[0]
        print(f"\nFirst record:")
        print(f"  Accession: {record.accession}")
        print(f"  Title: {record.title}")
        print(f"  Source: {record.source.value}")
        print(f"  Data Type: {record.data_type.value}")
        print(f"  Download URLs: {len(record.download_urls)} files")

def example_2_normalization():
    """Example 2: Metadata normalization"""
    print("\n=== Example 2: Metadata Normalization ===")
    
    # Get a specific record
    connector = GEOConnector()
    record = connector.get_record("GSE1234")  # Example GEO accession
    
    if record:
        print(f"Original record: {record.accession}")
        print(f"Raw metadata keys: {list(record.metadata.keys())[:5]}...")
        
        # Normalize metadata
        normalizer = MetadataNormalizer()
        normalized = normalizer.normalize_record(record)
        
        print(f"\nNormalized metadata:")
        print(f"  Accession: {normalized.accession}")
        print(f"  Data Type: {normalized.data_type}")
        print(f"  Organisms: {len(normalized.organisms)}")
        print(f"  Samples: {len(normalized.samples)}")
        print(f"  Files: {len(normalized.files)}")
        
        if normalized.organisms:
            print(f"  Primary organism: {normalized.organisms[0].scientific_name}")

def example_3_enhancement():
    """Example 3: Quality enhancement and provenance"""
    print("\n=== Example 3: Quality Enhancement ===")
    
    # Get and normalize a record
    connector = UniProtConnector()
    record = connector.get_record("P12345")  # Example UniProt accession
    
    if record:
        # Normalize first
        normalizer = MetadataNormalizer()
        normalized = normalizer.normalize_record(record)
        
        # Enhance with quality scores and provenance
        enhancer = MetadataEnhancer()
        enhanced = enhancer.enhance(normalized)
        
        print(f"Enhanced record: {enhanced.accession}")
        print(f"Quality metrics:")
        if enhanced.quality_metrics:
            qm = enhanced.quality_metrics
            print(f"  Overall quality: {qm.overall_quality}")
            print(f"  Completeness: {qm.completeness_score:.2f}")
            print(f"  Consistency: {qm.consistency_score:.2f}")
            print(f"  Accuracy: {qm.accuracy_score:.2f}")
        
        print(f"\nProvenance:")
        provenance = enhanced.provenance
        print(f"  Source repository: {provenance.get('source_repository')}")
        print(f"  Retrieved at: {provenance.get('retrieved_at')}")
        print(f"  Processing steps: {len(provenance.get('processing_chain', []))}")

def example_4_batch_processing():
    """Example 4: Batch processing multiple records"""
    print("\n=== Example 4: Batch Processing ===")
    
    # List of accessions from different sources
    accessions = [
        ("ncbi", "SRR123456"),
        ("uniprot", "P12345"),
        ("pdb", "1ABC")
    ]
    
    normalizer = MetadataNormalizer()
    enhancer = MetadataEnhancer()
    
    enhanced_records = []
    
    for source, accession in accessions:
        print(f"Processing {accession} from {source}...")
        
        # Get connector
        if source == "ncbi":
            connector = NCBIConnector()
        elif source == "uniprot":
            connector = UniProtConnector()
        elif source == "pdb":
            from lab_bio_scraper.layer1.connectors import PDBConnector
            connector = PDBConnector()
        else:
            continue
        
        # Get record
        record = connector.get_record(accession)
        
        if record:
            # Normalize and enhance
            normalized = normalizer.normalize_record(record)
            enhanced = enhancer.enhance(normalized)
            enhanced_records.append(enhanced)
            print(f"  ✓ Enhanced {accession}")
        else:
            print(f"  ✗ Record {accession} not found")
    
    # Create summary
    if enhanced_records:
        summary = enhancer.generate_summary_stats(enhanced_records)
        print(f"\nBatch Summary:")
        print(f"  Total records: {summary['total_records']}")
        print(f"  Sources: {list(summary['sources'].keys())}")
        print(f"  Data types: {list(summary['data_types'].keys())}")
        print(f"  Quality distribution: {summary['quality_distribution']}")

def example_5_ml_features():
    """Example 5: ML-ready feature extraction"""
    print("\n=== Example 5: ML Feature Extraction ===")
    
    # Get a record and enhance it
    connector = NCBIConnector()
    record = connector.get_record("SRR123456")
    
    if record:
        # Process through the full pipeline
        normalizer = MetadataNormalizer()
        enhancer = MetadataEnhancer()
        
        normalized = normalizer.normalize_record(record)
        enhanced = enhancer.enhance(normalized)
        
        # Extract ML features
        ml_formatter = enhancer.ml_formatter
        features = ml_formatter.extract_features(enhanced)
        
        print("ML-ready features:")
        print(f"  Categorical features: {len(features['categorical_features'])}")
        print(f"  Numerical features: {len(features['numerical_features'])}")
        print(f"  Text features: {len(features['text_features'])}")
        print(f"  List features: {len(features['list_features'])}")
        
        # Show some example features
        cat_features = features['categorical_features']
        if 'primary_organism' in cat_features:
            print(f"  Primary organism: {cat_features['primary_organism']}")
        
        num_features = features['numerical_features']
        if 'num_files' in num_features:
            print(f"  Number of files: {num_features['num_files']}")

def example_6_data_export():
    """Example 6: Data package creation"""
    print("\n=== Example 6: Data Package Export ===")
    
    # Process a few records
    connector = NCBIConnector()
    records = list(connector.search("human genome", limit=2))
    
    if records:
        normalizer = MetadataNormalizer()
        enhancer = MetadataEnhancer()
        
        # Process records
        enhanced_records = []
        for record in records:
            normalized = normalizer.normalize_record(record)
            enhanced = enhancer.enhance(normalized)
            enhanced_records.append(enhanced)
        
        # Create data package
        output_dir = "example_output"
        package_files = enhancer.create_data_package(enhanced_records, output_dir)
        
        print(f"Data package created in '{output_dir}':")
        for name, path in package_files.items():
            print(f"  {name}: {path}")
        
        # Show summary
        summary = enhancer.generate_summary_stats(enhanced_records)
        print(f"\nPackage summary:")
        print(f"  Records: {summary['total_records']}")
        print(f"  Avg completeness: {summary['avg_completeness']:.2f}")

if __name__ == "__main__":
    print("Lab Bio Scraper - Usage Examples")
    print("=" * 50)
    
    try:
        example_1_basic_search()
        example_2_normalization()
        example_3_enhancement()
        example_4_batch_processing()
        example_5_ml_features()
        example_6_data_export()
        
        print("\n" + "=" * 50)
        print("All examples completed successfully!")
        print("\nNote: Some examples use placeholder accessions that may not exist.")
        print("Replace with real accessions for actual data retrieval.")
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        print("This is normal if using placeholder accessions or without internet access.")
