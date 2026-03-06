"""
Command-line interface for Lab Bio Scraper
"""

import typer
import json
from typing import Optional, List
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel

from .core.base import DataSource
from .layer1.connectors import (
    NCBIConnector, GEOConnector, ENAConnector,
    UniProtConnector, PDBConnector, AlphaFoldConnector
)
from .layer2.normalizer import MetadataNormalizer
from .layer3.enhancer import MetadataEnhancer

app = typer.Typer(help="Lab Bio Scraper - Comprehensive biological data scraper")
console = Console()

# Connector mapping
CONNECTORS = {
    "ncbi": NCBIConnector,
    "geo": GEOConnector,
    "ena": ENAConnector,
    "uniprot": UniProtConnector,
    "pdb": PDBConnector,
    "alphafold": AlphaFoldConnector,
}

@app.command()
def search(
    source: str = typer.Argument(..., help="Data source (ncbi, geo, ena, uniprot, pdb, alphafold)"),
    query: str = typer.Argument(..., help="Search query"),
    limit: int = typer.Option(10, help="Maximum number of results"),
    output: Optional[str] = typer.Option(None, help="Output file (JSON format)"),
    normalize: bool = typer.Option(False, help="Normalize metadata"),
    enhance: bool = typer.Option(False, help="Enhance with quality scores and provenance"),
):
    """Search biological data repositories"""
    
    if source not in CONNECTORS:
        console.print(f"[red]Error: Unknown source '{source}'[/red]")
        console.print(f"Available sources: {', '.join(CONNECTORS.keys())}")
        raise typer.Exit(1)
    
    # Initialize connector
    connector = CONNECTORS[source]()
    
    console.print(f"🔍 Searching {source.upper()} for: '{query}'")
    
    # Search with progress bar
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(f"Searching {source.upper()}...", total=None)
        
        records = list(connector.search(query, limit))
        
        progress.update(task, completed=True)
    
    if not records:
        console.print("No records found.")
        return
    
    console.print(f"✅ Found {len(records)} records")
    
    # Process records
    processed_records = []
    
    if normalize or enhance:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Processing records...", total=len(records))
            
            normalizer = MetadataNormalizer() if normalize else None
            enhancer = MetadataEnhancer() if enhance else None
            
            for record in records:
                # Normalize
                if normalizer:
                    normalized = normalizer.normalize_record(record)
                else:
                    # Convert to basic dict format
                    normalized = record.to_dict()
                
                # Enhance
                if enhancer and hasattr(normalized, 'dict'):
                    enhanced = enhancer.enhance(normalized)
                    processed_records.append(enhanced.dict())
                else:
                    processed_records.append(normalized)
                
                progress.advance(task)
    else:
        processed_records = [record.to_dict() for record in records]
    
    # Display results
    _display_results(processed_records, source)
    
    # Save to file
    if output:
        output_path = Path(output)
        with open(output_path, 'w') as f:
            json.dump(processed_records, f, indent=2, default=str)
        console.print(f"💾 Results saved to {output_path}")

@app.command()
def get(
    source: str = typer.Argument(..., help="Data source"),
    accession: str = typer.Argument(..., help="Accession ID"),
    output: Optional[str] = typer.Option(None, help="Output file (JSON format)"),
    normalize: bool = typer.Option(True, help="Normalize metadata"),
    enhance: bool = typer.Option(True, help="Enhance with quality scores and provenance"),
):
    """Get a specific record by accession"""
    
    if source not in CONNECTORS:
        console.print(f"[red]Error: Unknown source '{source}'[/red]")
        raise typer.Exit(1)
    
    # Initialize connector
    connector = CONNECTORS[source]()
    
    console.print(f"🔍 Fetching {accession} from {source.upper()}...")
    
    # Get record
    record = connector.get_record(accession)
    
    if not record:
        console.print(f"[red]Record {accession} not found in {source.upper()}[/red]")
        raise typer.Exit(1)
    
    console.print("✅ Record found")
    
    # Process record
    if normalize:
        normalizer = MetadataNormalizer()
        normalized = normalizer.normalize_record(record)
        
        if enhance:
            enhancer = MetadataEnhancer()
            enhanced = enhancer.enhance(normalized)
            processed_record = enhanced.dict()
        else:
            processed_record = normalized.dict()
    else:
        processed_record = record.to_dict()
    
    # Display record
    _display_record_details(processed_record, source)
    
    # Save to file
    if output:
        output_path = Path(output)
        with open(output_path, 'w') as f:
            json.dump(processed_record, f, indent=2, default=str)
        console.print(f"💾 Record saved to {output_path}")

@app.command()
def batch(
    input_file: str = typer.Argument(..., help="Input file with accessions (JSON or CSV)"),
    output_dir: str = typer.Argument(..., help="Output directory"),
    source: Optional[str] = typer.Option(None, help="Override source detection"),
    normalize: bool = typer.Option(True, help="Normalize metadata"),
    enhance: bool = typer.Option(True, help="Enhance with quality scores and provenance"),
):
    """Batch process multiple accessions"""
    
    input_path = Path(input_file)
    output_path = Path(output_dir)
    
    if not input_path.exists():
        console.print(f"[red]Input file {input_path} not found[/red]")
        raise typer.Exit(1)
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Read input file
    console.print(f"📖 Reading accessions from {input_file}")
    
    if input_path.suffix == '.json':
        with open(input_path) as f:
            data = json.load(f)
            if isinstance(data, list):
                accessions = [item.get('accession') if isinstance(item, dict) else str(item) for item in data]
            else:
                accessions = [data.get('accession') if isinstance(data, dict) else str(data)]
    elif input_path.suffix == '.csv':
        import pandas as pd
        df = pd.read_csv(input_path)
        if 'accession' in df.columns:
            accessions = df['accession'].tolist()
        else:
            console.print("[red]CSV file must have 'accession' column[/red]")
            raise typer.Exit(1)
    else:
        console.print("[red]Input file must be JSON or CSV[/red]")
        raise typer.Exit(1)
    
    console.print(f"📊 Processing {len(accessions)} accessions")
    
    # Process batch
    results = []
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Processing accessions...", total=len(accessions))
        
        normalizer = MetadataNormalizer() if normalize else None
        enhancer = MetadataEnhancer() if enhance else None
        
        for accession in accessions:
            # Detect source if not provided
            if source:
                detected_source = source
            else:
                detected_source = _detect_source(accession)
            
            if not detected_source:
                console.print(f"[yellow]Warning: Could not detect source for {accession}, skipping[/yellow]")
                progress.advance(task)
                continue
            
            # Get record
            connector = CONNECTORS[detected_source]()
            record = connector.get_record(accession)
            
            if record:
                # Normalize and enhance
                if normalizer:
                    normalized = normalizer.normalize_record(record)
                    if enhancer:
                        enhanced = enhancer.enhance(normalized)
                        processed_record = enhanced.dict()
                    else:
                        processed_record = normalized.dict()
                else:
                    processed_record = record.to_dict()
                
                results.append(processed_record)
            else:
                console.print(f"[yellow]Warning: Record {accession} not found[/yellow]")
            
            progress.advance(task)
    
    # Create data package
    if enhance and normalizer:
        enhancer_obj = MetadataEnhancer()
        package_files = enhancer_obj.create_data_package(
            [UnifiedMetadata(**result) for result in results],
            str(output_path)
        )
        
        console.print("✅ Batch processing complete")
        console.print("📦 Data package created:")
        for name, path in package_files.items():
            console.print(f"  {name}: {path}")
    else:
        # Save basic results
        output_file = output_path / "batch_results.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        console.print(f"✅ Batch processing complete")
        console.print(f"💾 Results saved to {output_file}")

@app.command()
def sources():
    """List available data sources"""
    
    table = Table(title="Available Data Sources")
    table.add_column("Source", style="cyan")
    table.add_column("Description", style="white")
    table.add_column("Data Types", style="green")
    
    source_info = {
        "ncbi": ("NCBI SRA", "Raw sequencing data", "Genomic sequences"),
        "geo": ("GEO", "Gene expression and epigenomics", "Transcriptomic, epigenomic"),
        "ena": ("ENA", "European nucleotide archive", "Genomic sequences"),
        "uniprot": ("UniProt", "Protein sequences and annotation", "Proteomic"),
        "pdb": ("PDB", "Protein structures", "Structural"),
        "alphafold": ("AlphaFold", "Predicted protein structures", "Structural"),
    }
    
    for source, (name, description, data_types) in source_info.items():
        table.add_row(source, f"{name}: {description}", data_types)
    
    console.print(table)

@app.command()
def validate(
    accession: str = typer.Argument(..., help="Accession to validate"),
    source: Optional[str] = typer.Option(None, help="Specify source (auto-detect if not provided)"),
):
    """Validate accession format"""
    
    if source:
        if source not in CONNECTORS:
            console.print(f"[red]Error: Unknown source '{source}'[/red]")
            raise typer.Exit(1)
        
        connector = CONNECTORS[source]()
        is_valid = connector.validate_accession(accession)
        
        console.print(f"Accession: {accession}")
        console.print(f"Source: {source.upper()}")
        console.print(f"Valid: {'✅ Yes' if is_valid else '❌ No'}")
    else:
        detected_source = _detect_source(accession)
        
        if detected_source:
            connector = CONNECTORS[detected_source]()
            is_valid = connector.validate_accession(accession)
            
            console.print(f"Accession: {accession}")
            console.print(f"Detected source: {detected_source.upper()}")
            console.print(f"Valid: {'✅ Yes' if is_valid else '❌ No'}")
        else:
            console.print(f"[red]Could not detect source for accession: {accession}[/red]")
            console.print("Try specifying the source with --source")

def _display_results(records: List[dict], source: str):
    """Display search results in a table"""
    
    if not records:
        return
    
    table = Table(title=f"Results from {source.upper()}")
    table.add_column("Accession", style="cyan")
    table.add_column("Title", style="white")
    table.add_column("Data Type", style="green")
    
    for record in records[:10]:  # Show first 10
        accession = record.get('accession', 'N/A')
        title = record.get('title', 'N/A')[:50] + '...' if len(record.get('title', '')) > 50 else record.get('title', 'N/A')
        data_type = record.get('data_type', 'N/A')
        
        table.add_row(accession, title, data_type)
    
    console.print(table)
    
    if len(records) > 10:
        console.print(f"... and {len(records) - 10} more records")

def _display_record_details(record: dict, source: str):
    """Display detailed record information"""
    
    console.print(Panel(f"Record Details from {source.upper()}", expand=False))
    
    # Basic info
    console.print(f"[bold]Accession:[/bold] {record.get('accession', 'N/A')}")
    console.print(f"[bold]Title:[/bold] {record.get('title', 'N/A')}")
    console.print(f"[bold]Data Type:[/bold] {record.get('data_type', 'N/A')}")
    
    # Description
    if record.get('description'):
        console.print(f"[bold]Description:[/bold] {record['description']}")
    
    # Organisms
    organisms = record.get('organisms', [])
    if organisms:
        console.print(f"[bold]Organisms:[/bold]")
        for org in organisms[:3]:  # Show first 3
            if isinstance(org, dict):
                console.print(f"  • {org.get('scientific_name', 'N/A')}")
            else:
                console.print(f"  • {org}")
        if len(organisms) > 3:
            console.print(f"  ... and {len(organisms) - 3} more")
    
    # Files
    files = record.get('files', [])
    if files:
        console.print(f"[bold]Files:[/bold] {len(files)} files")
        for file in files[:3]:  # Show first 3
            if isinstance(file, dict):
                filename = file.get('filename', 'N/A')
                filetype = file.get('filetype', 'N/A')
                size = file.get('filesize')
                size_str = f" ({size:,} bytes)" if size else ""
                console.print(f"  • {filename} [{filetype}]{size_str}")
            else:
                console.print(f"  • {file}")
        if len(files) > 3:
            console.print(f"  ... and {len(files) - 3} more")
    
    # Quality metrics (if enhanced)
    quality_metrics = record.get('quality_metrics')
    if quality_metrics:
        console.print(f"[bold]Quality Metrics:[/bold]")
        console.print(f"  • Overall: {quality_metrics.get('overall_quality', 'N/A')}")
        console.print(f"  • Completeness: {quality_metrics.get('completeness_score', 'N/A')}")
        console.print(f"  • Consistency: {quality_metrics.get('consistency_score', 'N/A')}")
        console.print(f"  • Accuracy: {quality_metrics.get('accuracy_score', 'N/A')}")

def _detect_source(accession: str) -> Optional[str]:
    """Auto-detect data source from accession format"""
    
    import re
    
    patterns = {
        "ncbi": r'^(SRR|DRR|ERR)\d{6,}$',
        "geo": r'^(GSE|GSM|GPL)\d{3,}$',
        "ena": r'^(SRR|ERR|DRR)\d{6,}$',  # Same as NCBI SRA
        "uniprot": r'^[A-Z0-9]{6,}$',
        "pdb": r'^[0-9][A-Z0-9]{3}$',
        "alphafold": r'^[A-Z0-9]{6,}$',  # Same as UniProt
    }
    
    for source, pattern in patterns.items():
        if re.match(pattern, accession):
            return source
    
    return None

if __name__ == "__main__":
    app()
