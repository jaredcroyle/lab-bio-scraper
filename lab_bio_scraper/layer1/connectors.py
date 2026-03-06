"""
Layer 1 connectors for major biological data repositories
"""

import asyncio
import aiohttp
import requests
import time
from typing import Dict, List, Optional, Any, Iterator
from datetime import datetime
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
import re

from ..core.base import BaseConnector, DataRecord, DataSource, DataType
from ..core.schema import UnifiedMetadata, Organism, Sample, Technology, File, Citation

class NCBIConnector(BaseConnector):
    """Connector for NCBI databases (SRA, GEO, etc.)"""
    
    def get_default_base_url(self) -> str:
        return "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    
    def search(self, query: str, limit: int = 100) -> Iterator[DataRecord]:
        """Search NCBI databases"""
        base_params = {
            "db": "sra",
            "term": query,
            "retmax": limit,
            "retmode": "json",
            "tool": "lab-bio-scraper",
            "email": "admin@example.com"
        }
        
        search_url = urljoin(self.base_url, "esearch.fcgi")
        response = requests.get(search_url, params=base_params)
        response.raise_for_status()
        
        search_data = response.json()
        id_list = search_data.get("esearchresult", {}).get("idlist", [])
        
        for uid in id_list:
            record = self.get_record(uid)
            if record:
                yield record
            time.sleep(self.get_rate_limit_delay())
    
    def get_record(self, accession: str) -> Optional[DataRecord]:
        """Get a specific NCBI record by accession"""
        # First get the UID from accession
        uid = self._accession_to_uid(accession)
        if not uid:
            return None
            
        # Get summary
        summary_url = urljoin(self.base_url, "esummary.fcgi")
        params = {
            "db": "sra",
            "id": uid,
            "retmode": "json",
            "tool": "lab-bio-scraper"
        }
        
        response = requests.get(summary_url, params=params)
        response.raise_for_status()
        
        summary_data = response.json()
        record_data = summary_data.get("result", {}).get(uid, {})
        
        # Get detailed metadata
        metadata = self.get_metadata(accession)
        download_urls = self.download_urls(accession)
        
        return DataRecord(
            id=uid,
            source=DataSource.NCBI_SRA,
            data_type=DataType.GENOMIC_SEQUENCE,
            title=record_data.get("title", ""),
            description=record_data.get("description", ""),
            metadata=metadata,
            download_urls=download_urls,
            file_size=None,
            checksum=None,
            accession=accession,
            version=None,
            date_created=self._parse_date(record_data.get("createdate")),
            date_modified=self._parse_date(record_data.get("updatedate")),
            provenance={"source": "ncbi", "uid": uid}
        )
    
    def get_metadata(self, accession: str) -> Dict[str, Any]:
        """Get detailed metadata for NCBI record"""
        fetch_url = urljoin(self.base_url, "efetch.fcgi")
        params = {
            "db": "sra",
            "id": accession,
            "rettype": "full",
            "retmode": "xml",
            "tool": "lab-bio-scraper"
        }
        
        response = requests.get(fetch_url, params=params)
        response.raise_for_status()
        
        # Parse XML and extract metadata
        root = ET.fromstring(response.text)
        metadata = {"xml": response.text}
        
        # Extract key metadata fields
        for sample in root.findall(".//SAMPLE"):
            sample_title = sample.find("TITLE")
            if sample_title is not None:
                metadata["sample_title"] = sample_title.text
        
        return metadata
    
    def download_urls(self, accession: str) -> List[str]:
        """Get download URLs for NCBI SRA data"""
        # Use NCBI's SRA toolkit or direct FTP URLs
        ftp_base = "ftp://ftp.ncbi.nlm.nih.gov/sra/sra-instant/reads/ByRun/sra/"
        
        # Parse accession to get directory structure
        if len(accession) >= 6:
            prefix = accession[:3]
            middle = accession[3:6]
            filename = f"{accession}.sra"
            url = f"{ftp_base}{prefix}/{middle}/{accession}/{filename}"
            return [url]
        
        return []
    
    def _accession_to_uid(self, accession: str) -> Optional[str]:
        """Convert accession to NCBI UID"""
        search_url = urljoin(self.base_url, "esearch.fcgi")
        params = {
            "db": "sra",
            "term": accession,
            "retmode": "json",
            "tool": "lab-bio-scraper"
        }
        
        response = requests.get(search_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        id_list = data.get("esearchresult", {}).get("idlist", [])
        return id_list[0] if id_list else None
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse NCBI date format"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y/%m/%d %H:%M:%S")
        except ValueError:
            return None
    
    def validate_accession(self, accession: str) -> bool:
        """Validate SRA accession format (SRR, DRR, ERR)"""
        pattern = r'^(SRR|DRR|ERR)\d{6,}$'
        return bool(re.match(pattern, accession))

class GEOConnector(BaseConnector):
    """Connector for Gene Expression Omnibus (GEO)"""
    
    def get_default_base_url(self) -> str:
        return "https://www.ncbi.nlm.nih.gov/geo/query/"
    
    def search(self, query: str, limit: int = 100) -> Iterator[DataRecord]:
        """Search GEO database"""
        esearch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        params = {
            "db": "gds",
            "term": query,
            "retmax": limit,
            "retmode": "json",
            "tool": "lab-bio-scraper"
        }
        
        response = requests.get(esearch_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        id_list = data.get("esearchresult", {}).get("idlist", [])
        
        for uid in id_list:
            record = self._get_geo_record(uid)
            if record:
                yield record
            time.sleep(self.get_rate_limit_delay())
    
    def get_record(self, accession: str) -> Optional[DataRecord]:
        """Get a specific GEO record by accession"""
        # Convert GEO accession to internal ID
        uid = self._geo_accession_to_uid(accession)
        if uid:
            return self._get_geo_record(uid)
        return None
    
    def get_metadata(self, accession: str) -> Dict[str, Any]:
        """Get detailed metadata for GEO record"""
        fetch_url = urljoin(self.base_url, "acc.cgi")
        params = {
            "acc": accession,
            "targ": "gsm",
            "view": "full",
            "form": "text"
        }
        
        response = requests.get(fetch_url, params=params)
        response.raise_for_status()
        
        # Parse GEO format
        metadata = {"raw_text": response.text}
        lines = response.text.split('\n')
        
        current_key = None
        for line in lines:
            if line.startswith('^'):
                current_key = line[1:].strip()
                metadata[current_key] = []
            elif current_key and line.strip():
                metadata[current_key].append(line.strip())
        
        return metadata
    
    def download_urls(self, accession: str) -> List[str]:
        """Get download URLs for GEO data"""
        # GEO provides FTP links for series and samples
        ftp_base = "ftp://ftp.ncbi.nlm.nih.gov/geo/series/"
        
        if accession.startswith('GSE'):
            # Series accession
            series_num = accession[3:]
            series_dir = f"GSE{series_num[:-3]}nnn/{accession}"
            return [f"{ftp_base}{series_dir}/"]
        elif accession.startswith('GSM'):
            # Sample accession - need to find parent series
            return []  # Would need additional lookup
        
        return []
    
    def _get_geo_record(self, uid: str) -> Optional[DataRecord]:
        """Get GEO record by internal UID"""
        summary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
        params = {
            "db": "gds",
            "id": uid,
            "retmode": "json",
            "tool": "lab-bio-scraper"
        }
        
        response = requests.get(summary_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        record_data = data.get("result", {}).get(uid, {})
        
        if not record_data:
            return None
        
        # Determine accession from title or other fields
        accession = record_data.get("Accession", uid)
        
        return DataRecord(
            id=uid,
            source=DataSource.GEO,
            data_type=DataType.TRANSCRIPTOMIC,
            title=record_data.get("title", ""),
            description=record_data.get("summary", ""),
            metadata=record_data,
            download_urls=self.download_urls(accession),
            file_size=None,
            checksum=None,
            accession=accession,
            version=None,
            date_created=None,
            date_modified=None,
            provenance={"source": "geo", "uid": uid}
        )
    
    def _geo_accession_to_uid(self, accession: str) -> Optional[str]:
        """Convert GEO accession to internal UID"""
        search_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        params = {
            "db": "gds",
            "term": accession,
            "retmode": "json",
            "tool": "lab-bio-scraper"
        }
        
        response = requests.get(search_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        id_list = data.get("esearchresult", {}).get("idlist", [])
        return id_list[0] if id_list else None
    
    def validate_accession(self, accession: str) -> bool:
        """Validate GEO accession format"""
        pattern = r'^(GSE|GSM|GPL)\d{3,}$'
        return bool(re.match(pattern, accession))

class ENAConnector(BaseConnector):
    """Connector for European Nucleotide Archive (ENA)"""
    
    def get_default_base_url(self) -> str:
        return "https://www.ebi.ac.uk/ena/browser/api/"
    
    def search(self, query: str, limit: int = 100) -> Iterator[DataRecord]:
        """Search ENA database"""
        search_url = f"{self.base_url}search"
        params = {
            "query": query,
            "result": "read_run",
            "limit": limit,
            "format": "json",
            "tool": "lab-bio-scraper"
        }
        
        response = requests.get(search_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        for record_data in data:
            accession = record_data.get("run_accession", "")
            if accession:
                record = self.get_record(accession)
                if record:
                    yield record
            time.sleep(self.get_rate_limit_delay())
    
    def get_record(self, accession: str) -> Optional[DataRecord]:
        """Get a specific ENA record by accession"""
        fetch_url = f"{self.base_url}summary/{accession}"
        params = {"tool": "lab-bio-scraper"}
        
        response = requests.get(fetch_url, params=params)
        response.raise_for_status()
        
        record_data = response.json()
        
        return DataRecord(
            id=accession,
            source=DataSource.ENA,
            data_type=DataType.GENOMIC_SEQUENCE,
            title=record_data.get("study_title", ""),
            description=record_data.get("study_abstract", ""),
            metadata=record_data,
            download_urls=self.download_urls(accession),
            file_size=None,
            checksum=None,
            accession=accession,
            version=record_data.get("version"),
            date_created=self._parse_ena_date(record_data.get("first_created")),
            date_modified=self._parse_ena_date(record_data.get("last_modified")),
            provenance={"source": "ena"}
        )
    
    def get_metadata(self, accession: str) -> Dict[str, Any]:
        """Get detailed metadata for ENA record"""
        fetch_url = f"{self.base_url}xml/{accession}"
        params = {"tool": "lab-bio-scraper"}
        
        response = requests.get(fetch_url, params=params)
        response.raise_for_status()
        
        return {"xml": response.text}
    
    def download_urls(self, accession: str) -> List[str]:
        """Get download URLs for ENA data"""
        # ENA provides FTP and HTTP downloads
        ftp_base = "ftp://ftp.ebi.ac.uk/pub/databases/ena/sra/"
        http_base = "https://www.ebi.ac.uk/ena/browser/api/fasta/"
        
        # Direct download links
        return [
            f"{ftp_base}{accession}",
            f"http://www.ebi.ac.uk/ena/data/view/{accession}&download=fastq"
        ]
    
    def _parse_ena_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse ENA date format"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return None
    
    def validate_accession(self, accession: str) -> bool:
        """Validate ENA accession format"""
        pattern = r'^(SRR|ERR|DRR)\d{6,}$'
        return bool(re.match(pattern, accession))

class UniProtConnector(BaseConnector):
    """Connector for UniProt protein database"""
    
    def get_default_base_url(self) -> str:
        return "https://rest.uniprot.org/"
    
    def search(self, query: str, limit: int = 100) -> Iterator[DataRecord]:
        """Search UniProt database"""
        search_url = f"{self.base_url}uniprotkb/search"
        params = {
            "query": query,
            "limit": limit,
            "format": "json",
            "fields": "accession,id,protein_name,organism_name,length,genes,comment(FUNCTION),comment(SUBCELLULAR_LOCATION),keyword_id",
            "tool": "lab-bio-scraper"
        }
        
        response = requests.get(search_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        for record_data in data.get("results", []):
            accession = record_data.get("primaryAccession", "")
            if accession:
                record = self.get_record(accession)
                if record:
                    yield record
            time.sleep(self.get_rate_limit_delay())
    
    def get_record(self, accession: str) -> Optional[DataRecord]:
        """Get a specific UniProt record by accession"""
        fetch_url = f"{self.base_url}uniprotkb/{accession}.json"
        params = {"tool": "lab-bio-scraper"}
        
        response = requests.get(fetch_url, params=params)
        response.raise_for_status()
        
        record_data = response.json()
        
        return DataRecord(
            id=accession,
            source=DataSource.UNIPROT,
            data_type=DataType.PROTEOMIC,
            title=record_data.get("proteinDescription", {}).get("recommendedName", {}).get("fullName", {}).get("value", ""),
            description=self._extract_protein_description(record_data),
            metadata=record_data,
            download_urls=self.download_urls(accession),
            file_size=None,
            checksum=None,
            accession=accession,
            version=record_data.get("entryVersion"),
            date_created=None,
            date_modified=None,
            provenance={"source": "uniprot"}
        )
    
    def get_metadata(self, accession: str) -> Dict[str, Any]:
        """Get detailed metadata for UniProt record"""
        fetch_url = f"{self.base_url}uniprotkb/{accession}.json"
        params = {"tool": "lab-bio-scraper"}
        
        response = requests.get(fetch_url, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def download_urls(self, accession: str) -> List[str]:
        """Get download URLs for UniProt data"""
        return [
            f"{self.base_url}uniprotkb/{accession}.fasta",
            f"{self.base_url}uniprotkb/{accession}.xml",
            f"{self.base_url}uniprotkb/{accession}.txt"
        ]
    
    def _extract_protein_description(self, record_data: Dict[str, Any]) -> str:
        """Extract protein description from UniProt record"""
        comments = record_data.get("comments", [])
        for comment in comments:
            if comment.get("commentType") == "FUNCTION":
                texts = comment.get("texts", [])
                if texts:
                    return texts[0].get("value", "")
        return ""
    
    def validate_accession(self, accession: str) -> bool:
        """Validate UniProt accession format"""
        pattern = r'^[A-Z0-9]{6,}$'
        return bool(re.match(pattern, accession))

class PDBConnector(BaseConnector):
    """Connector for RCSB Protein Data Bank"""
    
    def get_default_base_url(self) -> str:
        return "https://data.rcsb.org/rest/v1/core/"
    
    def search(self, query: str, limit: int = 100) -> Iterator[DataRecord]:
        """Search PDB database"""
        search_url = "https://search.rcsb.org/rcsbsearch/v2/query"
        
        search_payload = {
            "query": {
                "type": "terminal",
                "service": "text",
                "parameters": {
                    "value": query
                }
            },
            "return_type": "entry",
            "request_options": {
                "return_all_hits": True,
                "paginate": {"start": 0, "rows": limit}
            }
        }
        
        response = requests.post(search_url, json=search_payload)
        response.raise_for_status()
        
        data = response.json()
        
        for result in data.get("result_set", []):
            pdb_id = result.get("identifier", "")
            if pdb_id:
                record = self.get_record(pdb_id)
                if record:
                    yield record
            time.sleep(self.get_rate_limit_delay())
    
    def get_record(self, accession: str) -> Optional[DataRecord]:
        """Get a specific PDB record by PDB ID"""
        fetch_url = f"{self.base_url}entry/{accession}"
        
        response = requests.get(fetch_url)
        response.raise_for_status()
        
        record_data = response.json()
        
        return DataRecord(
            id=accession,
            source=DataSource.PDB,
            data_type=DataType.STRUCTURAL,
            title=record_data.get("struct", {}).get("title", ""),
            description=record_data.get("struct", {}).get("pdbx_descriptor", ""),
            metadata=record_data,
            download_urls=self.download_urls(accession),
            file_size=None,
            checksum=None,
            accession=accession,
            version=None,
            date_created=self._parse_pdb_date(record_data.get("rcsb_entry_info", {}).get("deposit_date")),
            date_modified=self._parse_pdb_date(record_data.get("rcsb_entry_info", {}).get("revision_date")),
            provenance={"source": "pdb"}
        )
    
    def get_metadata(self, accession: str) -> Dict[str, Any]:
        """Get detailed metadata for PDB record"""
        fetch_url = f"{self.base_url}entry/{accession}"
        
        response = requests.get(fetch_url)
        response.raise_for_status()
        
        return response.json()
    
    def download_urls(self, accession: str) -> List[str]:
        """Get download URLs for PDB data"""
        base_download = "https://files.rcsb.org/download/"
        return [
            f"{base_download}{accession}.cif",
            f"{base_download}{accession}.pdb",
            f"{base_download}{accession}.fasta",
            f"{base_download}{accession}.xml"
        ]
    
    def _parse_pdb_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse PDB date format"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return None
    
    def validate_accession(self, accession: str) -> bool:
        """Validate PDB ID format"""
        pattern = r'^[0-9][A-Z0-9]{3}$'
        return bool(re.match(pattern, accession))

class AlphaFoldConnector(BaseConnector):
    """Connector for AlphaFold Protein Structure Database"""
    
    def get_default_base_url(self) -> str:
        return "https://alphafold.ebi.ac.uk/api/prediction/"
    
    def search(self, query: str, limit: int = 100) -> Iterator[DataRecord]:
        """Search AlphaFold database (by UniProt accession)"""
        # AlphaFold API is limited - primarily by UniProt accession
        # For broader search, would need to integrate with UniProt first
        
        if self.validate_accession(query):
            record = self.get_record(query)
            if record:
                yield record
    
    def get_record(self, accession: str) -> Optional[DataRecord]:
        """Get a specific AlphaFold prediction by UniProt accession"""
        fetch_url = f"{self.base_url}{accession}"
        
        response = requests.get(fetch_url)
        
        if response.status_code == 404:
            return None
        
        response.raise_for_status()
        
        record_data = response.json()
        
        return DataRecord(
            id=accession,
            source=DataSource.ALPHAFOLD,
            data_type=DataType.STRUCTURAL,
            title=f"AlphaFold prediction for {accession}",
            description=f"Predicted protein structure for UniProt accession {accession}",
            metadata=record_data,
            download_urls=self.download_urls(accession),
            file_size=None,
            checksum=None,
            accession=accession,
            version=None,
            date_created=None,
            date_modified=None,
            provenance={"source": "alphafold"}
        )
    
    def get_metadata(self, accession: str) -> Dict[str, Any]:
        """Get detailed metadata for AlphaFold prediction"""
        fetch_url = f"{self.base_url}{accession}"
        
        response = requests.get(fetch_url)
        response.raise_for_status()
        
        return response.json()
    
    def download_urls(self, accession: str) -> List[str]:
        """Get download URLs for AlphaFold data"""
        base_url = "https://alphafold.ebi.ac.uk/files/AF-"
        return [
            f"{base_url}{accession}-F1-model_v4.pdb",
            f"{base_url}{accession}-F1-predicted_alignment_error_v4.json",
            f"{base_url}{accession}-F1-confidence_v4.json",
            f"{base_url}{accession}-F1-metadata_v4.json"
        ]
    
    def validate_accession(self, accession: str) -> bool:
        """Validate UniProt accession format (AlphaFold uses UniProt IDs)"""
        pattern = r'^[A-Z0-9]{6,}$'
        return bool(re.match(pattern, accession))
