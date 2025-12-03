import time
from typing import List, Dict, Any, Optional
import requests


def normalize_citation(citation: Any) -> Optional[Dict[str, str]]:
    """
    Normalize a citation to the expected format.
    
    Expected format: {"doc": str, "page": str, "url": str} or {"doc": str, "section": str, "url": str}
    
    Args:
        citation: Citation object (dict, list, or other format)
        
    Returns:
        Normalized citation dict or None if invalid
    """
    if not isinstance(citation, dict):
        return None
    
    normalized = {}
    
    # Extract doc (required)
    doc = citation.get("doc") or citation.get("document") or citation.get("title") or citation.get("name")
    if not doc:
        return None
    normalized["doc"] = str(doc)
    
    # Extract page or section (at least one should be present)
    page = citation.get("page") or citation.get("p")
    section = citation.get("section") or citation.get("sec") or citation.get("section_number")
    
    if page:
        normalized["page"] = str(page)
    elif section:
        normalized["section"] = str(section)
    # If neither page nor section, we still include the citation but without these fields
    
    # Extract URL (optional but recommended)
    url = citation.get("url") or citation.get("link") or citation.get("href")
    if url:
        normalized["url"] = str(url)
    else:
        normalized["url"] = ""  # Empty string if not provided
    
    return normalized


def normalize_citations(citations: Any) -> List[Dict[str, str]]:
    """
    Normalize a list of citations to the expected format.
    
    Args:
        citations: List of citations or single citation object
        
    Returns:
        List of normalized citations
    """
    if not citations:
        return []
    
    # Handle single citation
    if isinstance(citations, dict):
        normalized = normalize_citation(citations)
        return [normalized] if normalized else []
    
    # Handle list of citations
    if isinstance(citations, list):
        normalized_list = []
        for citation in citations:
            normalized = normalize_citation(citation)
            if normalized:
                normalized_list.append(normalized)
        return normalized_list
    
    return []


def ask_normativa(service_name: str, endpoint: str, question: str, timeout_s: int = 15):
    """
    Call PP1 normativa RAG service.
    
    Sends to endpoint:
        POST {endpoint}
        Body: {"query": question}
    
    Args:
        service_name: Name of the service (for logging)
        endpoint: Full URL endpoint (e.g., http://54.161.208.25:5813/query)
        question: Question string to ask
        timeout_s: Request timeout in seconds
        
    Returns:
        Dictionary with service response, latency, status, and error information.
        The result field contains the raw response, which should have 'text' and 'citations'.
    """
    # Prepare payload - sends query to endpoint
    payload = {"query": question}
    
    start = time.time()
    try:
        r = requests.post(endpoint, json=payload, timeout=timeout_s)
        latency = (time.time() - start) * 1000
        try:
            data = r.json()
            # Normalize citations if present
            if isinstance(data, dict) and "citations" in data:
                data["citations"] = normalize_citations(data["citations"])
        except Exception:
            data = None
        return {
            "service_name": service_name,
            "endpoint": endpoint,
            "latency_ms": latency,
            "status_code": r.status_code,
            "result": data,
            "timeout": False,
            "error": None,
        }
    except requests.Timeout:
        latency = (time.time() - start) * 1000
        return {
            "service_name": service_name,
            "endpoint": endpoint,
            "latency_ms": latency,
            "status_code": None,
            "result": None,
            "timeout": True,
            "error": "timeout",
        }
    except Exception as e:
        latency = (time.time() - start) * 1000
        return {
            "service_name": service_name,
            "endpoint": endpoint,
            "latency_ms": latency,
            "status_code": None,
            "result": None,
            "timeout": False,
            "error": str(e),
        }
