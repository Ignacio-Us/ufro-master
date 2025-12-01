import time
from typing import Optional, Dict, Any
import httpx
from db.mongo import get_db


async def call_verifier(
    service_name: str,
    endpoint: str,
    image_b64: str,
    timeout_s: int = 10,
    request_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Call a PP2 verification service asynchronously.

    Args:
        service_name: Name of the service (for logging)
        endpoint: Full URL endpoint
        image_b64: Base64-encoded image string
        timeout_s: Request timeout in seconds
        request_id: Optional request ID for tracing

    Returns:
        Dictionary with service response, latency, status, and error information
    """
    url = endpoint
    payload = {"image_b64": image_b64}
    start_time = time.time()
    
    # Prepare timeout configuration
    timeout = httpx.Timeout(timeout_s, connect=5.0)  # 5s connect, timeout_s total
    
    result = {
        "service_name": service_name,
        "endpoint": url,
        "latency_ms": 0.0,
        "status_code": None,
        "result": None,
        "timeout": False,
        "error": None,
    }
    
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, json=payload)
            latency_ms = (time.time() - start_time) * 1000
            
            # Try to parse JSON response
            try:
                data = response.json()
            except Exception:
                data = None
            
            result.update({
                "latency_ms": latency_ms,
                "status_code": response.status_code,
                "result": data,
            })
            
            # Log to service_logs
            _log_service_call(request_id, service_name, url, result)
            
            return result
            
    except httpx.TimeoutException:
        latency_ms = (time.time() - start_time) * 1000
        result.update({
            "latency_ms": latency_ms,
            "timeout": True,
            "error": "timeout",
        })
        _log_service_call(request_id, service_name, url, result)
        return result
        
    except httpx.ConnectError as e:
        latency_ms = (time.time() - start_time) * 1000
        result.update({
            "latency_ms": latency_ms,
            "error": f"connection_error: {str(e)}",
        })
        _log_service_call(request_id, service_name, url, result)
        return result
        
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        result.update({
            "latency_ms": latency_ms,
            "error": str(e),
        })
        _log_service_call(request_id, service_name, url, result)
        return result


def _log_service_call(
    request_id: Optional[str],
    service_name: str,
    endpoint: str,
    result: Dict[str, Any]
) -> None:
    """
    Log service call to MongoDB service_logs collection.
    
    Args:
        request_id: Request ID for tracing
        service_name: Name of the service
        endpoint: Endpoint URL
        result: Result dictionary with latency, status, etc.
    """
    if request_id is None:
        return
        
    try:
        db = get_db()
        log_entry = {
            "request_id": request_id,
            "ts": time.time(),
            "service_type": "pp2",
            "service_name": service_name,
            "endpoint": endpoint,
            "latency_ms": result.get("latency_ms"),
            "status_code": result.get("status_code"),
            "payload_size_bytes": None,  # Could calculate from image_b64 if needed
            "result": result.get("result"),
            "timeout": result.get("timeout", False),
            "error": result.get("error"),
            "users": [],
        }
        db.service_logs.insert_one(log_entry)
    except Exception:
        # Silently fail logging to avoid breaking the main flow
        pass
