import os
import time
import base64
import uuid
import yaml
import asyncio
from typing import List
from .pp2_client import call_verifier
from .pp1_client import ask_normativa
from db.mongo import get_db

REGISTRY_PATH = os.path.join(os.path.dirname(__file__), "..", "conf", "registry.yaml")

def load_registry():
    try:
        with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        data = {}
    return data


async def _identify_person_async(image_b64: str, timeout_s: int, delta: float, req_id: str):
    """Internal async function to identify person using PP2 services."""
    registry = load_registry()
    pp2_list = registry.get("pp2", [])

    # Build list of async tasks for all active PP2 services
    tasks = []
    for v in pp2_list:
        if not v.get("active", True):
            continue
        service_name = v.get("name")
        endpoint = v.get("endpoint_verify")
        if not endpoint:
            continue
        # Create async task with request_id for tracing
        task = call_verifier(service_name, endpoint, image_b64, timeout_s, request_id=req_id)
        tasks.append(task)

    # Execute all PP2 calls in parallel
    if not tasks:
        return []
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Handle exceptions from gather
    processed_results = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            # Find service name for error reporting
            service_name = "unknown"
            active_services = [v for v in pp2_list if v.get("active", True)]
            if i < len(active_services):
                service_name = active_services[i].get("name", "unknown")
            processed_results.append({
                "service_name": service_name,
                "endpoint": active_services[i].get("endpoint_verify", "") if i < len(active_services) else "",
                "error": str(r),
                "timeout": False,
                "result": None,
                "status_code": None,
                "latency_ms": 0.0,
            })
        else:
            processed_results.append(r)
    
    return processed_results


def identify_person(image_b64: str, timeout_s: int = 15, delta: float = 0.05):
    """
    Identify a person using PP2 verification services.
    
    Decision rules using threshold (τ) and delta (δ):
    - τ (threshold): Minimum score required from registry configuration
    - δ (delta): Maximum score difference to detect ambiguity (default 0.05)
    
    Decision logic:
    1. "identified": is_me == True AND score >= τ AND (no second candidate OR difference > δ)
    2. "ambiguous": is_me == True AND score >= τ AND second candidate within δ
    3. "unknown": is_me == False OR score < τ
    
    This is a synchronous wrapper that runs the async identification logic.
    """
    req_id = uuid.uuid4().hex
    ts0 = time.time()
    
    # Run async function from sync context
    # Flask/gunicorn runs in sync context, so asyncio.run() is safe
    results = asyncio.run(_identify_person_async(image_b64, timeout_s, delta, req_id))

    # Extract candidates: expect each r['result'] to contain {'is_me': bool, 'score': float, 'threshold': float, 'timing_ms': float}
    registry = load_registry()
    pp2_list = registry.get("pp2", [])
    candidates = []
    for r in results:
        res = r.get("result") if isinstance(r, dict) else None
        if isinstance(res, dict) and "score" in res:
            try:
                is_me = bool(res.get("is_me", False))
                score = float(res.get("score", 0))
                threshold = float(res.get("threshold", 0.5))
                timing_ms = float(res.get("timing_ms", 0))
            except (ValueError, TypeError):
                is_me = False
                score = 0.0
                threshold = 0.5
                timing_ms = 0.0
            
            candidates.append({
                "is_me": is_me,
                "score": score,
                "threshold": threshold,
                "timing_ms": timing_ms,
                "service": r.get("service_name"),
            })

    # Sort candidates by score (highest first)
    candidates.sort(key=lambda x: x["score"], reverse=True)
    decision = "unknown"
    identity = None
    
    if candidates:
        top = candidates[0]
        # Use threshold from registry (τ) - this is the authoritative threshold for decision
        # The threshold in the response is informational only
        service_threshold = None
        for v in pp2_list:
            if v.get("name") == top.get("service"):
                try:
                    service_threshold = float(v.get("threshold", 0.9))
                except Exception:
                    service_threshold = 0.9
                break
        if service_threshold is None:
            service_threshold = 0.9

        # Decision logic with threshold (τ) and delta (δ):
        # Rule 1: If is_me == True and score >= threshold (τ)
        if top.get("is_me", False) and top["score"] >= service_threshold:
            # Check ambiguity using delta (δ): if second candidate is within δ of top
            if len(candidates) > 1 and (top["score"] - candidates[1]["score"]) <= delta:
                # Ambiguous: top meets threshold but second is too close (within δ)
                decision = "ambiguous"
            else:
                # Identified: top meets threshold and no ambiguity (difference > δ)
                decision = "identified"
                identity = {
                    "score": top["score"],
                    "threshold": service_threshold,  # Use registry threshold
                    "is_me": top["is_me"],
                }
        # Rule 2: If score >= threshold but is_me == False → unknown (service says it's NOT the person)
        elif top["score"] >= service_threshold:
            # Score meets threshold but is_me is False - service explicitly says it's not the person
            decision = "unknown"
        # Rule 3: If score < threshold → unknown
        else:
            decision = "unknown"

    timing_ms = (time.time() - ts0) * 1000

    # Persist access_log (service_logs are already logged by pp2_client)
    db = get_db()
    try:
        db.access_logs.insert_one({
            "request_id": req_id,
            "ts": time.time(),
            "route": "/identify-and-answer",
            "user": {},
            "input": {"has_image": True, "has_question": False, "image_hash": None, "size_bytes": None},
            "decision": decision,
            "identity": identity,
            "pp2_summary": {"total_consulted": len(results), "timeouts": sum(1 for r in results if r.get("timeout"))},
            "pp1_used": False,
            "timing_ms": timing_ms,
            "status_code": 200,
            "ip": None,
        })
    except Exception:
        pass

    return {
        "decision": decision,
        "identity": identity,
        "candidates": candidates,
        "timing_ms": timing_ms,
        "request_id": req_id,
    }


def ask_normativa_tool(question: str, timeout_s: int = 15):
    req_id = uuid.uuid4().hex
    ts0 = time.time()
    registry = load_registry()
    pp1 = registry.get("pp1", [])
    # pick first active
    if not pp1:
        return {"text": "No PP1 configured", "citations": []}
    svc = pp1[0]
    resp = ask_normativa(svc.get("name"), svc.get("endpoint"), question, timeout_s)
    # log service call
    db = get_db()
    try:
        db.service_logs.insert_one({
            "request_id": req_id,
            "ts": time.time(),
            "service_type": "pp1",
            "service_name": svc.get("name"),
            "endpoint": svc.get("endpoint"),
            "latency_ms": resp.get("latency_ms"),
            "status_code": resp.get("status_code"),
            "payload_size_bytes": None,
            "result": resp.get("result"),
            "timeout": resp.get("timeout", False),
            "error": resp.get("error"),
            "users": [],
        })
    except Exception:
        pass

    # return result shaped per spec
    data = resp.get("result") or {}
    return {"text": data.get("text") if isinstance(data, dict) else None, "citations": data.get("citations") if isinstance(data, dict) else []}
