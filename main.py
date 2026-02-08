import os
import asyncio
from typing import Any, Dict, Optional, List

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

load_dotenv()

app = FastAPI(title="KVK Company Lookup API", version="1.0.2")


def kvk_base_urls(env: str) -> Dict[str, str]:
    """
    TEST environment:
      - https://api.kvk.nl/test/api/v2/zoeken
      - https://api.kvk.nl/test/api/v1/basisprofielen
      - https://api.kvk.nl/test/api/v1/vestigingsprofielen
      - https://api.kvk.nl/test/api/v1/naamgevingen

    PROD environment:
      - https://api.kvk.nl/api/v2/zoeken
      - https://api.kvk.nl/api/v1/basisprofielen
      - https://api.kvk.nl/api/v1/vestigingsprofielen
      - https://api.kvk.nl/api/v1/naamgevingen
    """
    env = (env or "prod").lower().strip()
    root = "https://api.kvk.nl/test/api" if env == "test" else "https://api.kvk.nl/api"

    return {
        "zoeken": f"{root}/v2/zoeken",
        "basisprofielen": f"{root}/v1/basisprofielen",
        "vestigingsprofielen": f"{root}/v1/vestigingsprofielen",
        "naamgevingen": f"{root}/v1/naamgevingen",
    }


def get_headers() -> Dict[str, str]:
    api_key = (os.getenv("KVK_API_KEY") or "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="Missing KVK_API_KEY in environment/.env")
    return {"apikey": api_key}


async def kvk_get(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Generic GET wrapper that forwards KVK error details.
    """
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=get_headers(), params=params or {})

        if r.status_code >= 400:
            try:
                body = r.json()
            except Exception:
                body = {"raw": r.text}

            raise HTTPException(
                status_code=r.status_code,
                detail={
                    "kvk_status": r.status_code,
                    "kvk_url": str(r.request.url),
                    "kvk_error": body,
                },
            )

        return r.json()

    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="KVK upstream timeout")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"KVK upstream error: {str(e)}")


async def search_kvk_number_by_name(
    zoeken_url: str,
    name: str,
    place: Optional[str] = None,
    street: Optional[str] = None,
    page: int = 1,
    per_page: int = 10,
    include_inactive: bool = False,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "naam": name,
        "pagina": page,
        "resultatenPerPagina": per_page,
        "inclusiefInactieveRegistraties": str(include_inactive).lower(),
    }

    if place:
        params["plaats"] = place
    if street:
        params["straatnaam"] = street

    return await kvk_get(zoeken_url, params=params)


async def fetch_basisprofiel(
    basis_url: str,
    kvk_number: str,
    geo_data: bool = False,
    include_subresources: bool = True,
) -> Dict[str, Any]:
    """
    Fetches:
      - basisprofiel
      - optional subresources: eigenaar, hoofdvestiging, vestigingen

    Note: Some test records don't provide all subresources -> return None for 404.
    """
    base_params = {"geoData": str(geo_data).lower()}
    main_profile = await kvk_get(f"{basis_url}/{kvk_number}", params=base_params)

    if not include_subresources:
        return {"basisprofiel": main_profile}

    async with httpx.AsyncClient(timeout=20) as client:
        headers = get_headers()

        async def get_json_optional(path: str) -> Optional[Dict[str, Any]]:
            r = await client.get(path, headers=headers, params=base_params)

         
            if r.status_code == 404:
                return None

            if r.status_code >= 400:
                try:
                    return {"_error": r.json(), "_status": r.status_code, "_url": str(r.request.url)}
                except Exception:
                    return {"_error": r.text, "_status": r.status_code, "_url": str(r.request.url)}

            return r.json()

        eigenaar_url = f"{basis_url}/{kvk_number}/eigenaar"
        hoofdvestiging_url = f"{basis_url}/{kvk_number}/hoofdvestiging"
        vestigingen_url = f"{basis_url}/{kvk_number}/vestigingen"

        eigenaar, hoofdvestiging, vestigingen = await asyncio.gather(
            get_json_optional(eigenaar_url),
            get_json_optional(hoofdvestiging_url),
            get_json_optional(vestigingen_url),
        )


    return {
        "basisprofiel": main_profile,
        "hoofdvestiging": hoofdvestiging,
        "vestigingen": vestigingen,
    }


async def fetch_naamgeving(naamgevingen_base_url: str, kvk_number: str) -> Optional[Dict[str, Any]]:
    """
    Naamgeving API:
      GET {naamgevingen_base_url}/kvknummer/{kvk_number}
    """
    try:
        return await kvk_get(f"{naamgevingen_base_url}/kvknummer/{kvk_number}")
    except HTTPException as e:
        if e.status_code == 404:
            return None
        raise


async def fetch_vestigingsprofiel(vestigingsprofielen_url: str, vestigingsnummer: str) -> Optional[Dict[str, Any]]:
    """
    Vestigingsprofiel API:
      GET {vestigingsprofielen_url}/{vestigingsnummer}
    """
    try:
        return await kvk_get(f"{vestigingsprofielen_url}/{vestigingsnummer}")
    except HTTPException as e:
        if e.status_code == 404:
            return None
        raise


def extract_vestigingsnummers(vestigingen_payload: Dict[str, Any]) -> List[str]:
    """
    Attempts to extract vestigingsnummers from the 'vestigingen' payload.
    Different test/prod records may structure it differently.
    Returns a de-duplicated list.
    """
    found: List[str] = []

    items = vestigingen_payload.get("vestigingen")
    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict):
                vn = it.get("vestigingsnummer") or it.get("vestigingsNummer")
                if vn:
                    found.append(str(vn))


    embedded = vestigingen_payload.get("_embedded") or {}
    if isinstance(embedded, dict):
        for _, val in embedded.items():
            if isinstance(val, list):
                for it in val:
                    if isinstance(it, dict):
                        vn = it.get("vestigingsnummer") or it.get("vestigingsNummer")
                        if vn:
                            found.append(str(vn))


    return sorted(set(found))


@app.get("/debug/kvk")
def debug_kvk():
    """Quick check to confirm env + URL base + key presence (does NOT reveal the key)."""
    key = (os.getenv("KVK_API_KEY") or "").strip()
    env = os.getenv("KVK_ENV", "prod")
    urls = kvk_base_urls(env)
    return {
        "KVK_ENV": env,
        "KVK_API_KEY_present": bool(key),
        "KVK_API_KEY_len": len(key),
        "urls": urls,
    }


@app.get("/company")
async def get_company(
    kvk_number: Optional[str] = Query(default=None, description="KVK number (8 digits for test data)"),
    name: Optional[str] = Query(default=None, description="Company name"),
    place: Optional[str] = Query(default=None, description="Optional place filter"),
    street: Optional[str] = Query(default=None, description="Optional street filter"),
    include_inactive: bool = Query(default=False),
    geo_data: bool = Query(default=False),
    include_subresources: bool = Query(default=True),
):
    """
    Usage:
      - /company?kvk_number=69599068
      - /company?name=test&place=Veendam
    """
    if not kvk_number and not name:
        raise HTTPException(status_code=400, detail="Provide either kvk_number or name")

    urls = kvk_base_urls(os.getenv("KVK_ENV", "prod"))


    if kvk_number:
        details = await fetch_basisprofiel(
            basis_url=urls["basisprofielen"],
            kvk_number=kvk_number,
            geo_data=geo_data,
            include_subresources=include_subresources,
        )
        return {"input": {"kvk_number": kvk_number}, "details": details}


    zoek_res = await search_kvk_number_by_name(
        zoeken_url=urls["zoeken"],
        name=name,
        place=place,
        street=street,
        page=1,
        per_page=10,
        include_inactive=include_inactive,
    )

    resultaten = zoek_res.get("resultaten") or []
    if not resultaten:
        raise HTTPException(status_code=404, detail="No companies found for that name/filter")

    best = resultaten[0]
    found_kvk = best.get("kvkNummer")
    if not found_kvk:
        raise HTTPException(status_code=502, detail={"message": "Result missing kvkNummer", "best": best})

    details = await fetch_basisprofiel(
        basis_url=urls["basisprofielen"],
        kvk_number=found_kvk,
        geo_data=geo_data,
        include_subresources=include_subresources,
    )

    return {
        "input": {"name": name, "place": place, "street": street},
        "search": {"raw": zoek_res, "selected_result": best},
        "details": details,
    }


@app.get("/company/full")
async def get_company_full(
    kvk_number: str = Query(..., description="KVK number"),
    geo_data: bool = Query(default=False),
    include_subresources: bool = Query(default=True),
    include_naamgeving: bool = Query(default=True),
    include_vestigingsprofielen: bool = Query(default=True),
):
    """
    Full pipeline:
      - basisprofiel (+ optional subresources)
      - naamgeving (optional)
      - vestigingsprofielen (optional, only if vestigingsnummers found)
    """
    urls = kvk_base_urls(os.getenv("KVK_ENV", "prod"))

    details = await fetch_basisprofiel(
        basis_url=urls["basisprofielen"],
        kvk_number=kvk_number,
        geo_data=geo_data,
        include_subresources=include_subresources,
    )

    response: Dict[str, Any] = {
        "input": {"kvk_number": kvk_number},
        "details": details,
    }

    if include_naamgeving:
        response["naamgeving"] = await fetch_naamgeving(urls["naamgevingen"], kvk_number)

    if include_vestigingsprofielen:
        vestigingen_payload = details.get("vestigingen") or {}
        vestigingsnummers = extract_vestigingsnummers(vestigingen_payload)

        response["vestigingsnummers"] = vestigingsnummers

        if vestigingsnummers:
            profiles = await asyncio.gather(
                *[fetch_vestigingsprofiel(urls["vestigingsprofielen"], vn) for vn in vestigingsnummers]
            )
            response["vestigingsprofielen"] = profiles
        else:
            response["vestigingsprofielen"] = []

    return response


@app.get("/vestiging/{vestigingsnummer}")
async def get_vestiging(vestigingsnummer: str):
    urls = kvk_base_urls(os.getenv("KVK_ENV", "prod"))
    return await fetch_vestigingsprofiel(urls["vestigingsprofielen"], vestigingsnummer)
