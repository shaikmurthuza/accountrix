import os
import asyncio
from typing import Any, Dict, Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

load_dotenv()

app = FastAPI(title="KVK Company Lookup API", version="1.0.1")


def kvk_base_urls(env: str) -> Dict[str, str]:
    """
    TEST environment:
      - https://api.kvk.nl/test/api/v2/zoeken
      - https://api.kvk.nl/test/api/v1/basisprofielen

    PROD environment:
      - https://api.kvk.nl/api/v2/zoeken
      - https://api.kvk.nl/api/v1/basisprofielen
    """
    env = (env or "prod").lower().strip()

    if env == "test":
        root = "https://api.kvk.nl/test/api"
    else:
        root = "https://api.kvk.nl/api"

    return {
        "zoeken": f"{root}/v2/zoeken",
        "basisprofielen": f"{root}/v1/basisprofielen",
    }


def get_headers() -> Dict[str, str]:
    api_key = (os.getenv("KVK_API_KEY") or "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="Missing KVK_API_KEY in environment/.env")
    return {"apikey": api_key}


async def kvk_get(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=get_headers(), params=params or {})

        if r.status_code >= 400:
            # try to decode error body
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
    Basisprofiel main:
      GET {basis_url}/{kvk_number}?geoData=true|false

    Subresources:
      GET {basis_url}/{kvk_number}/eigenaar
      GET {basis_url}/{kvk_number}/hoofdvestiging
      GET {basis_url}/{kvk_number}/vestigingen
    """
    base_params = {"geoData": str(geo_data).lower()}
    main_profile = await kvk_get(f"{basis_url}/{kvk_number}", params=base_params)

    if not include_subresources:
        return {"basisprofiel": main_profile}

    async with httpx.AsyncClient(timeout=20) as client:
        headers = get_headers()

        async def get_json(path: str) -> Dict[str, Any]:
            r = await client.get(path, headers=headers, params=base_params)
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
            get_json(eigenaar_url),
            get_json(hoofdvestiging_url),
            get_json(vestigingen_url),
        )

    return {
        "basisprofiel": main_profile,
        "eigenaar": eigenaar,
        "hoofdvestiging": hoofdvestiging,
        "vestigingen": vestigingen,
    }


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
    if not kvk_number and not name:
        raise HTTPException(status_code=400, detail="Provide either kvk_number or name")

    urls = kvk_base_urls(os.getenv("KVK_ENV", "prod"))

    # 1) Direct KVK lookup
    if kvk_number:
        details = await fetch_basisprofiel(
            basis_url=urls["basisprofielen"],
            kvk_number=kvk_number,
            geo_data=geo_data,
            include_subresources=include_subresources,
        )
        return {"input": {"kvk_number": kvk_number}, "details": details}

    # 2) Search by name -> pick first -> fetch basisprofiel
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
