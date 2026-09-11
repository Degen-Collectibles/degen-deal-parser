"""Photo-driven sealed listings. Deliberately separate from legacy role ranks."""
from __future__ import annotations

import asyncio
import json
import os
from difflib import SequenceMatcher

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Request, UploadFile
from fastapi.responses import Response
from starlette.concurrency import run_in_threadpool
from sqlmodel import Session, select

from ..csrf import CSRFProtectedRoute, issue_token
from ..db import get_session
from ..models import AppSetting, TikTokProduct
from ..shared import get_request_user, templates
from ..tiktok import listing_assistant as service
from ..tiktok import listing_defaults as defaults

router = APIRouter(prefix="/tiktok/products/assistant", route_class=CSRFProtectedRoute)


class TikTokRejected(ValueError):
    """A definite API rejection, as opposed to an uncertain transport result."""


class ListingValidationFailed(ValueError):
    pass


def validate_listing(ctx, payload):
    result = shop_call(ctx, '/product/202309/products/listing_check', payload)
    if result.get('check_result') != 'PASS':
        reasons = [str(r.get('message') or r.get('code')) for r in result.get('fail_reasons', [])]
        raise ListingValidationFailed('TikTok listing validation did not pass: ' + ('; '.join(reasons)[:1200] or 'No passing result returned.'))
    return result


def authorized(request: Request):
    if os.environ.get("TIKTOK_LISTING_ASSISTANT_ENABLED", "true").lower() in {"0", "false", "off"}:
        raise HTTPException(404, "Listing assistant is disabled.")
    user = getattr(request.state, "current_user", None) or get_request_user(request)
    if not user or not user.is_active or user.role not in {"admin", "manager"}:
        raise HTTPException(403, "Only admins and managers can prepare or submit listings.")
    request.state.current_user = user
    return user


def stock_transfers_enabled():
    return os.environ.get("TIKTOK_LISTING_STOCK_TRANSFERS_ENABLED", "false").lower() in {"1", "true", "on"}


def require_stock_transfers():
    if not stock_transfers_enabled():
        raise HTTPException(403, "Stock transfers are not enabled. Manage stock in TikTok Seller Center.")


def fail(exc: Exception):
    from ..tiktok.listing_images import ImageGenerationError
    if isinstance(exc, ImageGenerationError):
        raise HTTPException(422, {"code": exc.code, "message": str(exc)}) from exc
    if isinstance(exc, service.DraftConflict):
        raise HTTPException(409, str(exc)) from exc
    if isinstance(exc, LookupError):
        raise HTTPException(404, str(exc)) from exc
    if isinstance(exc, ValueError):
        raise HTTPException(422, str(exc)) from exc
    # External errors may embed credentials/URLs. Log redacted details only.
    from ..discord.ops_log import redact_log_details
    import logging
    logging.getLogger(__name__).warning("Listing assistant failed: %s", redact_log_details({"error": str(exc)}))
    raise HTTPException(502, "The service could not complete this step. Your saved draft is preserved.") from exc


def editable(session, draft_id, version):
    draft, previous = service.load_draft(session, draft_id)
    if draft["status"] != "draft":
        raise service.DraftConflict("This draft has already been submitted or needs its submission status checked.")
    if draft.get("image_job", {}).get("status") == "running":
        raise service.DraftConflict("Image generation is running. Refresh its status before editing or generating again.")
    if version != draft["version"]:
        raise service.DraftConflict("This draft changed. Reload it before editing.")
    return draft, previous


def public(draft):
    result = dict(draft)
    result["assets"] = {key: f"/tiktok/products/assistant/drafts/{draft['id']}/images/{key}?v={name}" for key, name in draft["assets"].items()}
    return result


def context(session):
    from .tiktok_products import _get_tiktok_api_client_context
    ctx = _get_tiktok_api_client_context(session)
    if not ctx.get("access_token"):
        raise ValueError("Connect TikTok before loading shop fields or submitting. Local drafts still work.")
    return ctx


def shop_call(ctx, path, body=None, query=None):
    """One request only: a timed-out product create must never be auto-retried."""
    from scripts.tiktok_backfill import build_tiktok_request, extract_tiktok_data, raise_for_tiktok_error
    extra_query = dict(query or {})
    if path.startswith("/product/202309/categories") or path == "/product/202309/brands":
        extra_query["category_version"] = "v2"
    url, encoded, headers = build_tiktok_request(path=path, body=body, extra_query=extra_query, **ctx)
    with httpx.Client(timeout=60, follow_redirects=False) as client:
        response = client.request("POST" if body is not None else "GET", url,
                                  content=encoded.encode() if body is not None else None, headers=headers)
        response.raise_for_status()
        payload = response.json()
        if body is not None and isinstance(payload, dict) and payload.get("code") not in {None, 0, "0"}:
            from ..discord.ops_log import redact_log_details
            message = str(redact_log_details({"error": str(payload.get("message", "Listing rejected"))})["error"])[:500]
            raise TikTokRejected("TikTok rejected the listing: " + message)
        raise_for_tiktok_error(payload, path=path)
        return extract_tiktok_data(payload)


def duplicates(session, name):
    tokens = {word for word in str(name).lower().split() if len(word) > 2}
    matches = []
    for p in session.exec(select(TikTokProduct)):
        title = p.title.lower()
        score = sum(word in title for word in tokens) / max(1, len(tokens))
        if tokens and (score >= .75 or SequenceMatcher(None, name.lower(), title).ratio() > .8):
            matches.append({"id": p.tiktok_product_id, "title": p.title, "status": p.status})
    return matches[:15]


@router.get("")
def page(request: Request, user=Depends(authorized)):
    return templates.TemplateResponse(request, "tiktok_listing_assistant.html", {
        "current_user": user, "csrf_token": issue_token(request), "title": "Photo to listing",
        "preview_mode": getattr(request.state, "listing_preview", False),
        "stock_transfers_enabled": stock_transfers_enabled(),
    })


@router.get("/drafts")
def list_drafts(user=Depends(authorized), session: Session = Depends(get_session)):
    rows = session.exec(select(AppSetting).where(AppSetting.key.startswith(service.PREFIX, autoescape=True))).all()
    drafts = [json.loads(row.value) for row in rows]
    return {"drafts": [{"id": d["id"], "title": d["fields"].get("title") or d["fields"].get("product_name") or "Untitled sealed product",
                        "status": d["status"], "updated_at": d["updated_at"]}
                       for d in sorted(drafts, key=lambda d: d["updated_at"], reverse=True)[:50]]}


@router.get("/packaging")
def packaging(user=Depends(authorized), session: Session = Depends(get_session)):
    rows = session.exec(select(AppSetting).where(AppSetting.key.startswith("tiktok_listing_packaging:", autoescape=True))).all()
    return {"presets": [json.loads(row.value) for row in rows]}


@router.post("/packaging")
def save_packaging(body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        from uuid import uuid4
        name = str(body.get("name", "")).strip()
        if not 1 <= len(name) <= 80:
            raise ValueError("Give the packaging preset a name up to 80 characters.")
        preset = {"name": name, "weight": service.decimal_field(body.get("weight"), "Weight", "150", 3)}
        preset.update({k: service.decimal_field(body.get(k), k.title(), "120", 0) for k in ("length", "width", "height")})
        session.add(AppSetting(key="tiktok_listing_packaging:" + uuid4().hex, value=json.dumps(preset)))
        session.commit()
        return preset
    except Exception as exc:
        fail(exc)


@router.post("/drafts")
def new_draft(user=Depends(authorized), session: Session = Depends(get_session)):
    return public(service.create_draft(session, user.id))


@router.get("/drafts/{draft_id}")
def get_draft(draft_id: str, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = service.load_draft(session, draft_id)
        from ..tiktok.listing_jobs import expire_job
        if expire_job(draft):
            service.save_draft(session, draft, previous, user.id, "image_job_interrupted")
        return public(draft)
    except Exception as exc:
        fail(exc)


@router.get("/drafts/{draft_id}/images/{kind}")
def image(draft_id: str, kind: str, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, _ = service.load_draft(session, draft_id)
        return Response(service.read_asset(draft["assets"][kind]), media_type="image/png", headers={"Cache-Control": "private, no-store"})
    except Exception as exc:
        fail(exc)


@router.post("/drafts/{draft_id}/photo")
async def photo(draft_id: str, request: Request, file: UploadFile = File(...), user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        form = await request.form()
        draft, previous = editable(session, draft_id, int(form.get("version", 0)))
        raw = service.normalize_image(await file.read(service.MAX_IMAGE_BYTES + 1))
        draft["assets"]["photo"] = service.store_asset(raw)
        draft.pop("identification", None)
        draft["fields"]["product_confirmed"] = False
        # Save the photo before AI so timeouts cannot lose it.
        draft = service.save_draft(session, draft, previous, user.id, "photo_uploaded")
        identification = await run_in_threadpool(service.identify, raw)
        current, previous = editable(session, draft_id, draft["version"])
        current["identification"] = identification
        current["fields"]["product_confirmed"] = False
        return public(service.save_draft(session, current, previous, user.id, "identified"))
    except Exception as exc:
        fail(exc)


@router.post("/drafts/{draft_id}/search")
async def search(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = editable(session, draft_id, body.get("version"))
        query = str(body.get("query", "")).strip()
        if not 3 <= len(query) <= 180:
            raise ValueError("Enter a product name between 3 and 180 characters.")
        query = service.product_search_query(query, str(body.get("game", "Pokemon")))
        from ..inventory.routes import _search_sealed_products
        results, warning = await asyncio.wait_for(_search_sealed_products(query, game=str(body.get("game", "Pokemon")), limit=8), timeout=50)
        draft["candidates"] = [{key: str(p.get(key) or "") for key in
                                ("name", "set_name", "set_id", "kind", "game", "category_id", "language", "market_price", "market_price_source", "external_id", "external_url", "image_url", "source_name")} for p in results]
        for candidate in draft["candidates"]:
            candidate['language'] = defaults.language(candidate)
            candidate['price_checked_at'] = service.utcnow().isoformat()
            if candidate["external_id"].isdigit():
                candidate["catalog_image_url"] = candidate["image_url"]
                candidate["image_url"] = "https://product-images.tcgplayer.com/fit-in/800x800/" + candidate["external_id"] + ".jpg"
        draft["search_warning"] = warning
        return public(service.save_draft(session, draft, previous, user.id, "searched"))
    except Exception as exc:
        fail(exc)


@router.post("/drafts/{draft_id}/select")
def select_product(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = editable(session, draft_id, body.get("version"))
        index = body.get("index")
        if not isinstance(index, int) or not 0 <= index < len(draft["candidates"]):
            raise ValueError("Select one of the returned products.")
        selected = draft["candidates"][index]
        try:
            raw = service.fetch_product_image(selected["image_url"])
        except (httpx.HTTPError, ValueError):
            if not selected.get("catalog_image_url"):
                raise
            raw = service.fetch_product_image(selected["catalog_image_url"])
            selected["image_url"] = selected["catalog_image_url"]
        draft["selected_product"] = selected
        draft["assets"]["source"] = service.store_asset(raw)
        draft['assets'] = {k: v for k, v in draft['assets'].items() if k in {'photo', 'source'}}
        draft.pop('image_history', None)
        draft.pop('defaults', None)
        draft.pop('shop_metadata', None)
        draft.pop('language_options', None)
        draft.pop('missing_defaults', None)
        draft.pop('image_job', None)
        draft['fields'] = {k: v for k, v in draft['fields'].items() if k in {'quantity', 'fulfillment_mode', 'sealed_quantity'}}
        draft["fields"].update({"product_name": selected["name"], "image_heading": selected["name"], 'language': defaults.language(selected),
                               "product_confirmed": False, "image_confirmed": False, "review_confirmed": False})
        draft["duplicates"] = duplicates(session, selected["name"])
        return public(service.save_draft(session, draft, previous, user.id, "product_selected"))
    except Exception as exc:
        fail(exc)


@router.post('/drafts/{draft_id}/autofill')
def autofill(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = editable(session, draft_id, body.get('version'))
        product = draft.get('selected_product') or {}
        if not product.get('external_id'):
            raise ValueError('Choose a catalog product to look up defaults. Uploaded products can be completed in Edit details.')
        warnings, details, listing = [], {}, None
        try:
            details = defaults.catalog_details(product)
        except Exception:
            warnings.append('Product contents/language lookup is unavailable. Catalog identity is retained; review the details.')
        metadata = draft.get('shop_metadata') or {'categories': [], 'warehouses': [], 'attributes': []}
        try:
            # Rank synced listings locally, then verify the selected source live.
            rows = session.exec(select(TikTokProduct).order_by(TikTokProduct.synced_at.desc()).limit(1000)).all()
            catalog = []
            for row in rows:
                try:
                    raw = json.loads(row.raw_payload or '{}')
                    catalog.append({**raw, 'id': row.tiktok_product_id, 'title': row.title, 'status': row.status})
                except (ValueError, TypeError):
                    continue
            matches = defaults.rank_listings(product, draft['fields'], catalog, allow_stale_mode=True)
            ctx = context(session)
            # Cached titles can predate a fulfillment change. Verify a bounded
            # shortlist live rather than giving up on the first outdated row.
            for candidate in matches[:5]:
                try:
                    fresh = shop_call(ctx, '/product/202309/products/' + str(candidate['id']))
                except (httpx.HTTPError, ValueError):
                    continue
                if defaults.comparable(product, draft['fields'], fresh):
                    listing = fresh
                    break
            category = str((listing or {}).get('category_id') or next((c['id'] for c in (listing or {}).get('category_chains', []) if c.get('is_leaf')), ''))
            current_category = draft['fields'].get('category_id')
            if current_category and (not category or current_category != draft.get('defaults', {}).get('values', {}).get('category_id')):
                category = str(current_category)
            metadata = shop_fields(category, user, session)
            metadata['category_id'] = category
            if not listing:
                warnings.append('No verified shop listing matches this product type, language and order option. Complete the missing shop/package details.')
        except Exception:
            warnings.append('Shop defaults could not be verified. Previously loaded requirements are retained. Your product is saved; use Edit details or retry defaults.')
        proposed, sources = defaults.suggestions(product, draft['fields'], details, listing, metadata)
        defaults.apply_defaults(draft, proposed, sources, details, ' '.join(warnings))
        draft['shop_metadata'] = metadata
        draft['missing_defaults'] = defaults.missing_fields(draft['fields'], metadata)
        return public(service.save_draft(session, draft, previous, user.id, 'defaults_loaded'))
    except Exception as exc:
        fail(exc)


@router.post("/drafts/{draft_id}/source")
async def upload_source(draft_id: str, request: Request, file: UploadFile = File(...), user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        form = await request.form()
        draft, previous = editable(session, draft_id, int(form.get("version", 0)))
        raw = service.normalize_image(await file.read(service.MAX_IMAGE_BYTES + 1))
        draft["assets"]["source"] = service.store_asset(raw)
        draft["assets"].pop("designed", None)
        for key in list(draft['assets']):
            if key.startswith('history_'):
                draft['assets'].pop(key)
        draft.pop('image_history', None)
        draft["selected_product"] = {"source_name": "Staff upload", "external_url": ""}
        draft["fields"].update({"image_confirmed": False, "review_confirmed": False})
        return public(service.save_draft(session, draft, previous, user.id, "source_uploaded"))
    except Exception as exc:
        fail(exc)


TEXT_FIELDS = {"product_name", "title", "description", "language", "image_heading", "theme", "category_id", "brand_id", "warehouse_id", "price", "quantity", "weight", "length", "width", "height", "fulfillment_mode", "sealed_quantity", "rip_price"}
BOOL_FIELDS = {"product_confirmed", "image_confirmed", "shipping_confirmed", "review_confirmed", "duplicates_confirmed"}


@router.post("/drafts/{draft_id}/save")
def save(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = editable(session, draft_id, body.get("version"))
        fields = body.get("fields")
        if not isinstance(fields, dict) or len(json.dumps(fields)) > 18000:
            raise ValueError("Invalid listing fields.")
        clean = {k: str(v)[:8000 if k == "description" else 255] for k, v in fields.items() if k in TEXT_FIELDS}
        clean.update({k: v is True for k, v in fields.items() if k in BOOL_FIELDS})
        attrs = fields.get("attributes", {})
        if not isinstance(attrs, dict) or len(attrs) > 50:
            raise ValueError("Invalid category attributes.")
        clean["attributes"] = {str(k)[:100]: str(v)[:200] for k, v in attrs.items()}
        old = draft["fields"]
        for attr in draft.get('shop_metadata', {}).get('attributes', []):
            if str(attr.get('name', '')).lower() in {'language', 'card language'}:
                clean['attributes'][str(attr['id'])] = clean.get('language', old.get('language', ''))
        if clean.get('product_name', old.get('product_name')) != old.get('product_name') and draft.get('selected_product', {}).get('external_id'):
            draft['selected_product'] = {'source_name': 'Manual product details'}
            draft.pop('defaults', None)
            draft.pop('language_options', None)
            clean.update(price='', product_confirmed=False, image_confirmed=False)
            for key in defaults.PACKAGE_KEYS:
                clean[key] = ''
            draft['assets'].pop('source', None)
        if clean.get('language', old.get('language')) != old.get('language'):
            options = draft.get('language_options', [])
            if draft.get('selected_product', {}).get('external_id') and options and clean['language'] not in options:
                raise ValueError('Choose an available language for this product: ' + ', '.join(options) + '. Search for a different catalog edition if needed.')
            baseline = draft.get('defaults', {}).get('values', {})
            for key in ('title', 'description'):
                if baseline.get(key):
                    before = (' — ' + old['language'] + ' — ') if key == 'title' and old.get('language') else ('Language: ' + old['language']) if old.get('language') else ''
                    after = (' — ' + clean['language'] + ' — ') if key == 'title' else ('Language: ' + clean['language'])
                    generated = str(baseline[key])
                    if before:
                        updated = generated.replace(before, after)
                    elif key == 'title':
                        head, separator, tail = generated.rpartition(' — ')
                        updated = head + after + tail if separator else generated
                    else:
                        head, separator, tail = generated.partition('\n\n')
                        updated = head + '\n\n' + after + (separator + tail if separator else '')
                    if clean.get(key, old.get(key)) == baseline[key]:
                        clean[key] = updated
                    baseline[key] = updated
            if old.get('language'):
                clean.update(product_confirmed=False, image_confirmed=False, shipping_confirmed=False)
                for key in ('price', 'rip_price'):
                    if clean.get(key, old.get(key)) == old.get(key):
                        clean[key] = ''
                        if key in baseline:
                            baseline[key] = ''
                draft.get('defaults', {}).get('sources', {}).pop('price', None)
                for key in defaults.PACKAGE_KEYS:
                    clean[key] = ''
                    if key in baseline:
                        baseline[key] = ''
                # A catalog photo from a different language is not a valid source.
                if draft.get('selected_product', {}).get('external_id'):
                    draft['assets'].pop('source', None)
        if any(old.get(k) for k in defaults.PACKAGE_KEYS) and clean.get('fulfillment_mode', old.get('fulfillment_mode')) != old.get('fulfillment_mode'):
            # Rip packaging does not describe a complete sealed box.
            for key in defaults.PACKAGE_KEYS:
                clean[key] = ''
            clean['shipping_confirmed'] = False
            draft.get('defaults', {}).get('sources', {}).pop('shop', None)
        service.fulfillment({**old, **clean})
        changed = any(clean.get(k, old.get(k)) != old.get(k) for k in TEXT_FIELDS | {"attributes"})
        if changed:
            clean["review_confirmed"] = False
        if any(clean.get(k, old.get(k)) != old.get(k) for k in {"image_heading", "product_name", "language", "theme", "fulfillment_mode"}):
            draft["assets"].pop("designed", None)
            for key in list(draft['assets']):
                if key.startswith('history_'):
                    draft['assets'].pop(key)
            draft.pop('image_history', None)
        if clean.get("category_id", old.get("category_id")) != old.get("category_id"):
            clean["attributes"] = {}
            draft.pop('shop_metadata', None)
        draft["fields"].update(clean)
        draft['missing_defaults'] = defaults.missing_fields(draft['fields'], draft.get('shop_metadata', {}))
        draft["duplicates"] = duplicates(session, draft["fields"].get("product_name", ""))
        return public(service.save_draft(session, draft, previous, user.id))
    except Exception as exc:
        fail(exc)


@router.post("/drafts/{draft_id}/design", status_code=202)
def design(draft_id: str, body: dict, background_tasks: BackgroundTasks, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = editable(session, draft_id, body.get("version"))
        if not draft["assets"].get("source"):
            raise ValueError("Choose or upload a clean product image first.")
        from ..tiktok.listing_jobs import run_design
        from uuid import uuid4
        import time
        job_id = str(uuid4())
        revision = body.get('revision', '')
        if not isinstance(revision, str) or len(revision) > 1500:
            raise ValueError('Describe your image changes in 1,500 characters or fewer.')
        revision = revision.strip()
        if revision and not draft['assets'].get('designed'):
            raise ValueError('Generate an image before requesting changes to it.')
        missing = defaults.missing_fields(draft['fields'], draft.get('shop_metadata', {}))
        if missing:
            raise ValueError('Complete these details before generating: ' + ', '.join(missing) + '.')
        service.stock_allocation(draft['fields'])
        service.decimal_field(draft['fields'].get('price'), 'Price', '999999.99')
        if service.fulfillment(draft['fields']) == 'both' and draft['fields'].get('rip_price'):
            service.decimal_field(draft['fields']['rip_price'], 'Live Rip price', '999999.99')
        service.decimal_field(draft['fields'].get('weight'), 'Packed weight', '150', 3)
        for key in ('length', 'width', 'height'):
            service.decimal_field(draft['fields'].get(key), 'Packed ' + key, '120', 0)
        draft["image_job"] = {"id": job_id, "status": "running", "started_at": time.time(), 'revision': revision}
        service.save_draft(session, draft, previous, user.id, "image_job_started")
        background_tasks.add_task(run_design, session.get_bind(), draft_id, job_id, user.id)
        return public(draft)
    except Exception as exc:
        fail(exc)


@router.post('/drafts/{draft_id}/restore-image')
def restore_image(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = editable(session, draft_id, body.get('version'))
        key = body.get('key')
        entry = next((x for x in draft.get('image_history', []) if x['key'] == key), None)
        if not entry or key not in draft['assets']:
            raise ValueError('Choose one of this draft’s saved image versions.')
        draft['assets']['designed'], draft['assets'][key] = draft['assets'][key], draft['assets']['designed']
        draft['image_generator'], entry['generator'] = entry.get('generator', 'gpt-image-2'), draft.get('image_generator', 'gpt-image-2')
        draft['fields']['review_confirmed'] = False
        return public(service.save_draft(session, draft, previous, user.id, 'image_restored'))
    except Exception as exc:
        fail(exc)


def available_categories(categories):
    lookup = {str(c["id"]): c for c in categories}
    result = []
    for category in categories:
        statuses = category.get("permission_statuses") or []
        if not category.get("is_leaf") or (statuses and "AVAILABLE" not in statuses):
            continue
        labels, seen, node = [], set(), category
        while node and str(node["id"]) not in seen:
            seen.add(str(node["id"]))
            labels.append(str(node.get("local_name") or node.get("name") or node["id"]))
            node = lookup.get(str(node.get("parent_id")))
        result.append({"id": str(category["id"]), "name": " > ".join(reversed(labels)), "is_leaf": True})
    return result


@router.post('/drafts/{draft_id}/use-source-image')
def use_source_image(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = editable(session, draft_id, body.get('version'))
        if not draft['assets'].get('source'):
            raise ValueError('Choose a product photo first.')
        from ..tiktok.listing_jobs import remember_image
        remember_image(draft)
        draft['assets']['designed'] = draft['assets']['source']
        draft['image_generator'] = 'original-photo'
        draft['fields']['review_confirmed'] = False
        return public(service.save_draft(session, draft, previous, user.id, 'original_photo_selected'))
    except Exception as exc:
        fail(exc)


@router.get("/shop-fields")
def shop_fields(category_id: str = "", user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        ctx = context(session)
        categories = shop_call(ctx, "/product/202309/categories").get("categories", [])
        warehouses = shop_call(ctx, "/logistics/202309/warehouses").get("warehouses", [])
        attrs = shop_call(ctx, f"/product/202309/categories/{category_id}/attributes").get("attributes", []) if category_id.isdigit() else []
        # TikTok's v202309 response uses the misspelling is_requried.
        attrs = [{**a, "is_required": bool(a.get("is_required") or a.get("is_requried"))}
                 for a in attrs if a.get("type", "PRODUCT_PROPERTY") == "PRODUCT_PROPERTY"]
        return {"categories": available_categories(categories), "attributes": attrs,
                "warehouses": [{"id": w["id"], "name": w["name"]} for w in warehouses if w.get("effect_status") == "ENABLED" and w.get("type") == "SALES_WAREHOUSE"]}
    except Exception as exc:
        fail(exc)


@router.post('/drafts/{draft_id}/shop-fields')
def draft_shop_fields(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = editable(session, draft_id, body.get('version'))
        category = str(draft['fields'].get('category_id') or '')
        metadata = shop_fields(category, user, session)
        metadata['category_id'] = category
        draft['shop_metadata'] = metadata
        allowed = {str(a['id']) for a in metadata['attributes']}
        draft['fields']['attributes'] = {k: v for k, v in draft['fields'].get('attributes', {}).items() if k in allowed}
        for attr in metadata['attributes']:
            if str(attr.get('name', '')).lower() in {'language', 'card language'}:
                draft['fields']['attributes'][str(attr['id'])] = draft['fields'].get('language', '')
        draft['fields']['review_confirmed'] = False
        draft['missing_defaults'] = defaults.missing_fields(draft['fields'], metadata)
        return public(service.save_draft(session, draft, previous, user.id, 'shop_fields_loaded'))
    except Exception as exc:
        fail(exc)


@router.get("/brands")
def brands(q: str = "", category_id: str = "", user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        if not 1 <= len(q.strip()) <= 100:
            raise ValueError("Enter a brand name to search.")
        query = {"brand_name": q.strip(), "page_size": "50"}
        if category_id:
            query["category_id"] = category_id
        values = shop_call(context(session), "/product/202309/brands", query=query).get("brands", [])
        return {"brands": [{"id": str(v["id"]), "name": str(v["name"])} for v in values]}
    except Exception as exc:
        fail(exc)


@router.post("/drafts/{draft_id}/submit")
def submit(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = service.load_draft(session, draft_id)
        if draft["status"] == "submitted":
            return public(draft)
        draft, previous = editable(session, draft_id, body.get("version"))
        mode = body.get("mode")
        from uuid import uuid4
        draft["submission_key"] = str(uuid4())
        payload = service.build_payload(draft, mode, ["preflight"])
        if duplicates(session, draft["fields"].get("product_name", "")) and not draft["fields"].get("duplicates_confirmed"):
            raise ValueError("Review the matching listings and confirm a separate listing is intended.")
        ctx = context(session)
        metadata = shop_fields(payload["category_id"], user, session)
        if payload["category_id"] not in {str(c["id"]) for c in metadata["categories"]}:
            raise ValueError("Select an available leaf category from TikTok.")
        if draft["fields"]["warehouse_id"] not in {str(w["id"]) for w in metadata["warehouses"]}:
            raise ValueError("Select an enabled dispatch warehouse.")
        mapped_attrs = []
        for attr in metadata["attributes"]:
            if (attr.get("is_required") or attr.get("requirement", {}).get("is_required")) and not draft["fields"].get("attributes", {}).get(str(attr["id"])):
                raise ValueError("Complete required category attribute: " + str(attr.get("name", attr["id"])))
            value = draft["fields"].get("attributes", {}).get(str(attr["id"]))
            if value:
                match = next((v for v in attr.get("values", []) if v.get("name") == value), None)
                mapped_attrs.append({"id": str(attr["id"]), "values": [{"id": str(match["id"])} if match else {"name": value}]})
        payload["product_attributes"] = mapped_attrs
        draft["status"] = "submitting"
        draft["submitted_mode"] = mode
        service.save_draft(session, draft, previous, user.id, "submission_started")
    except HTTPException:
        raise
    except Exception as exc:
        fail(exc)
    create_started = False
    try:
        from scripts.tiktok_backfill import upload_tiktok_product_image
        with httpx.Client(timeout=60, follow_redirects=False) as client:
            uris = [upload_tiktok_product_image(client, image_data=service.read_asset(draft["assets"][kind]), file_name=kind + ".png", **ctx) for kind in ("designed", "source")]
        payload["main_images"] = [{"uri": uri} for uri in uris]
        validation = validate_listing(ctx, payload)
        current, previous = service.load_draft(session, draft_id)
        current['listing_validation'] = validation
        current["submitted_payload"] = payload
        service.save_draft(session, current, previous, user.id, "payload_ready")
        create_started = True
        result = shop_call(ctx, "/product/202309/products", payload)
        product_id = str(result.get("product_id") or result.get("id") or "")
        if not product_id:
            raise RuntimeError("TikTok returned no product ID.")
        current, previous = service.load_draft(session, draft_id)
        current.update({"status": "submitted", "product_id": product_id, "verification": "pending"})
        service.save_draft(session, current, previous, user.id, "submitted")
        return verify_submission(draft_id, user, session)
    except Exception as exc:
        current, previous = service.load_draft(session, draft_id)
        if current["status"] != "submitted":
            uncertain = create_started and not isinstance(exc, TikTokRejected)
            current["status"] = "unknown" if uncertain else "draft"
            current["submission_error"] = ("TikTok may have received this listing. Check Seller Center for SKU DGN-LST-" + draft_id + ". Do not recreate it until reconciled." if uncertain else str(exc) if isinstance(exc, (TikTokRejected, ListingValidationFailed)) else "Image upload or validation failed before product creation. It is safe to try again.")
            service.save_draft(session, current, previous, user.id, "submission_uncertain" if uncertain else "submission_failed")
        fail(exc)


@router.post("/drafts/{draft_id}/verify")
def verify_submission(draft_id: str, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = service.load_draft(session, draft_id)
        if not draft.get("product_id"):
            raise ValueError("No confirmed product ID. Check Seller Center before retrying creation.")
        ctx = context(session)
        detail = shop_call(ctx, "/product/202309/products/" + draft["product_id"])
        from scripts.tiktok_backfill import upsert_tiktok_product_row
        upsert_tiktok_product_row(session, detail, shop_id=ctx["shop_id"], shop_cipher=ctx["shop_cipher"], source="listing_assistant", dry_run=False)
        issues = service.verification_issues(draft.get("submitted_payload", {}), detail)
        draft.update({"verification": "verified" if not issues else "mismatch", "verification_issues": issues, "tiktok_status": detail.get("status"), "audit_status": detail.get("audit", {}).get("status")})
        return public(service.save_draft(session, draft, previous, user.id, "verified"))
    except Exception as exc:
        fail(exc)


@router.post("/drafts/{draft_id}/reconcile")
def reconcile(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, previous = service.load_draft(session, draft_id)
        if draft["status"] not in {"unknown", "submitting"}:
            raise ValueError("Only an uncertain submission can be reconciled.")
        product_id = str(body.get("product_id", ""))
        if not product_id.isdigit() or len(product_id) > 30:
            raise ValueError("Enter the product ID from Seller Center.")
        detail = shop_call(context(session), "/product/202309/products/" + product_id)
        expected_skus = {s["seller_sku"] for s in draft.get("submitted_payload", {}).get("skus", [])} or {"DGN-LST-" + draft_id}
        if not expected_skus.issubset({s.get("seller_sku") for s in detail.get("skus", [])}):
            raise ValueError("That product does not carry this draft’s unique seller SKU.")
        draft.update({"product_id": product_id, "status": "submitted", "submission_error": ""})
        service.save_draft(session, draft, previous, user.id, "reconciled")
        return verify_submission(draft_id, user, session)
    except Exception as exc:
        fail(exc)


def stock_draft(session, draft_id):
    draft, previous = service.load_draft(session, draft_id)
    if draft['status'] != 'submitted' or service.fulfillment(draft['fields']) != 'both':
        raise ValueError('Stock transfers require a confirmed two-variant TikTok product.')
    return draft, previous


def current_stock(session, draft):
    from ..tiktok.listing_stock import snapshot
    return snapshot(draft, shop_call(context(session), '/product/202309/products/' + draft['product_id']))


@router.get('/drafts/{draft_id}/stock')
def get_stock(draft_id: str, user=Depends(authorized), session: Session = Depends(get_session)):
    try:
        draft, _ = stock_draft(session, draft_id)
        return {'stock':current_stock(session, draft), 'transfer':draft.get('stock_transfer')}
    except Exception as exc:
        fail(exc)


@router.post('/drafts/{draft_id}/stock/preview')
def preview_transfer(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    require_stock_transfers()
    from ..tiktok.listing_stock import plan
    from uuid import uuid4
    import time
    try:
        draft, previous = stock_draft(session, draft_id)
        if body.get('version') != draft['version']:
            raise service.DraftConflict('This listing changed. Reload it before transferring stock.')
        if draft.get('stock_transfer', {}).get('status') in {'in_progress', 'needs_review'}:
            raise ValueError('The previous transfer needs reconciliation. Check Seller Center before another transfer.')
        current = current_stock(session, draft)
        draft['stock_transfer'] = {'id':str(uuid4()), 'status':'preview', 'actor_id':user.id,
                                   'created_at':time.time(), 'snapshot':current, **plan(current, body.get('amount'))}
        return public(service.save_draft(session, draft, previous, user.id, 'stock_transfer_preview'))
    except Exception as exc:
        fail(exc)


@router.post('/drafts/{draft_id}/stock/confirm')
def confirm_transfer(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    require_stock_transfers()
    from ..tiktok.listing_stock import update_payload, matches_plan
    import time
    try:
        draft, previous = stock_draft(session, draft_id)
        transfer = draft.get('stock_transfer', {})
        if body.get('transfer_id') != transfer.get('id') or transfer.get('actor_id') != user.id:
            raise ValueError('Preview this transfer with your account before confirming it.')
        if transfer.get('status') == 'completed':
            return public(draft)
        if body.get('version') != draft['version'] or transfer.get('status') != 'preview':
            raise service.DraftConflict('This transfer changed or has already been attempted. Reload its status; do not retry it.')
        if time.time() - transfer['created_at'] > 120:
            raise ValueError('The transfer preview expired. Refresh stock and preview again.')
        current = current_stock(session, draft)
        if current['fingerprint'] != transfer['snapshot']['fingerprint']:
            raise service.DraftConflict('TikTok stock or listing status changed. Preview a fresh transfer.')
        payload = update_payload(current, transfer)
        transfer['status'] = 'in_progress'
        transfer['started_at'] = time.time()
        service.save_draft(session, draft, previous, user.id, 'stock_transfer_started')
    except Exception as exc:
        fail(exc)
    # The CAS above grants exactly one caller permission to send this request.
    try:
        result = shop_call(context(session), '/product/202309/products/' + draft['product_id'] + '/inventory/update', payload)
        fresh = current_stock(session, draft)
        matches = matches_plan(current, transfer, fresh)
        success = not result.get('errors') and matches
        message = 'Transfer verified. Total stock is unchanged.' if success else 'TikTok returned a partial error or unexpected stock. Check Seller Center; do not retry this transfer.'
    except Exception:
        fresh, success = None, False
        message = 'The transfer outcome is uncertain. Check Seller Center before making further changes. No automatic retry was made.'
    draft, previous = stock_draft(session, draft_id)
    draft['stock_transfer'].update(status='completed' if success else 'needs_review', message=message, readback=fresh)
    if success:
        for sku in draft.get('submitted_payload', {}).get('skus', []):
            for option in ('sealed', 'rip'):
                if sku.get('seller_sku') == 'DGN-LST-' + draft_id + '-' + option:
                    for inventory in sku['inventory']:
                        if str(inventory['warehouse_id']) == current['warehouse_id']:
                            inventory['quantity'] = transfer['after'][option]
    return public(service.save_draft(session, draft, previous, user.id, 'stock_transfer_completed' if success else 'stock_transfer_needs_review'))


@router.post('/drafts/{draft_id}/stock/reconcile')
def reconcile_transfer(draft_id: str, body: dict, user=Depends(authorized), session: Session = Depends(get_session)):
    from ..tiktok.listing_stock import matches_plan
    import time
    try:
        draft, previous = stock_draft(session, draft_id)
        transfer = draft.get('stock_transfer', {})
        stalled = transfer.get('status') == 'in_progress' and time.time() - transfer.get('started_at',time.time()) > 300
        if transfer.get('status') != 'needs_review' and not stalled:
            raise ValueError('Only a transfer needing review can be reconciled.')
        fresh = current_stock(session, draft)
        if not matches_plan(transfer['snapshot'], transfer, fresh):
            raise ValueError('Current counts do not match the intended transfer. Review and correct the paused listing in Seller Center first. No stock was changed here.')
        transfer.update(status='completed', message='Transfer reconciled against TikTok stock.', readback=fresh)
        for sku in draft.get('submitted_payload', {}).get('skus', []):
            for option in ('sealed','rip'):
                if sku.get('seller_sku') == 'DGN-LST-' + draft_id + '-' + option:
                    for inventory in sku['inventory']:
                        if str(inventory['warehouse_id']) == fresh['warehouse_id']:
                            inventory['quantity'] = transfer['after'][option]
        return public(service.save_draft(session, draft, previous, user.id, 'stock_transfer_reconciled'))
    except Exception as exc:
        fail(exc)
