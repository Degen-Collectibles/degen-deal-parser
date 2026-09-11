"""Persist image-job outcomes so browser/proxy timeouts cannot lose the result."""
import logging
import time

from sqlmodel import Session
from . import listing_assistant as service
from .listing_images import ImageGenerationError


def expire_job(draft):
    job = draft.get("image_job", {})
    if job.get("status") == "running" and time.time() - job["started_at"] > 600:
        job.update(status="failed", code="image_timeout", message="Generation was interrupted or exceeded its time limit. No automatic retry was made. Your previous image is saved; review it before trying again.")
        return True
    return False


def run_design(bind, draft_id, job_id, actor_id):
    from .listing_images import generate
    try:
        with Session(bind) as session:
            draft, _ = service.load_draft(session, draft_id)
            if draft.get("image_job", {}).get("id") != job_id or draft["image_job"]["status"] != "running":
                return
            source = service.read_asset(draft["assets"]["source"])
            fields = dict(draft["fields"])
        # Release the DB transaction/connection during the provider request.
        try:
            generated = generate(source, fields)
            error = None
        except ImageGenerationError as exc:
            generated, error = None, {"code": exc.code, "message": str(exc)}
        except Exception:
            generated, error = None, {"code": "image_failed", "message": "Image generation could not complete. Your draft and previous image are saved. Try again later or use the original product photo."}
        with Session(bind) as session:
            draft, previous = service.load_draft(session, draft_id)
            job = draft.get("image_job", {})
            if job.get("id") != job_id or job.get("status") != "running":
                return
            if error:
                job.update(status="failed", **error)
            else:
                draft["assets"]["designed"] = service.store_asset(generated)
                draft["image_generator"] = "gpt-image-2"
                draft["fields"]["review_confirmed"] = False
                job.update(status="completed")
            service.save_draft(session, draft, previous, actor_id, "image_job_" + job["status"])
    except Exception:
        # No provider exception/URL is logged. The durable running record expires
        # visibly on refresh; it is never automatically submitted again.
        logging.getLogger(__name__).exception("Listing image job could not save its outcome", exc_info=False)
