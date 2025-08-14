
import base64
import json
import logging
import os
import time
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, Request, Response, HTTPException
from pydantic import BaseModel

from google.oauth2 import id_token
from google.auth.transport import requests as google_requests

from .gmail_client import (
    list_domain_users, start_watch_for_user, list_history_since,
    get_message, publish_to_llm_sink, headers_to_dict, basic_email_summary, LLM_SINK_TOPIC
)
from . import models

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

PUBSUB_AUDIENCE = os.environ.get("PUBSUB_AUDIENCE", "")  # Cloud Run service URL for OIDC token validation
RENEW_LEEWAY_SEC = int(os.environ.get("RENEW_LEEWAY_SEC", "86400"))  # default: 24h

app = FastAPI()


@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": int(time.time())}


class BootstrapRequest(BaseModel):
    users: Optional[List[str]] = None  # if omitted, enumerate all via Admin SDK


@app.post("/bootstrap")
def bootstrap(req: BootstrapRequest):
    users = req.users or list(list_domain_users())
    created = []
    for user in users:
        try:
            resp = start_watch_for_user(user)
            models.upsert_watcher(user, {
                "user": user,
                "historyId": resp.get("historyId"),
                "expiration": int(resp.get("expiration", 0)),
                "updatedAt": int(time.time() * 1000)
            })
            created.append({"user": user, "expiration": resp.get("expiration"), "historyId": resp.get("historyId")})
            logger.info("Started watch for %s exp=%s", user, resp.get("expiration"))
        except Exception as e:
            logger.exception("Failed to start watch for %s: %s", user, e)
    return {"created": created, "count": len(created)}


@app.post("/renew")
def renew():
    expiring = models.watchers_expiring_within(RENEW_LEEWAY_SEC)
    renewed = []
    for w in expiring:
        user = w["user"]
        try:
            resp = start_watch_for_user(user)
            models.upsert_watcher(user, {
                "user": user,
                "historyId": resp.get("historyId"),
                "expiration": int(resp.get("expiration", 0)),
                "updatedAt": int(time.time() * 1000)
            })
            renewed.append({"user": user, "expiration": resp.get("expiration")})
            logger.info("Renewed watch for %s", user)
        except Exception as e:
            logger.exception("Failed to renew watch for %s: %s", user, e)
    return {"renewed": renewed, "count": len(renewed)}


def _verify_pubsub_push(req: Request):
    # Verify OIDC token if present (recommended for Pub/Sub push to Cloud Run)
    auth_header = req.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header.split(" ", 1)[1]
        if not PUBSUB_AUDIENCE:
            return  # best effort if not configured
        try:
            info = id_token.verify_oauth2_token(
                token,
                google_requests.Request(),
                audience=PUBSUB_AUDIENCE
            )
            return
        except Exception as e:
            logger.error("OIDC token verification failed: %s", e)
            raise HTTPException(status_code=401, detail="Invalid token")


@app.post("/pubsub")
async def pubsub_push(request: Request):
    _verify_pubsub_push(request)

    envelope = await request.json()
    if not envelope or "message" not in envelope:
        raise HTTPException(status_code=400, detail="Bad Pub/Sub message")
    message = envelope["message"]

    data_b64 = message.get("data")
    if not data_b64:
        return {"status": "no-data"}
    try:
        decoded = base64.b64decode(data_b64).decode("utf-8")
        payload = json.loads(decoded)
    except Exception:
        logger.exception("Failed to decode Pub/Sub data")
        raise HTTPException(status_code=400, detail="Invalid data")

    # Gmail watch payload contains emailAddress and historyId
    user_email = payload.get("emailAddress")
    history_id = payload.get("historyId")
    if not user_email or not history_id:
        logger.warning("Missing emailAddress/historyId in payload: %s", payload)
        return {"status": "ignored"}

    # Determine start history id from store
    state = models.get_watcher(user_email) or {}
    start_id = state.get("historyId", str(history_id))

    # Fetch deltas
    deltas = list_history_since(user_email, start_id)
    max_hid = deltas.get("maxHistoryId", str(history_id))

    processed = 0
    skipped_dupe = 0

    for item in deltas.get("items", []):
        mid = item["messageId"]
        try:
            msg = get_message(user_email, mid, fmt="full")
            h = headers_to_dict(msg)
            rfc822_id = h.get("message-id")
            if rfc822_id:
                # Deduplicate across the whole domain based on RFC822 Message-Id
                is_new = models.seen_check_and_mark(rfc822_id, {
                    "user": user_email,
                    "gmailId": msg.get("id"),
                    "threadId": msg.get("threadId"),
                    "subject": h.get("subject",""),
                    "from": h.get("from",""),
                    "date": h.get("date","")
                })
                if not is_new:
                    skipped_dupe += 1
                    continue

            # If we have an LLM sink topic, publish the raw message payload
            if LLM_SINK_TOPIC:
                publish_to_llm_sink({
                    "user": user_email,
                    "message": msg
                })
            else:
                # Verbose logging when no sink configured
                summary = basic_email_summary(user_email, msg)
                logger.info("[GMAIL INGEST] %s", json.dumps(summary))

            processed += 1

        except Exception as e:
            logger.exception("Failed to fetch/publish message %s for %s: %s", mid, user_email, e)

    # Update stored historyId regardless
    models.upsert_watcher(user_email, {
        "user": user_email,
        "historyId": max_hid,
        "updatedAt": int(time.time() * 1000)
    })

    return {"status": "ok", "user": user_email, "new": processed, "skipped_dupe": skipped_dupe}


@app.get("/status")
def status_json():
    watchers = models.list_all_watchers()
    expiring = models.watchers_expiring_within(24*3600)
    recent = models.recent_seen(25)
    return {
        "watchers": watchers,
        "expiringSoonCount": len(expiring),
        "recentSeen": recent
    }


@app.get("/admin")
def admin_page():
    data = status_json()
    rows = ""
    for it in data["recentSeen"]:
        rows += f"<tr><td>{it.get('date','')}</td><td>{it.get('subject','')}</td><td>{it.get('from','')}</td><td>{it.get('user','')}</td></tr>"
    html = """
    <!doctype html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>Gmail Ingest - Admin</title>
        <style>
            body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #ddd; padding: 8px; }
            th { background: #f5f5f5; text-align: left; }
            .pill { display:inline-block;padding:2px 8px;border-radius:9999px;background:#eef; }
        </style>
    </head>
    <body>
        <h2>Gmail Ingest - Admin</h2>
        <p><span class="pill">Watchers: {len(data["watchers"])}</span>
           <span class="pill">Expiring &lt;24h: {data["expiringSoonCount"]}</span></p>

        <h3>Recent seen messages</h3>
        <table>
            <thead><tr><th>Date</th><th>Subject</th><th>From</th><th>User (mailbox)</th></tr></thead>
            <tbody>{rows or '<tr><td colspan="4">No records yet</td></tr>'}</tbody>
        </table>
    </body>
    </html>
    """
    return Response(content=html, media_type="text/html")
