import base64
import json
import logging
import os
from typing import Dict, Any, Iterable, List, Optional

from googleapiclient.errors import HttpError
from google.cloud import pubsub_v1

from .dwd_auth import build_delegated_service

LOGGER = logging.getLogger(__name__)

# Scopes
SCOPE_GMAIL_READONLY = "https://www.googleapis.com/auth/gmail.readonly"
SCOPE_ADMIN_DIR_READONLY = "https://www.googleapis.com/auth/admin.directory.user.readonly"

# Env
GMAIL_TOPIC = os.environ.get("GMAIL_TOPIC", "")  # projects/..../topics/...
LLM_SINK_TOPIC = os.environ.get("LLM_SINK_TOPIC", "")  # optional; if set we publish message payloads here
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "")  # super admin for Directory API
WATCH_LABEL_IDS = [s for s in os.environ.get("WATCH_LABEL_IDS", "INBOX").split(",") if s]
WATCH_LABEL_FILTER_ACTION = os.environ.get("WATCH_LABEL_FILTER_ACTION", "include")  # include|exclude

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")

publisher = None
if LLM_SINK_TOPIC:
    try:
        publisher = pubsub_v1.PublisherClient()
    except Exception as e:
        LOGGER.warning("Pub/Sub publisher init failed: %s", e)


def list_domain_users() -> Iterable[str]:
    '''Enumerate all active users via Admin SDK Directory API.'''
    if not ADMIN_EMAIL:
        raise RuntimeError("ADMIN_EMAIL is required to enumerate users")

    service = build_delegated_service("admin", "directory_v1",
                                      [SCOPE_ADMIN_DIR_READONLY],
                                      subject=ADMIN_EMAIL)
    req = service.users().list(customer="my_customer", maxResults=200, orderBy="email")
    while req is not None:
        resp = req.execute()
        for u in resp.get("users", []):
            if u.get("suspended"):
                continue
            primary = u.get("primaryEmail")
            if primary:
                yield primary
        req = service.users().list_next(req, resp)


def start_watch_for_user(user_email: str) -> Dict[str, Any]:
    '''Call Gmail users.watch for a given user via DWD.'''
    svc = build_delegated_service("gmail", "v1", [SCOPE_GMAIL_READONLY], subject=user_email)
    body = {
        "topicName": GMAIL_TOPIC,
        "labelFilterAction": WATCH_LABEL_FILTER_ACTION,
        "labelIds": WATCH_LABEL_IDS
    }
    resp = svc.users().watch(userId="me", body=body).execute()
    return resp  # contains historyId (string), expiration (ms since epoch)


def list_history_since(user_email: str, start_history_id: str) -> Dict[str, Any]:
    '''Fetch Gmail history deltas from a starting historyId.'''
    svc = build_delegated_service("gmail", "v1", [SCOPE_GMAIL_READONLY], subject=user_email)
    items: List[Dict[str, Any]] = []
    page_token = None
    max_history_id = start_history_id
    while True:
        req = svc.users().history().list(
            userId="me",
            startHistoryId=start_history_id,
            historyTypes=["messageAdded","labelAdded"],
            pageToken=page_token
        )
        resp = req.execute()
        for h in resp.get("history", []):
            hid = h.get("id", max_history_id)
            if hid > max_history_id:
                max_history_id = hid
            # Collect newly added messages
            for m in h.get("messagesAdded", []):
                msg = m.get("message", {})
                mid = msg.get("id")
                if mid:
                    items.append({"messageId": mid})
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return {"items": items, "maxHistoryId": max_history_id}


def get_message(user_email: str, message_id: str, fmt: str = "full") -> Dict[str, Any]:
    svc = build_delegated_service("gmail", "v1", [SCOPE_GMAIL_READONLY], subject=user_email)
    return svc.users().messages().get(userId="me", id=message_id, format=fmt).execute()


def publish_to_llm_sink(payload: Dict[str, Any]):
    if not LLM_SINK_TOPIC or not publisher:
        LOGGER.info("LLM sink not configured; skipping publish.")
        return
    data = json.dumps(payload).encode("utf-8")
    publisher.publish(LLM_SINK_TOPIC, data=data)
