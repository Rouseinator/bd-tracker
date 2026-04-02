"""
BD Tracker -- Business Development Command Centre
Connects to Outlook via Microsoft Graph, tracks client outreach,
and uses AI to classify BD relevance and pipeline stage.
"""

from __future__ import annotations

import html
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import msal
import pandas as pd
import requests
import streamlit as st

log = logging.getLogger(__name__)

# ─── Page config (must be first Streamlit call) ──────────────────────────────

st.set_page_config(
    page_title="BD Tracker",
    page_icon="\U0001F4C8",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Constants ────────────────────────────────────────────────────────────────

try:
    CLIENT_ID = st.secrets["CLIENT_ID"]
except Exception:
    CLIENT_ID = "74a06330-3a89-4cf8-871d-9d783c483d9d"

try:
    TENANT_ID = st.secrets["TENANT_ID"]
except Exception:
    TENANT_ID = "a14b16a4-0cbe-435c-a893-78e3e95b09c3"

try:
    ANTHROPIC_API_KEY = st.secrets["ANTHROPIC_API_KEY"]
except Exception:
    ANTHROPIC_API_KEY = ""

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["Mail.Read"]
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
DEFAULT_INTERNAL_DOMAINS = ["forethought.com.au", "forethought.com"]
FOLLOW_UP_DAYS = 5

STAGE_ORDER = [
    "Outreach Sent",
    "Follow-up Needed",
    "Engaged",
    "Meeting Proposed",
    "Meeting Booked",
    "Proposal Requested",
    "Proposal Sent",
    "Commissioned",
    "Closed / Not Now",
]

STAGE_STYLES = {
    "Outreach Sent":     ("#b9d1ff", "rgba(110,168,254,0.10)", "rgba(110,168,254,0.22)"),
    "Follow-up Needed":  ("#ffe4a8", "rgba(255,212,121,0.10)", "rgba(255,212,121,0.22)"),
    "Engaged":           ("#bbffe3", "rgba(126,240,194,0.10)", "rgba(126,240,194,0.22)"),
    "Meeting Proposed":  ("#ddd1ff", "rgba(176,151,255,0.11)", "rgba(176,151,255,0.22)"),
    "Meeting Booked":    ("#c3fbff", "rgba(93,224,230,0.12)",  "rgba(93,224,230,0.22)"),
    "Proposal Requested":("#ffd0d0", "rgba(255,143,143,0.10)", "rgba(255,143,143,0.22)"),
    "Proposal Sent":     ("#ffe0ba", "rgba(255,173,90,0.11)",  "rgba(255,173,90,0.22)"),
    "Commissioned":      ("#d6ffd8", "rgba(117,243,128,0.11)", "rgba(117,243,128,0.22)"),
    "Closed / Not Now":  ("#dde4f1", "rgba(155,166,190,0.12)", "rgba(155,166,190,0.22)"),
    "Pending":           ("#8090a7", "rgba(128,144,167,0.08)", "rgba(128,144,167,0.15)"),
}

CONTACT_TYPE_STYLES = {
    "current_client":    ("#5ec6c1", "rgba(94,198,193,0.10)", "rgba(94,198,193,0.20)"),
    "prospective_client":("#b9d1ff", "rgba(110,168,254,0.10)", "rgba(110,168,254,0.20)"),
    "former_client":     ("#c8b8e0", "rgba(200,184,224,0.10)", "rgba(200,184,224,0.20)"),
    "not_relevant":      ("#8090a7", "rgba(128,144,167,0.08)", "rgba(128,144,167,0.15)"),
}

CONTACT_TYPE_LABELS = {
    "current_client": "Client",
    "prospective_client": "Prospect",
    "former_client": "Former",
    "not_relevant": "Not BD",
}

SAMPLE_DATA = [
    {
        "client_name": "Acme Advisory",
        "contact_name": "Sarah Lim",
        "counterparty_email": "sarah.lim@acmeadvisory.com",
        "owner": "Caleb",
        "stage": "Follow-up Needed",
        "last_touch": "2026-03-18T09:15:00Z",
        "days_since_touch": 14,
        "latest_subject": "Following up on market mix modelling support",
        "next_step": "Send a brief note with two available times for next week.",
        "notes": "Initial outreach landed well but no reply since the first note.",
        "contact_type": "prospective_client",
        "bd_relevant": True,
        "ai_confidence": 0.88,
        "ai_reasoning": "Thread discusses potential consulting engagement around market mix modelling. Contact is external and conversation is exploratory.",
        "ai_stage_reasoning": "Outbound message sent with no reply for 14 days. Needs follow-up.",
    },
    {
        "client_name": "North Coast Health",
        "contact_name": "Emma Wood",
        "counterparty_email": "emma.wood@nchealth.com.au",
        "owner": "Caleb",
        "stage": "Meeting Booked",
        "last_touch": "2026-03-30T03:00:00Z",
        "days_since_touch": 2,
        "latest_subject": "Confirmed: coffee next Tuesday",
        "next_step": "Prepare a one-page discussion agenda before the meeting.",
        "notes": "Catch-up locked in for Tuesday morning.",
        "contact_type": "current_client",
        "bd_relevant": True,
        "ai_confidence": 0.92,
        "ai_reasoning": "Ongoing relationship with an existing client. Meeting confirmed for relationship development.",
        "ai_stage_reasoning": "Meeting explicitly confirmed with a specific date.",
    },
    {
        "client_name": "Southbank Capital",
        "contact_name": "James Tran",
        "counterparty_email": "jtran@southbankcapital.com",
        "owner": "Caleb",
        "stage": "Proposal Sent",
        "last_touch": "2026-03-27T14:30:00Z",
        "days_since_touch": 5,
        "latest_subject": "Re: Scope & fee estimate for brand strategy review",
        "next_step": "Follow up on proposal feedback and timing.",
        "notes": "Proposal sent Thursday. Awaiting feedback from their GM.",
        "contact_type": "prospective_client",
        "bd_relevant": True,
        "ai_confidence": 0.95,
        "ai_reasoning": "Active proposal discussion with scope and fee language. Clear BD opportunity.",
        "ai_stage_reasoning": "Proposal with fee estimate has been sent. Awaiting client response.",
    },
    {
        "client_name": "Horizon Education",
        "contact_name": "Priya Mehta",
        "counterparty_email": "priya.mehta@horizonedu.com.au",
        "owner": "Caleb",
        "stage": "Engaged",
        "last_touch": "2026-03-29T11:00:00Z",
        "days_since_touch": 3,
        "latest_subject": "Re: Great chat at the conference",
        "next_step": "Keep momentum with a relevant next touchpoint.",
        "notes": "Good initial exchange after meeting at the industry conference last week.",
        "contact_type": "prospective_client",
        "bd_relevant": True,
        "ai_confidence": 0.78,
        "ai_reasoning": "Post-conference networking exchange. Early-stage prospecting — could develop into a BD opportunity.",
        "ai_stage_reasoning": "Two-way conversation is active. No specific meeting or proposal yet.",
    },
    {
        "client_name": "Pacific Retail Group",
        "contact_name": "Tom Nguyen",
        "counterparty_email": "tom.nguyen@pacificretail.com.au",
        "owner": "Caleb",
        "stage": "Outreach Sent",
        "last_touch": "2026-03-31T08:45:00Z",
        "days_since_touch": 1,
        "latest_subject": "Intro \u2014 Forethought x Pacific Retail",
        "next_step": "Monitor for reply or send a follow-up in a few days.",
        "notes": "Cold outreach sent. No response yet.",
        "contact_type": "prospective_client",
        "bd_relevant": True,
        "ai_confidence": 0.82,
        "ai_reasoning": "Initial outreach email to a new external contact. BD intent is clear from the subject line.",
        "ai_stage_reasoning": "Single outbound message with no reply. Early outreach stage.",
    },
]


# ─── CSS loader ───────────────────────────────────────────────────────────────

def load_css() -> None:
    css_path = Path(__file__).parent / "style.css"
    if css_path.exists():
        css_text = css_path.read_text(encoding="utf-8")
    else:
        css_text = ""
    st.markdown(f"<style>{css_text}</style>", unsafe_allow_html=True)


def _load_logo_b64() -> str:
    import base64
    logo_path = Path(__file__).parent / "logo.svg"
    if logo_path.exists():
        svg_bytes = logo_path.read_bytes()
        b64 = base64.b64encode(svg_bytes).decode("utf-8")
        return f"data:image/svg+xml;base64,{b64}"
    return ""


# ─── Session state ────────────────────────────────────────────────────────────

def init_state() -> None:
    defaults = {
        "access_token": None,
        "tracker_df": pd.DataFrame(),
        "raw_messages": [],
        "authenticated": False,
        "device_flow": None,
        "auth_message": "",
        "user_code": "",
        "auth_result": None,
        "account_label": "",
        "last_sync": None,
        "last_classify": None,
        "internal_domains": ", ".join(DEFAULT_INTERNAL_DOMAINS),
        "show_excluded": False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# ─── MSAL / Auth ─────────────────────────────────────────────────────────────

def _get_app() -> msal.PublicClientApplication:
    return msal.PublicClientApplication(client_id=CLIENT_ID, authority=AUTHORITY)


def start_device_flow() -> None:
    app = _get_app()
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(f"Failed to create device flow: {flow}")
    st.session_state.device_flow = flow
    st.session_state.auth_message = flow.get("message", "")
    st.session_state.user_code = flow.get("user_code", "")


def complete_device_flow() -> None:
    flow = st.session_state.device_flow
    if not flow:
        raise RuntimeError("Start the device code flow first.")
    app = _get_app()
    result = app.acquire_token_by_device_flow(flow)
    st.session_state.auth_result = result
    if "access_token" not in result:
        raise RuntimeError(f"Authentication failed: {result}")
    st.session_state.access_token = result["access_token"]
    st.session_state.authenticated = True
    claims = result.get("id_token_claims", {})
    display_name = claims.get("name") or claims.get("preferred_username") or "User"
    username = claims.get("preferred_username", "")
    st.session_state.account_label = (
        f"{display_name} ({username})" if username else display_name
    )


def sign_out() -> None:
    for key in [
        "access_token", "device_flow", "auth_result",
        "auth_message", "user_code",
    ]:
        st.session_state[key] = None
    st.session_state.authenticated = False
    st.session_state.account_label = ""
    st.session_state.tracker_df = pd.DataFrame()
    st.session_state.raw_messages = []
    st.session_state.last_sync = None
    st.session_state.last_classify = None


# ─── Microsoft Graph helpers ─────────────────────────────────────────────────

def _graph_get(path: str, params: dict = None) -> dict:
    token = st.session_state.access_token
    if not token:
        raise RuntimeError("No access token. Please sign in first.")
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(
        f"{GRAPH_BASE}{path}", headers=headers, params=params, timeout=30,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Graph API {resp.status_code}: {resp.text}")
    return resp.json()


def _email_addr(obj) -> str:
    try:
        return obj["emailAddress"]["address"].lower()
    except Exception:
        return None


def _email_name(obj) -> str:
    try:
        return obj["emailAddress"]["name"]
    except Exception:
        return ""


def _domain_to_client(email) -> str:
    if not email or "@" not in email:
        return "Unknown"
    domain = email.split("@", 1)[1]
    base = domain.split(".")[0]
    return " ".join(part.capitalize() for part in base.replace("-", " ").split())


def _is_internal(email, internal_domains) -> bool:
    if not email:
        return True
    lower = email.lower()
    return any(lower.endswith(f"@{d}") for d in internal_domains)


# ─── Email fetching & normalisation ──────────────────────────────────────────

def fetch_messages(limit: int = 100):
    select = (
        "id,subject,from,toRecipients,receivedDateTime,"
        "sentDateTime,conversationId,bodyPreview"
    )
    inbox = _graph_get(
        "/me/mailFolders/Inbox/messages",
        {"$top": limit, "$orderby": "receivedDateTime desc", "$select": select},
    ).get("value", [])
    sent = _graph_get(
        "/me/mailFolders/SentItems/messages",
        {"$top": limit, "$orderby": "sentDateTime desc", "$select": select},
    ).get("value", [])
    return inbox, sent


def normalise_messages(messages, box, internal_domains):
    rows = []
    for m in messages:
        if box == "inbox":
            counterparty = _email_addr(m.get("from", {}))
            if _is_internal(counterparty, internal_domains):
                continue
            rows.append({
                "message_id": m.get("id"),
                "conversation_id": m.get("conversationId"),
                "direction": "inbound",
                "datetime": m.get("receivedDateTime"),
                "subject": m.get("subject"),
                "counterparty_email": counterparty,
                "contact_name": _email_name(m.get("from", {})),
                "preview": m.get("bodyPreview"),
            })
        else:
            recipients = m.get("toRecipients", [])
            external = None
            for r in recipients:
                addr = _email_addr(r)
                if not _is_internal(addr, internal_domains):
                    external = r
                    break
            if not external and recipients:
                external = recipients[0]
            counterparty = _email_addr(external)
            if _is_internal(counterparty, internal_domains):
                continue
            rows.append({
                "message_id": m.get("id"),
                "conversation_id": m.get("conversationId"),
                "direction": "outbound",
                "datetime": m.get("sentDateTime"),
                "subject": m.get("subject"),
                "counterparty_email": counterparty,
                "contact_name": _email_name(external),
                "preview": m.get("bodyPreview"),
            })
    return rows


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _derive_days(date_str):
    if not date_str:
        return None
    dt = pd.to_datetime(date_str, utc=True, errors="coerce")
    if pd.isna(dt):
        return None
    return max(0, (datetime.now(timezone.utc) - dt.to_pydatetime()).days)


# ─── Tracker builder ─────────────────────────────────────────────────────────

def build_tracker(messages, owner):
    cols = [
        "client_name", "contact_name", "counterparty_email", "owner",
        "stage", "last_touch", "days_since_touch", "latest_subject",
        "next_step", "notes", "thread_data",
        "contact_type", "bd_relevant", "ai_confidence",
        "ai_reasoning", "ai_stage_reasoning",
    ]
    if not messages:
        return pd.DataFrame(columns=cols)

    grouped = {}
    for msg in messages:
        grouped.setdefault(msg["counterparty_email"], []).append(msg)

    rows = []
    for email, group in grouped.items():
        group.sort(
            key=lambda x: pd.to_datetime(x["datetime"], utc=True, errors="coerce"),
        )
        latest = group[-1]

        # Build thread summary for AI classification
        thread_data = []
        for m in group:
            thread_data.append({
                "direction": m["direction"],
                "date": m.get("datetime", ""),
                "subject": m.get("subject", ""),
                "preview": (m.get("preview") or "")[:500],
            })

        rows.append({
            "client_name": _domain_to_client(email),
            "contact_name": latest.get("contact_name", ""),
            "counterparty_email": email,
            "owner": owner,
            "stage": "Pending",
            "last_touch": latest.get("datetime"),
            "days_since_touch": _derive_days(latest.get("datetime")),
            "latest_subject": latest.get("subject", ""),
            "next_step": "Run AI classification to determine stage.",
            "notes": latest.get("preview", ""),
            "thread_data": json.dumps(thread_data),
            "contact_type": "",
            "bd_relevant": None,
            "ai_confidence": None,
            "ai_reasoning": "",
            "ai_stage_reasoning": "",
        })

    df = pd.DataFrame(rows)
    return df.sort_values(
        ["days_since_touch", "client_name"],
        ascending=[False, True],
        na_position="last",
    )


# ─── AI Classification (Anthropic Claude) ────────────────────────────────────

AI_RELEVANCE_SYSTEM = """You are an AI assistant for Forethought Outcomes, a consulting firm.
Your job is to classify whether an email thread is relevant to CLIENT BUSINESS DEVELOPMENT.

The tracker should ONLY include:
- Current clients (organisations that have commissioned or are actively working with Forethought)
- Prospective clients (organisations that Forethought is reaching out to or engaging with about potential work)
- Former/dormant clients (organisations that previously engaged Forethought but are currently inactive)

The tracker should EXCLUDE:
- Suppliers, vendors, or service providers to Forethought
- Fieldwork houses, research panel providers, or data suppliers
- Recruitment contacts, job applicants, or staffing agencies
- Admin or operational contacts (IT support, office management, subscriptions)
- Industry bodies, associations, or event organisers (unless they are also a client)
- Personal contacts or social messages
- Newsletters, automated notifications, or marketing emails
- Internal colleagues at Forethought

Respond with ONLY a JSON object, no other text."""

AI_STAGE_SYSTEM = """You are an AI assistant for Forethought Outcomes, a consulting firm.
Your job is to classify the BD pipeline stage of an email thread that has already been confirmed as relevant to client business development.

The stages are (in order of progression):
1. Outreach Sent - Initial contact made, no substantive reply yet
2. Follow-up Needed - Previous outreach went unanswered for several days
3. Engaged - Two-way conversation active, but no meeting or proposal yet
4. Meeting Proposed - A meeting has been suggested but not yet confirmed
5. Meeting Booked - A meeting or call is confirmed with a date/time
6. Proposal Requested - The client has asked for or discussed a proposal, scope, or pricing
7. Proposal Sent - A proposal, scope document, or fee estimate has been sent to the client
8. Commissioned - The client has agreed to proceed or the work has been confirmed
9. Closed / Not Now - The opportunity has been declined or deferred

Consider the FULL context: direction of messages, timing, language, and thread progression.
Do not rely only on keywords — consider what the conversation as a whole indicates.

Respond with ONLY a JSON object, no other text."""


def _call_claude(system_prompt, user_prompt):
    """Call Anthropic Messages API and return the text response."""
    if not ANTHROPIC_API_KEY:
        return None

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 512,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        },
        timeout=30,
    )
    if resp.status_code != 200:
        log.warning("Claude API error %s: %s", resp.status_code, resp.text[:200])
        return None

    data = resp.json()
    text = data.get("content", [{}])[0].get("text", "")
    return text


def _parse_json_response(text):
    """Extract JSON from a Claude response, handling markdown fences."""
    if not text:
        return None
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        log.warning("Failed to parse JSON from Claude: %s", text[:200])
        return None


def classify_relevance(contact_name, email, domain, thread_data_json):
    """Classify a single contact's BD relevance using Claude."""
    thread_data = json.loads(thread_data_json) if isinstance(thread_data_json, str) else thread_data_json

    thread_summary = ""
    for msg in thread_data:
        direction = "SENT" if msg["direction"] == "outbound" else "RECEIVED"
        thread_summary += f"[{direction}] {msg['date']}\n"
        thread_summary += f"Subject: {msg['subject']}\n"
        thread_summary += f"Preview: {msg['preview']}\n\n"

    user_prompt = f"""Classify this email thread for BD relevance.

Contact: {contact_name}
Email: {email}
Domain: {domain}

Thread ({len(thread_data)} messages):
{thread_summary}

Respond with this exact JSON structure:
{{
  "bd_relevant": true or false,
  "contact_type": "current_client" | "prospective_client" | "former_client" | "not_relevant",
  "confidence": 0.0 to 1.0,
  "reasoning": "One or two sentences explaining why this thread is or is not relevant to client BD."
}}"""

    text = _call_claude(AI_RELEVANCE_SYSTEM, user_prompt)
    result = _parse_json_response(text)
    if not result:
        return None
    return {
        "bd_relevant": result.get("bd_relevant", False),
        "contact_type": result.get("contact_type", "not_relevant"),
        "ai_confidence": result.get("confidence", 0.0),
        "ai_reasoning": result.get("reasoning", ""),
    }


def classify_stage(contact_name, email, thread_data_json):
    """Classify the BD pipeline stage using Claude."""
    thread_data = json.loads(thread_data_json) if isinstance(thread_data_json, str) else thread_data_json

    thread_summary = ""
    for msg in thread_data:
        direction = "SENT" if msg["direction"] == "outbound" else "RECEIVED"
        thread_summary += f"[{direction}] {msg['date']}\n"
        thread_summary += f"Subject: {msg['subject']}\n"
        thread_summary += f"Preview: {msg['preview']}\n\n"

    stages_str = ", ".join(STAGE_ORDER)

    user_prompt = f"""Classify the BD pipeline stage for this email thread.

Contact: {contact_name}
Email: {email}

Thread ({len(thread_data)} messages):
{thread_summary}

Available stages (in order of progression): {stages_str}

Important:
- Read the FULL thread carefully. Do not rely on individual keywords.
- A "catch up" or "coffee" is a Meeting, not a Proposal.
- "Proposal" stage requires actual discussion of scope, fees, pricing, or a formal proposal document.
- Consider who sent what, the sequence, and what has actually been agreed.

Respond with this exact JSON structure:
{{
  "stage": "one of the stages listed above",
  "confidence": 0.0 to 1.0,
  "reasoning": "One or two sentences explaining why this stage was chosen.",
  "next_step": "A specific, actionable next step for this thread."
}}"""

    text = _call_claude(AI_STAGE_SYSTEM, user_prompt)
    result = _parse_json_response(text)
    if not result:
        return None
    return {
        "stage": result.get("stage", "Pending"),
        "ai_confidence": result.get("confidence", 0.0),
        "ai_stage_reasoning": result.get("reasoning", ""),
        "next_step": result.get("next_step", ""),
    }


def run_ai_classification(df, progress_callback=None):
    """Run AI classification on all contacts in the tracker DataFrame."""
    if df.empty or not ANTHROPIC_API_KEY:
        return df

    total = len(df)
    results = []

    for idx, row in df.iterrows():
        i = len(results) + 1
        if progress_callback:
            progress_callback(i / total, f"Classifying {row['contact_name'] or row['counterparty_email']} ({i}/{total})")

        domain = row["counterparty_email"].split("@", 1)[1] if "@" in str(row.get("counterparty_email", "")) else ""

        # Step 1: BD relevance
        rel = classify_relevance(
            row.get("contact_name", ""),
            row.get("counterparty_email", ""),
            domain,
            row.get("thread_data", "[]"),
        )

        if rel:
            df.at[idx, "bd_relevant"] = rel["bd_relevant"]
            df.at[idx, "contact_type"] = rel["contact_type"]
            df.at[idx, "ai_confidence"] = rel["ai_confidence"]
            df.at[idx, "ai_reasoning"] = rel["ai_reasoning"]

            # Step 2: Stage classification (only if BD relevant)
            if rel["bd_relevant"]:
                stage_result = classify_stage(
                    row.get("contact_name", ""),
                    row.get("counterparty_email", ""),
                    row.get("thread_data", "[]"),
                )
                if stage_result:
                    df.at[idx, "stage"] = stage_result["stage"]
                    df.at[idx, "ai_stage_reasoning"] = stage_result["ai_stage_reasoning"]
                    df.at[idx, "next_step"] = stage_result["next_step"]
                    stage_conf = stage_result.get("ai_confidence", 0)
                    df.at[idx, "ai_confidence"] = round(
                        (rel["ai_confidence"] + stage_conf) / 2, 2
                    )
            else:
                # Not BD relevant — mark stage clearly
                df.at[idx, "stage"] = "Not BD"
                df.at[idx, "next_step"] = ""
        else:
            # API failed for this contact — leave as unclassified
            df.at[idx, "bd_relevant"] = None
            df.at[idx, "contact_type"] = ""

    return df


# ─── Sync action ─────────────────────────────────────────────────────────────

def sync_outlook():
    """Pull messages from Outlook and rebuild the tracker."""
    internal = [
        d.strip().lower()
        for d in st.session_state.internal_domains.split(",")
        if d.strip()
    ]
    inbox_raw, sent_raw = fetch_messages(limit=100)
    messages = (
        normalise_messages(inbox_raw, "inbox", internal)
        + normalise_messages(sent_raw, "sent", internal)
    )
    st.session_state.raw_messages = messages
    owner = (
        st.session_state.account_label.split(" (")[0]
        if st.session_state.account_label else "Me"
    )
    st.session_state.tracker_df = build_tracker(messages, owner)
    st.session_state.last_sync = datetime.now(timezone.utc)
    st.session_state.last_classify = None


# ─── HTML helpers ─────────────────────────────────────────────────────────────

def _esc(text):
    return html.escape(str(text)) if text else ""


def _pill_html(stage):
    fg, bg, border = STAGE_STYLES.get(
        stage, ("#b9d1ff", "rgba(110,168,254,0.10)", "rgba(110,168,254,0.22)"),
    )
    return (
        f'<span class="pill" style="color:{fg};background:{bg};'
        f'border-color:{border};">{_esc(stage)}</span>'
    )


def _contact_type_pill(contact_type):
    label = CONTACT_TYPE_LABELS.get(contact_type, "")
    if not label:
        return ""
    fg, bg, border = CONTACT_TYPE_STYLES.get(
        contact_type, ("#8090a7", "rgba(128,144,167,0.08)", "rgba(128,144,167,0.15)"),
    )
    return (
        f'<span class="pill" style="color:{fg};background:{bg};'
        f'border-color:{border};font-size:0.62rem;">{_esc(label)}</span>'
    )


def _confidence_bar(confidence):
    if confidence is None:
        return ""
    pct = int(confidence * 100)
    color = "#5ec6c1" if confidence >= 0.7 else "#ffe4a8" if confidence >= 0.4 else "#ffa07a"
    return (
        f'<div class="confidence-bar">'
        f'<div class="confidence-fill" style="width:{pct}%;background:{color};"></div>'
        f'</div>'
    )


def _days_html(days):
    if days is None:
        return ""
    cls = "days-badge days-urgent" if days >= FOLLOW_UP_DAYS else "days-badge days-normal"
    label = f"{days}d ago" if days > 0 else "Today"
    return f'<span class="{cls}">{label}</span>'


# ─── Filter logic ─────────────────────────────────────────────────────────────

def apply_filters(df, search, stage, sort, show_excluded):
    if df.empty:
        return df

    filtered = df.copy()

    # After AI classification, hide non-BD contacts unless toggled on
    if not show_excluded and "bd_relevant" in filtered.columns:
        # Keep: unclassified (None/NaN) OR bd_relevant == True
        classified_mask = filtered["bd_relevant"].notna()
        if classified_mask.any():
            relevant_mask = filtered["bd_relevant"].fillna(False).astype(bool)
            unclassified_mask = ~classified_mask
            filtered = filtered[relevant_mask | unclassified_mask]

    if search:
        mask = filtered.astype(str).apply(
            lambda col: col.str.contains(search, case=False, na=False),
        )
        filtered = filtered[mask.any(axis=1)]

    if stage != "All stages":
        filtered = filtered[filtered["stage"] == stage]

    sort_map = {
        "Days since touch (newest)": ("days_since_touch", False),
        "Days since touch (oldest)": ("days_since_touch", True),
        "Client A\u2013Z": ("client_name", True),
        "Stage": ("stage", True),
    }
    col, asc = sort_map.get(sort, ("days_since_touch", False))
    filtered = filtered.sort_values(col, ascending=asc, na_position="last")

    return filtered


# ═══════════════════════════════════════════════════════════════════════════════
#  RENDER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def _logo_img(height="28px"):
    src = _load_logo_b64()
    if src:
        return f'<img src="{src}" alt="Forethought Outcomes" style="height:{height};">'
    return '<span style="font-weight:700;color:#e8edf7;">Forethought</span>'


def render_auth_screen():
    _, center, _ = st.columns([1.3, 1.8, 1.3])

    with center:
        logo = _logo_img("36px")

        if not st.session_state.device_flow:
            st.markdown(
                f'<div class="auth-outer"><div class="auth-card">'
                f'<div class="auth-logo">{logo}</div>'
                f'<div class="auth-title">BD Tracker</div>'
                f'<div class="auth-subtitle">'
                f'Connect your Microsoft account to sync Outlook '
                f'and track client business development activity.'
                f'</div></div></div>',
                unsafe_allow_html=True,
            )
            if st.button(
                "Connect Microsoft Account",
                type="primary",
                use_container_width=True,
                key="auth_connect",
            ):
                try:
                    start_device_flow()
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

            st.markdown(
                '<div class="auth-footer">'
                'Read-only access \u00b7 Mail.Read permission only'
                '</div>',
                unsafe_allow_html=True,
            )

        else:
            code = st.session_state.user_code or "--------"
            st.markdown(
                f'<div class="auth-outer"><div class="auth-card">'
                f'<div class="auth-logo">{logo}</div>'
                f'<div class="auth-title">Enter the code below</div>'
                f'<div class="auth-subtitle">'
                f'Go to the Microsoft login page, enter this code, '
                f'and complete sign-in. Then return here.'
                f'</div>'
                f'<div class="device-code-display">{_esc(code)}</div>'
                f'<div class="auth-instruction">'
                f'Open <a href="https://microsoft.com/devicelogin" '
                f'target="_blank">microsoft.com/devicelogin</a> '
                f'and enter the code above.'
                f'</div></div></div>',
                unsafe_allow_html=True,
            )
            if st.button(
                "I\u2019ve completed sign-in",
                type="primary",
                use_container_width=True,
                key="auth_complete",
            ):
                with st.spinner("Verifying\u2026"):
                    try:
                        complete_device_flow()
                        st.rerun()
                    except Exception as exc:
                        st.error(str(exc))

            st.markdown(
                '<div class="auth-footer">'
                'Read-only access \u00b7 Mail.Read permission only'
                '</div>',
                unsafe_allow_html=True,
            )


def render_top_nav():
    left, right = st.columns([3, 4])

    with left:
        st.markdown(
            f'<div class="nav-brand">{_logo_img("26px")}</div>',
            unsafe_allow_html=True,
        )

    with right:
        c1, c2, c3, c4 = st.columns([2.5, 2.5, 2.5, 1.5])
        with c1:
            st.markdown(
                f'<div class="nav-account">'
                f'{_esc(st.session_state.account_label)}</div>',
                unsafe_allow_html=True,
            )
        with c2:
            if st.button("Sync Outlook", type="primary", use_container_width=True):
                with st.spinner("Syncing\u2026"):
                    try:
                        sync_outlook()
                        st.rerun()
                    except Exception as exc:
                        st.error(str(exc))
        with c3:
            has_data = not st.session_state.tracker_df.empty
            ai_ready = bool(ANTHROPIC_API_KEY) and has_data
            if st.button(
                "Classify AI",
                type="primary" if ai_ready else "secondary",
                use_container_width=True,
                disabled=not ai_ready,
            ):
                progress = st.progress(0, text="Starting AI classification\u2026")
                try:
                    st.session_state.tracker_df = run_ai_classification(
                        st.session_state.tracker_df,
                        progress_callback=lambda p, t: progress.progress(p, text=t),
                    )
                    st.session_state.last_classify = datetime.now(timezone.utc)
                    progress.empty()
                    st.rerun()
                except Exception as exc:
                    progress.empty()
                    st.error(f"AI classification error: {exc}")
        with c4:
            if st.button("Sign out", use_container_width=True):
                sign_out()
                st.rerun()

    st.markdown('<hr class="subtle-divider">', unsafe_allow_html=True)

    with st.expander("Settings"):
        new_domains = st.text_input(
            "Internal domains (comma-separated)",
            value=st.session_state.internal_domains,
            help="Email domains to exclude as internal.",
        )
        if new_domains != st.session_state.internal_domains:
            st.session_state.internal_domains = new_domains


def render_pipeline_bar(df):
    st.markdown(
        '<div class="section-header">Pipeline</div>', unsafe_allow_html=True,
    )

    blocks = []
    for stage in STAGE_ORDER:
        count = int((df["stage"] == stage).sum()) if not df.empty else 0
        fg, bg, border = STAGE_STYLES[stage]
        zero_class = " zero" if count == 0 else ""
        blocks.append(
            f'<div class="pipeline-stage{zero_class}" '
            f'style="border-color:{border};background:{bg};">'
            f'<div class="pipeline-count" style="color:{fg};">{count}</div>'
            f'<div class="pipeline-label" style="color:{fg};">{_esc(stage)}</div>'
            f'</div>'
        )

    st.markdown(
        f'<div class="pipeline-bar">{"".join(blocks)}</div>',
        unsafe_allow_html=True,
    )


def render_kpi_row(df):
    total = len(df)
    follow_up = int((df["stage"] == "Follow-up Needed").sum()) if not df.empty else 0
    meetings = int(
        df["stage"].isin(["Meeting Proposed", "Meeting Booked"]).sum()
    ) if not df.empty else 0
    proposals = int(
        df["stage"].isin(["Proposal Requested", "Proposal Sent"]).sum()
    ) if not df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Active Contacts", total)
    c2.metric("Need Follow-up", follow_up)
    c3.metric("Meetings", meetings)
    c4.metric("Proposals", proposals)


def render_filter_bar(df):
    c1, c2, c3, c4 = st.columns([3, 1.5, 1.5, 1])
    with c1:
        search = st.text_input(
            "Search",
            placeholder="Search by client, contact, subject\u2026",
            label_visibility="collapsed",
        )
    with c2:
        stage_options = ["All stages"]
        if not df.empty:
            stage_options += sorted(df["stage"].dropna().unique().tolist())
        stage = st.selectbox("Stage", stage_options, label_visibility="collapsed")
    with c3:
        sort = st.selectbox(
            "Sort",
            [
                "Days since touch (newest)",
                "Days since touch (oldest)",
                "Client A\u2013Z",
                "Stage",
            ],
            label_visibility="collapsed",
        )
    with c4:
        show_excluded = st.checkbox("Show excluded", value=st.session_state.show_excluded)
        st.session_state.show_excluded = show_excluded

    return search, stage, sort, show_excluded


def render_contact_cards(df):
    if df.empty:
        st.markdown(
            '<div class="empty-state">'
            '<div class="empty-state-icon">\U0001F4ED</div>'
            '<div class="empty-state-title">No contacts to display</div>'
            '<div class="empty-state-desc">'
            'Sync your Outlook to pull in recent client communications, '
            'or adjust your filters above.'
            '</div></div>',
            unsafe_allow_html=True,
        )
        return

    for _, row in df.iterrows():
        stage = row.get("stage", "")
        days = row.get("days_since_touch")
        touch_dt = pd.to_datetime(row.get("last_touch"), utc=True, errors="coerce")
        date_str = touch_dt.strftime("%d %b %Y") if pd.notna(touch_dt) else "\u2014"

        contact_type = row.get("contact_type", "")
        bd_relevant = row.get("bd_relevant")
        confidence = row.get("ai_confidence")
        reasoning = row.get("ai_reasoning", "")
        stage_reasoning = row.get("ai_stage_reasoning", "")

        notes_text = str(row.get("notes", "")) if row.get("notes") else ""
        if len(notes_text) > 300:
            notes_text = notes_text[:300] + "\u2026"

        # Build AI insight section for details
        ai_section = ""
        if contact_type or reasoning:
            ai_section = '<div class="card-detail-ai">'
            if reasoning:
                ai_section += (
                    f'<div class="card-detail-row">'
                    f'<span class="card-detail-label">AI Relevance</span>'
                    f'<span class="card-detail-value">{_esc(reasoning)}</span>'
                    f'</div>'
                )
            if stage_reasoning:
                ai_section += (
                    f'<div class="card-detail-row">'
                    f'<span class="card-detail-label">AI Stage</span>'
                    f'<span class="card-detail-value">{_esc(stage_reasoning)}</span>'
                    f'</div>'
                )
            if confidence is not None:
                pct = int(confidence * 100)
                ai_section += (
                    f'<div class="card-detail-row">'
                    f'<span class="card-detail-label">Confidence</span>'
                    f'<span class="card-detail-value">{pct}%</span>'
                    f'</div>'
                )
            ai_section += '</div>'

        # Dim the card if not BD relevant
        card_class = "contact-card"
        if bd_relevant is False:
            card_class += " contact-card-excluded"

        card_html = (
            f'<div class="{card_class}">'
            f'<div class="contact-card-header">'
            f'<div class="contact-card-left">'
            f'<span class="contact-card-client">{_esc(row.get("client_name", ""))}</span>'
            f'<span class="contact-card-name">{_esc(row.get("contact_name", ""))}</span>'
            f'</div>'
            f'<div class="contact-card-right">'
            f'{_contact_type_pill(contact_type)}'
            f'{_pill_html(stage)}'
            f'{_days_html(days)}'
            f'</div>'
            f'</div>'
            f'<div class="contact-card-subject">{_esc(row.get("latest_subject", ""))}</div>'
            f'<div class="contact-card-meta">'
            f'<div class="contact-card-next">{_esc(row.get("next_step", ""))}</div>'
            f'</div>'
            f'<details class="card-details">'
            f'<summary>View details</summary>'
            f'<div class="card-detail-row">'
            f'<span class="card-detail-label">Email</span>'
            f'<span class="card-detail-value">{_esc(row.get("counterparty_email", ""))}</span>'
            f'</div>'
            f'<div class="card-detail-row">'
            f'<span class="card-detail-label">Owner</span>'
            f'<span class="card-detail-value">{_esc(row.get("owner", ""))}</span>'
            f'</div>'
            f'<div class="card-detail-row">'
            f'<span class="card-detail-label">Last touch</span>'
            f'<span class="card-detail-value">{date_str}</span>'
            f'</div>'
            f'<div class="card-detail-notes">{_esc(notes_text)}</div>'
            f'{ai_section}'
            f'</details>'
            f'</div>'
        )
        st.markdown(card_html, unsafe_allow_html=True)


def render_csv_export(df):
    if df.empty:
        return
    _, right = st.columns([5, 1])
    with right:
        export_df = df.drop(columns=["thread_data"], errors="ignore")
        csv_bytes = export_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Export CSV",
            data=csv_bytes,
            file_name="bd_tracker_export.csv",
            mime="text/csv",
            use_container_width=True,
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    init_state()
    load_css()

    if not st.session_state.authenticated:
        render_auth_screen()
        return

    render_top_nav()

    df = st.session_state.tracker_df
    using_sample = False
    if df.empty:
        df = pd.DataFrame(SAMPLE_DATA)
        using_sample = True

    # Pipeline/KPIs only show BD-relevant contacts (exclude Pending and Not BD)
    display_df = df.copy()
    if "bd_relevant" in display_df.columns:
        classified = display_df["bd_relevant"].notna()
        if classified.any():
            bd_only = display_df[display_df["bd_relevant"].fillna(False).astype(bool)]
        else:
            bd_only = display_df
    else:
        bd_only = display_df

    render_pipeline_bar(bd_only)
    render_kpi_row(bd_only)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    if using_sample:
        st.markdown(
            '<div class="sample-notice">'
            'Showing sample data \u2014 click "Sync Outlook" to load your real activity, '
            'then "Classify AI" to run intelligent classification.'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    elif not ANTHROPIC_API_KEY:
        st.markdown(
            '<div class="sample-notice">'
            'Add ANTHROPIC_API_KEY to Streamlit secrets to enable AI classification.'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    search, stage, sort, show_excluded = render_filter_bar(df)
    filtered = apply_filters(df, search, stage, sort, show_excluded)

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    render_contact_cards(filtered)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    render_csv_export(filtered)

    # Status footer
    parts = []
    if st.session_state.last_sync:
        parts.append(f"Synced: {st.session_state.last_sync.strftime('%d %b %H:%M UTC')}")
    if st.session_state.last_classify:
        parts.append(f"Classified: {st.session_state.last_classify.strftime('%d %b %H:%M UTC')}")
    if parts:
        st.markdown(
            f'<div class="sample-notice">{" \u00b7 ".join(parts)}</div>',
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()
