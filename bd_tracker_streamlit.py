"""
BD Tracker -- Business Development Command Centre
Connects to Outlook via Microsoft Graph, tracks client outreach,
and uses AI to classify BD relevance and pipeline stage.
"""

from __future__ import annotations

import html
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
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
DEFAULT_INTERNAL_DOMAINS = ["forethought.com.au", "forethought.com", "brandcomms.ai"]
FOLLOW_UP_DAYS = 5
MELB_TZ = ZoneInfo("Australia/Melbourne")

STAGE_ORDER = [
    "Outreach",
    "In Conversation",
    "Proposal",
    "Commissioned",
    "Active Project",
    "Dormant",
    "Closed",
]

STAGE_DEFINITIONS = {
    "Outreach": "Initial contact or re-engagement. Message sent but no reply yet, or first-time contact with no two-way exchange.",
    "In Conversation": "Active two-way exchange \u2014 catch-ups, coffees, meetings, exploring needs. No mention of scope, fees, or proposals yet.",
    "Proposal": "Any discussion of scope, fees, pricing, quotes, budgets, deliverables, or formal project outlines. Includes both requesting and sending proposals.",
    "Commissioned": "Client has explicitly agreed to proceed \u2014 \u201clet\u2019s go ahead\u201d, PO attached, contract signed, budget approved.",
    "Active Project": "Work is underway. Deliverables being produced, status updates, data sharing, draft reports, fieldwork in progress.",
    "Dormant": "Thread has gone quiet \u2014 last message is 21+ days old with no reply, or the conversation visibly stalled.",
    "Closed": "Opportunity explicitly declined or lost \u2014 \u201cdecided to go another direction\u201d, \u201cnot proceeding\u201d, \u201cbudget was cut\u201d.",
}

STAGE_STYLES = {
    "Outreach":        ("#b9d1ff", "rgba(110,168,254,0.10)", "rgba(110,168,254,0.22)"),
    "In Conversation": ("#bbffe3", "rgba(126,240,194,0.10)", "rgba(126,240,194,0.22)"),
    "Proposal":        ("#ffe0ba", "rgba(255,173,90,0.11)",  "rgba(255,173,90,0.22)"),
    "Commissioned":    ("#d6ffd8", "rgba(117,243,128,0.11)", "rgba(117,243,128,0.22)"),
    "Active Project":  ("#c3fbff", "rgba(93,224,230,0.12)",  "rgba(93,224,230,0.22)"),
    "Dormant":         ("#ffe4a8", "rgba(255,212,121,0.10)", "rgba(255,212,121,0.22)"),
    "Closed":          ("#dde4f1", "rgba(155,166,190,0.12)", "rgba(155,166,190,0.22)"),
    "Pending":         ("#8090a7", "rgba(128,144,167,0.08)", "rgba(128,144,167,0.15)"),
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


# ─── Classification memory (persistence) ─────────────────────────────────────

_MEMORY_FILE = Path(__file__).parent / "classification_memory.json"


def _load_memory() -> dict:
    """Load saved classifications from disk."""
    if _MEMORY_FILE.exists():
        try:
            return json.loads(_MEMORY_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_memory(memory: dict) -> None:
    """Save classifications to disk."""
    try:
        _MEMORY_FILE.write_text(json.dumps(memory, indent=2, default=str), encoding="utf-8")
    except OSError as exc:
        log.warning("Could not save classification memory: %s", exc)


def _apply_memory(df) -> pd.DataFrame:
    """Apply saved classifications to a DataFrame after sync."""
    memory = _load_memory()
    if not memory:
        return df
    for idx, row in df.iterrows():
        email = row.get("counterparty_email", "")
        if email in memory:
            saved = memory[email]
            df.at[idx, "bd_relevant"] = saved.get("bd_relevant")
            df.at[idx, "contact_type"] = saved.get("contact_type", "")
            df.at[idx, "stage"] = saved.get("stage", "Pending")
            df.at[idx, "ai_confidence"] = saved.get("ai_confidence")
            df.at[idx, "ai_reasoning"] = saved.get("ai_reasoning", "")
            df.at[idx, "ai_stage_reasoning"] = saved.get("ai_stage_reasoning", "")
            df.at[idx, "next_step"] = saved.get("next_step", "")
            # Restore proper company name if saved
            saved_name = saved.get("client_name", "")
            if saved_name:
                df.at[idx, "client_name"] = saved_name
    return df


def _update_memory(df) -> None:
    """Save current classifications to memory file."""
    memory = _load_memory()
    for _, row in df.iterrows():
        email = row.get("counterparty_email", "")
        if not email:
            continue
        bd_relevant = row.get("bd_relevant")
        # Only save contacts that have been classified (not None/Pending)
        if bd_relevant is not None:
            memory[email] = {
                "bd_relevant": bool(bd_relevant) if bd_relevant is not None else None,
                "contact_type": row.get("contact_type", ""),
                "stage": row.get("stage", ""),
                "ai_confidence": row.get("ai_confidence"),
                "ai_reasoning": row.get("ai_reasoning", ""),
                "ai_stage_reasoning": row.get("ai_stage_reasoning", ""),
                "next_step": row.get("next_step", ""),
                "client_name": row.get("client_name", ""),
                "classified_at": datetime.now(timezone.utc).isoformat(),
            }
    _save_memory(memory)


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
        "pipeline_summary": "",
        "pipeline_stage_filter": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    # Ensure all default internal domains are always present
    current = st.session_state.internal_domains.lower()
    for d in DEFAULT_INTERNAL_DOMAINS:
        if d not in current:
            st.session_state.internal_domains += f", {d}"


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


_CLIENT_NAME_OVERRIDES = {
    "cpaaustralia": "CPA Australia",
    "unimelb": "University of Melbourne",
    "monash": "Monash University",
    "anu": "Australian National University",
    "unsw": "UNSW Sydney",
    "usyd": "University of Sydney",
    "uts": "UTS",
    "rmit": "RMIT University",
    "deakin": "Deakin University",
    "latrobe": "La Trobe University",
    "swinburne": "Swinburne University",
    "anzsog": "ANZSOG",
    "csiro": "CSIRO",
    "abc": "ABC",
    "nab": "NAB",
    "anz": "ANZ",
    "cba": "Commonwealth Bank",
    "westpac": "Westpac",
    "kpmg": "KPMG",
    "ey": "EY",
    "pwc": "PwC",
    "deloitte": "Deloitte",
    "accenture": "Accenture",
    "ibm": "IBM",
    "microsoft": "Microsoft",
    "google": "Google",
    "amazon": "Amazon",
    "atlassian": "Atlassian",
    "canva": "Canva",
    "nbn": "NBN Co",
    "telstra": "Telstra",
    "optus": "Optus",
    "vic.gov": "Victorian Government",
    "health.gov": "Department of Health",
    "education.gov": "Department of Education",
    "defence.gov": "Department of Defence",
    "treasury.gov": "Treasury",
    "dynata": "Dynata",
    "lightspeedresearch": "Lightspeed Research",
    "toluna": "Toluna",
    "netsuite": "NetSuite",
    "salesforce": "Salesforce",
    "hubspot": "HubSpot",
}


def _domain_to_client(email) -> str:
    if not email or "@" not in email:
        return "Unknown"
    domain = email.split("@", 1)[1].lower()
    base = domain.split(".")[0]

    # Check exact match first
    if base in _CLIENT_NAME_OVERRIDES:
        return _CLIENT_NAME_OVERRIDES[base]

    # Check if domain (without TLD) matches any override key
    domain_no_tld = domain.rsplit(".", 1)[0] if "." in domain else domain
    for key, name in _CLIENT_NAME_OVERRIDES.items():
        if key in domain_no_tld:
            return name

    # Default: split on hyphens/dots and title-case
    return " ".join(part.capitalize() for part in base.replace("-", " ").replace("_", " ").split())


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


# Prefixes in the local part of the email address that indicate automated senders
_NOREPLY_PREFIXES = [
    "no-reply", "noreply", "do-not-reply", "donotreply",
    "no_reply", "notifications", "notification", "alert",
    "alerts", "mailer-daemon", "postmaster", "bounce",
    "auto-confirm", "info@", "news@", "updates@",
    "support@", "billing@", "invoice@", "receipt@",
    "payroll_manager", "security@", "admin@",
]

# Domains that are NEVER BD clients — automated systems, SaaS, vendors, fieldwork
_EXCLUDED_DOMAINS = [
    # Payroll / HR / finance systems
    "employmenthero.com", "liquidhrpayroll.com.au", "xero.com",
    "myob.com", "deputy.com", "keypay.com.au",
    # Software / SaaS platforms
    "netlify.com", "netlify.app", "github.com", "anthropic.com",
    "microsoft.com", "google.com", "apple.com", "amazon.com",
    "zoom.us", "slack.com", "canva.com", "adobe.com", "dropbox.com",
    "atlassian.com", "notion.so", "figma.com", "zapier.com",
    "mailchimp.com", "hubspot.com", "salesforce.com",
    "netsuite.com", "oracle.com", "sap.com",
    # Fieldwork / panel / research suppliers (provide services TO Forethought)
    "dynata.com", "lightspeedresearch.com", "pureprofile.com",
    "toluna.com", "theresearchshop.com.au", "qualtricsxm.com",
    "qualtrics.com", "cint.com", "lucidholdings.com",
    "prodege.com", "borderlesspanel.com",
    # Research industry — newsletters/promotions (not commissioning clients)
    "ipsos.com", "kantar.com", "nielsen.com", "gfk.com",
    "roymorgan.com.au",
    # Security / notifications / Microsoft
    "microsoftonline.com", "protection.outlook.com",
    "sharepointonline.com", "office365.com", "office.com",
    # Newsletters / digests
    "substack.com", "medium.com", "linkedin.com",
    "eventbrite.com", "meetup.com",
]

# Subject-line patterns that indicate automated / non-BD messages
_JUNK_SUBJECT_PATTERNS = [
    "pay slip", "payslip", "pay period", "payroll",
    "password reset", "security alert", "sign-in activity",
    "unusual sign-in", "suspicious activity",
    "verify your", "confirm your email", "secure link to log in",
    "daily digest", "weekly digest", "reaction daily",
    "your invoice", "your receipt", "your subscription",
    "out of office", "automatic reply", "auto-reply",
    "undeliverable", "delivery failure", "delivery status",
    "phd update", "thesis update", "thesis chapter",
    "sunday phd", "monday phd", "weekly phd",
    "new pay slip", "your payslip",
    "reaction daily digest", "reaction weekly",
    "what's on this month", "whats on this month",
    "building update", "building notice",
]


def _is_auto_excluded(email, latest_subject=""):
    """Return True if this contact should be auto-excluded as obviously not BD."""
    if not email:
        return True

    local_part = email.split("@", 1)[0].lower()
    domain = email.split("@", 1)[1].lower() if "@" in email else ""

    # Check no-reply style addresses
    for prefix in _NOREPLY_PREFIXES:
        if local_part.startswith(prefix.rstrip("@")):
            return True

    # Check excluded domains
    for excluded in _EXCLUDED_DOMAINS:
        if domain == excluded or domain.endswith("." + excluded):
            return True

    # Check subject patterns for automated messages
    subj_lower = (latest_subject or "").lower()
    for pattern in _JUNK_SUBJECT_PATTERNS:
        if pattern in subj_lower:
            return True

    return False


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
        cp = msg.get("counterparty_email")
        if cp:
            grouped.setdefault(cp, []).append(msg)

    rows = []
    for email, group in grouped.items():
        group.sort(
            key=lambda x: pd.to_datetime(x["datetime"], utc=True, errors="coerce"),
        )
        latest = group[-1]
        latest_subject = latest.get("subject", "")

        # Check if this contact is obviously non-BD (automated, system, etc.)
        auto_excluded = _is_auto_excluded(email, latest_subject)

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
            "stage": "Not BD" if auto_excluded else "Pending",
            "last_touch": latest.get("datetime"),
            "days_since_touch": _derive_days(latest.get("datetime")),
            "latest_subject": latest_subject,
            "next_step": "" if auto_excluded else "Run AI classification to determine stage.",
            "notes": latest.get("preview", ""),
            "thread_data": json.dumps(thread_data),
            "contact_type": "not_relevant" if auto_excluded else "",
            "bd_relevant": False if auto_excluded else None,
            "ai_confidence": 1.0 if auto_excluded else None,
            "ai_reasoning": "Auto-excluded: automated sender, system notification, or known non-client domain." if auto_excluded else "",
            "ai_stage_reasoning": "",
        })

    df = pd.DataFrame(rows)
    return df.sort_values(
        ["days_since_touch", "client_name"],
        ascending=[False, True],
        na_position="last",
    )


# ─── AI Classification (Anthropic Claude) ────────────────────────────────────

AI_RELEVANCE_SYSTEM = """You are an AI assistant for Forethought Outcomes, a market research and strategy consultancy based in Australia.

Forethought's CLIENTS are organisations (brands, government agencies, NFPs, corporations) that commission market research, insights, strategy, or consulting work from Forethought.

Your job: decide whether an email thread represents communication with a CLIENT or PROSPECTIVE CLIENT.

INCLUDE (bd_relevant = true):
- Current clients who have commissioned or are discussing research/consulting work
- Prospective clients Forethought is reaching out to about potential engagements
- Former clients who previously commissioned work (mark as former_client)
- Government agencies, universities, NFPs, corporates, or any other organisation that commissions (or could commission) research, insights, or consulting from Forethought
- Anyone Forethought is building a relationship with for potential future work, even if the conversation is early or informal (e.g. catch-ups, coffees, intros)
- When in doubt about whether a contact is a potential client, lean towards INCLUDING them — it is better to include a borderline contact than to miss a real one

EXCLUDE (bd_relevant = false) — these are NOT clients:
- FIELDWORK HOUSES and research panel providers (e.g. PureProfile, Lightspeed, Dynata, Qualtrics panels, The Research Shop, fieldwork agencies, anyone providing data collection, sample, panel recruitment, or survey hosting services TO Forethought)
- SOFTWARE and SAAS providers (e.g. Microsoft, NetSuite, Adobe, Canva, Zoom, Slack, any platform/tool Forethought uses)
- IT support, security alerts, system notifications (e.g. Microsoft Security, password resets, account alerts)
- RECRUITMENT contacts, job applicants, staffing agencies
- OTHER SUPPLIERS or vendors providing services TO Forethought (design agencies, printers, accountants, lawyers, office suppliers)
- Promotional emails, newsletters, marketing where there is no personal relationship or BD conversation
- Automated notifications from any system

KEY TEST: Is this person/organisation someone who PAYS (or could pay) Forethought for research, insights, or consulting services? If they provide services TO Forethought, or are a platform/tool, they are NOT a client. But if they COULD be a buyer of Forethought's services (including government, universities, NFPs, corporates), include them.

Respond with ONLY a JSON object, no other text."""

AI_STAGE_SYSTEM = """You are an AI assistant for Forethought Outcomes, a market research and strategy consultancy.

You are classifying the BD pipeline stage of a thread that is confirmed as client-relevant.

The stages are:
1. Outreach - Initial contact or re-engagement. Includes cold outreach, intro emails, LinkedIn follow-ups, or reconnecting with a dormant contact. No substantive two-way conversation yet.
2. In Conversation - Active two-way engagement. Includes catch-ups, coffees, meetings (proposed or confirmed), general relationship building, discussing potential needs. No formal proposal or scope yet.
3. Proposal - A proposal, scope, fee estimate, or pricing discussion is underway. The client has asked for a proposal OR one has been sent. This stage covers from "can you put something together" through to "here's our proposal."
4. Commissioned - The client has explicitly agreed to proceed. The work has been confirmed, signed off, or a PO/agreement is in place. Only use this if the email shows clear commissioning language.
5. Active Project - Ongoing delivery of commissioned work. Project management, deliverable discussions, status updates, data sharing for an active engagement. This is post-commissioning.
6. Dormant - The thread has gone quiet. Previous engagement existed but no recent activity or the conversation stalled without resolution.
7. Closed - The opportunity was explicitly declined, lost, or deferred with no near-term prospect.

IMPORTANT:
- A "coffee", "catch up", "chat", or "meeting" is "In Conversation", not Proposal.
- "Proposal" requires actual discussion of scope, fees, deliverables, or a formal document.
- "Commissioned" requires explicit agreement to proceed — not just interest.
- "Active Project" is for emails about work already underway, not new BD.
- If the last message is old and unanswered, consider "Dormant".

Respond with ONLY a JSON object, no other text."""


def _call_claude(system_prompt, user_prompt, max_tokens=512, retries=2):
    """Call Anthropic Messages API with retry and rate-limit handling."""
    if not ANTHROPIC_API_KEY:
        return None

    for attempt in range(retries + 1):
        try:
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": max_tokens,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_prompt}],
                },
                timeout=60,
            )

            if resp.status_code == 200:
                data = resp.json()
                content = data.get("content", [])
                if content:
                    return content[0].get("text", "")
                return ""

            # Rate limited or overloaded — wait and retry
            if resp.status_code in (429, 529) and attempt < retries:
                wait = float(resp.headers.get("retry-after", 3 * (attempt + 1)))
                log.warning("Rate limited (attempt %d/%d), waiting %.1fs", attempt + 1, retries + 1, wait)
                time.sleep(wait)
                continue

            log.warning("Claude API error %s: %s", resp.status_code, resp.text[:300])
            return None

        except requests.exceptions.Timeout:
            if attempt < retries:
                log.warning("Timeout (attempt %d/%d), retrying...", attempt + 1, retries + 1)
                time.sleep(2)
                continue
            return None
        except Exception as exc:
            log.warning("Claude API exception: %s", exc)
            return None

    return None


def _parse_json_response(text):
    """Extract JSON from a Claude response, handling markdown fences."""
    if not text:
        return None
    text = text.strip()
    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()
    # Try parsing as-is
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try to find JSON array or object within the text
    for start_char, end_char in [("[", "]"), ("{", "}")]:
        start = text.find(start_char)
        end = text.rfind(end_char)
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                continue
    log.warning("Failed to parse JSON from Claude: %s", text[:300])
    return None


AI_BATCH_SYSTEM = AI_RELEVANCE_SYSTEM + """

For contacts that ARE bd_relevant, also classify their pipeline stage. Use the STRICT rules below. You must follow these rules exactly — do not use your own judgment to override them.

=== STAGE CLASSIFICATION RULES ===

1. OUTREACH
   USE WHEN: A message has been sent TO the contact but they have NOT replied, OR this is a first-time/re-engagement contact with no two-way exchange yet.
   EXAMPLES: cold email sent, intro email sent, LinkedIn follow-up sent, re-engagement after a long gap with no reply.
   NEVER USE WHEN: The contact has replied with substantive content.

2. IN CONVERSATION
   USE WHEN: There is a two-way exchange (both parties have sent messages) AND the discussion is about relationship building, general catch-ups, exploring needs, or meetings — but NO mention of scope, fees, proposals, or deliverables.
   EXAMPLES: "Let's grab a coffee", "Great to meet you at the conference", scheduling a catch-up, discussing potential needs at a high level, "I'd love to hear more about what you do."
   NEVER USE WHEN: There is any mention of a proposal, scope, pricing, fee estimate, or formal deliverables.

3. PROPOSAL
   USE WHEN: The email thread contains ANY of these keywords or concepts: "proposal", "scope", "fee estimate", "quote", "pricing", "budget", "cost", "deliverables", "put something together", "send through a proposal", "scope of work", "terms", "how much would it cost". This includes both the client asking for a proposal AND Forethought having sent one.
   EXAMPLES: "Can you send a proposal?", "Here's our scope and fee estimate", "What would this cost?", "I've attached the proposal", "Let me put something together for you."
   ALWAYS USE WHEN: Any discussion of money, pricing, scope documents, or formal project outlines — even if informal.

4. COMMISSIONED
   USE WHEN: The client has explicitly agreed to proceed. Look for: "Let's go ahead", "We'd like to proceed", "Approved", "PO attached", "Contract signed", "Please start", "We've got budget approval."
   NEVER USE WHEN: The proposal has been sent but no explicit confirmation of acceptance.

5. ACTIVE PROJECT
   USE WHEN: Work is underway. The emails discuss deliverables being worked on, status updates, data being shared for analysis, draft reports, fieldwork progress, presentation scheduling.
   EXAMPLES: "Here's the latest data", "Draft report attached", "Fieldwork update", "Presentation next Thursday."
   NEVER USE WHEN: The project hasn't started yet — use Commissioned instead.

6. DORMANT
   USE WHEN: There was previous engagement but the most recent message is more than 21 days old with no reply, OR the conversation visibly stalled (one party stopped responding).
   NEVER USE WHEN: The last message is less than 21 days old — use the appropriate active stage instead.

7. CLOSED
   USE WHEN: The opportunity was explicitly declined or lost. Look for: "We've decided to go another direction", "Not proceeding", "Budget was cut", "Maybe next year."
   NEVER USE WHEN: The thread is simply quiet — that's Dormant, not Closed.

=== END RULES ===

Apply these rules mechanically based on the email content. The same email content must always produce the same stage classification.

Respond with ONLY a JSON array, no other text."""

BATCH_SIZE = 5


def _build_thread_summary(thread_data_json):
    """Build a text summary from thread data."""
    thread_data = json.loads(thread_data_json) if isinstance(thread_data_json, str) else thread_data_json
    summary = ""
    for msg in thread_data:
        direction = "SENT" if msg["direction"] == "outbound" else "RECEIVED"
        summary += f"[{direction}] {msg['date']}\n"
        summary += f"Subject: {msg['subject']}\n"
        summary += f"Preview: {msg['preview']}\n\n"
    return summary, len(thread_data)


def _classify_batch(batch_rows):
    """Classify a batch of contacts in a single API call."""
    contacts_text = ""
    for i, (idx, row) in enumerate(batch_rows):
        domain = row["counterparty_email"].split("@", 1)[1] if "@" in str(row.get("counterparty_email", "")) else ""
        thread_summary, msg_count = _build_thread_summary(row.get("thread_data", "[]"))
        contacts_text += f"""--- CONTACT {i + 1} ---
Contact: {row.get('contact_name', '')}
Email: {row.get('counterparty_email', '')}
Domain: {domain}

Thread ({msg_count} messages):
{thread_summary}
"""

    stages_str = ", ".join(STAGE_ORDER)

    user_prompt = f"""Classify these {len(batch_rows)} email threads.

{contacts_text}

For EACH contact, respond with a JSON object inside a JSON array. Each object must have:
{{
  "contact_index": 1-based index matching the CONTACT number above,
  "bd_relevant": true or false,
  "contact_type": "current_client" | "prospective_client" | "former_client" | "not_relevant",
  "confidence": 0.0 to 1.0,
  "reasoning": "One or two sentences on why this is or is not BD relevant.",
  "stage": "one of: {stages_str}" (only if bd_relevant is true, otherwise "Not BD"),
  "stage_reasoning": "One or two sentences on why this stage was chosen." (only if bd_relevant),
  "next_step": "A specific, actionable next step." (only if bd_relevant, otherwise ""),
  "proper_company_name": "The correct, properly formatted business name (e.g. 'CPA Australia' not 'Cpaaustralia', 'University of Melbourne' not 'Unimelb', 'ANZSOG' not 'Anzsog'). Use standard capitalisation and the name the organisation is commonly known by."
}}

Respond with ONLY the JSON array, no other text."""

    text = _call_claude(AI_BATCH_SYSTEM, user_prompt, max_tokens=4096)
    if not text:
        log.warning("Batch classification returned no text")
        return None
    result = _parse_json_response(text)
    if result is None:
        log.warning("Batch JSON parse failed. Raw response: %s", text[:500])
    return result


def run_ai_classification(df, progress_callback=None):
    """Run AI classification on all contacts using batched API calls."""
    if df.empty or not ANTHROPIC_API_KEY:
        return df

    # Collect contacts that need classification
    # Skip: auto-excluded AND already classified (from memory)
    to_classify = []
    for idx, row in df.iterrows():
        # Skip auto-excluded
        if row.get("bd_relevant") is False and str(row.get("ai_reasoning", "")).startswith("Auto-excluded"):
            continue
        # Skip already classified (restored from memory or previous run)
        if row.get("bd_relevant") is not None:
            continue
        to_classify.append((idx, row))

    total = len(to_classify)
    if total == 0:
        # Nothing new to classify — save memory and return
        _update_memory(df)
        return df

    # Process in batches
    batch_errors = []
    for batch_start in range(0, total, BATCH_SIZE):
        batch = to_classify[batch_start:batch_start + BATCH_SIZE]
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch_num = batch_start // BATCH_SIZE + 1

        if progress_callback:
            names = ", ".join(r["contact_name"] or r["counterparty_email"] for _, r in batch)
            progress_callback(batch_end / total, f"Classifying batch {batch_num} ({batch_end}/{total}): {names[:80]}")

        # Pace between batches
        if batch_start > 0:
            time.sleep(2)

        results = _classify_batch(batch)

        if results and isinstance(results, list):
            matched = 0
            for result in results:
                ci = result.get("contact_index", 0) - 1
                if 0 <= ci < len(batch):
                    idx = batch[ci][0]
                    bd_relevant = result.get("bd_relevant", False)
                    df.at[idx, "bd_relevant"] = bd_relevant
                    df.at[idx, "contact_type"] = result.get("contact_type", "not_relevant")
                    df.at[idx, "ai_confidence"] = result.get("confidence", 0.0)
                    df.at[idx, "ai_reasoning"] = result.get("reasoning", "")

                    # Update client name if AI returned a proper company name
                    proper_name = result.get("proper_company_name", "")
                    if proper_name and proper_name.strip():
                        df.at[idx, "client_name"] = proper_name.strip()

                    if bd_relevant:
                        df.at[idx, "stage"] = result.get("stage", "Pending")
                        df.at[idx, "ai_stage_reasoning"] = result.get("stage_reasoning", "")
                        df.at[idx, "next_step"] = result.get("next_step", "")
                    else:
                        df.at[idx, "stage"] = "Not BD"
                        df.at[idx, "next_step"] = ""
                    matched += 1
            if matched == 0:
                batch_errors.append(f"Batch {batch_num}: got {len(results)} results but none matched contact indices")
        else:
            batch_errors.append(f"Batch {batch_num}: API returned no usable results")
            for idx, row in batch:
                df.at[idx, "ai_reasoning"] = f"Batch {batch_num} failed — contact left unclassified."

    # Save all classifications to memory for future sessions
    _update_memory(df)

    # Surface any errors via session state so the UI can show them
    if batch_errors:
        st.session_state["_classify_errors"] = batch_errors

    return df


def generate_pipeline_summary(df):
    """Generate an AI-written narrative summary of the BD pipeline state."""
    if df.empty or not ANTHROPIC_API_KEY:
        return ""

    # Build a concise data snapshot for Claude
    bd_df = df[df["bd_relevant"].fillna(False).astype(bool)].copy() if "bd_relevant" in df.columns else df.copy()

    if bd_df.empty:
        return "No BD-relevant contacts found after classification."

    total = len(bd_df)
    stage_counts = {}
    stage_contacts = {}
    for stage in STAGE_ORDER:
        matches = bd_df[bd_df["stage"] == stage]
        count = len(matches)
        stage_counts[stage] = count
        if count > 0:
            names = []
            for _, r in matches.iterrows():
                client = r.get("client_name", "")
                contact = r.get("contact_name", "")
                days = r.get("days_since_touch", "?")
                names.append(f"{client} ({contact}, {days}d ago)")
            stage_contacts[stage] = names

    snapshot = f"Total BD-relevant contacts: {total}\n\n"
    for stage in STAGE_ORDER:
        count = stage_counts[stage]
        if count > 0:
            snapshot += f"{stage} ({count}):\n"
            for name in stage_contacts[stage]:
                snapshot += f"  - {name}\n"
            snapshot += "\n"

    system_prompt = """You are a reporting assistant for Forethought Outcomes, a market research and strategy consultancy.

Write a brief, factual pipeline summary (3-5 sentences) based purely on the data provided. Be specific — mention actual client names and their current stages. Report only what the data shows:
- How many BD-relevant contacts there are and how they are distributed across stages
- Which contacts have the longest time since last touch
- Which contacts are at the proposal or commissioned stage
- Any contacts where there has been no activity for an extended period

IMPORTANT: Be strictly factual. Do NOT make judgments, opinions, or assessments like "concerning", "lacks depth", "impressive", "needs improvement", "healthy", or "at risk". Simply report the facts and let the reader draw their own conclusions.

Write in a neutral, professional tone. No bullet points — flowing prose only. Do not use any markdown formatting."""

    user_prompt = f"""Here is the current BD pipeline snapshot after AI classification:\n\n{snapshot}\n\nWrite a concise pipeline summary."""

    try:
        # Wait before summary call to avoid rate limiting after classification
        time.sleep(3)
        text = _call_claude(system_prompt, user_prompt, max_tokens=1024, retries=3)
        if text and text.strip():
            return text.strip()
        return "Pipeline summary could not be generated — the AI returned an empty response. This is usually due to API rate limiting. Try clicking Classify AI again."
    except Exception as exc:
        log.warning("Pipeline summary generation failed: %s", exc)
        return f"Pipeline summary could not be generated — {exc}"


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
    df = build_tracker(messages, owner)
    # Restore any previous classifications from memory
    df = _apply_memory(df)
    st.session_state.tracker_df = df
    st.session_state.last_sync = datetime.now(timezone.utc)
    # If memory restored some classifications, keep last_classify set
    has_classified = "bd_relevant" in df.columns and df["bd_relevant"].notna().any()
    if not has_classified:
        st.session_state.last_classify = None
    st.session_state.pipeline_summary = ""


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

def apply_filters(df, search, stage, sort, show_excluded, date_from=None, date_to=None):
    if df.empty:
        return df

    filtered = df.copy()

    # Hide non-BD contacts unless "Show excluded" is checked
    if not show_excluded and "bd_relevant" in filtered.columns:
        # Always hide contacts explicitly marked as not relevant (False)
        # Always show contacts marked as relevant (True) or unclassified (None)
        filtered = filtered[filtered["bd_relevant"].fillna(True).astype(bool)]

    # Date range filter on last_touch
    if date_from and "last_touch" in filtered.columns:
        filtered["_touch_date"] = pd.to_datetime(filtered["last_touch"], errors="coerce").dt.date
        filtered = filtered[filtered["_touch_date"] >= date_from]
        if date_to:
            filtered = filtered[filtered["_touch_date"] <= date_to]
        filtered = filtered.drop(columns=["_touch_date"])

    if search:
        mask = filtered.astype(str).apply(
            lambda col: col.str.contains(search, case=False, na=False),
        )
        filtered = filtered[mask.any(axis=1)]

    if stage != "All stages":
        filtered = filtered[filtered["stage"] == stage]

    sort_map = {
        "Most recent": ("last_touch", False),
        "Oldest first": ("last_touch", True),
        "Client A\u2013Z": ("client_name", True),
        "Stage": ("stage", True),
    }
    col, asc = sort_map.get(sort, ("last_touch", False))
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
            f'<div class="nav-brand">{_logo_img("32px")}</div>',
            unsafe_allow_html=True,
        )

    with right:
        c1, c2, c3 = st.columns([3, 3, 2])
        with c1:
            if st.button("Sync Outlook", type="primary", use_container_width=True):
                with st.spinner("Syncing\u2026"):
                    try:
                        sync_outlook()
                    except Exception as exc:
                        st.error(str(exc))
        with c2:
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
                    progress.progress(1.0, text="Generating pipeline summary\u2026")
                    st.session_state.pipeline_summary = generate_pipeline_summary(
                        st.session_state.tracker_df,
                    )
                    st.session_state.last_classify = datetime.now(timezone.utc)
                    progress.empty()
                    # Show any batch errors
                    errors = st.session_state.pop("_classify_errors", [])
                    if errors:
                        st.warning("Some batches had issues: " + "; ".join(errors))
                except Exception as exc:
                    progress.empty()
                    st.error(f"AI classification error: {exc}")
        with c3:
            if st.button("Sign out", use_container_width=True):
                sign_out()

    st.markdown('<hr class="subtle-divider">', unsafe_allow_html=True)

    with st.expander("Settings"):
        new_domains = st.text_input(
            "Internal domains (comma-separated)",
            value=st.session_state.internal_domains,
            help="Email domains to exclude as internal.",
        )
        if new_domains != st.session_state.internal_domains:
            st.session_state.internal_domains = new_domains

        if st.button("Clear classification memory", help="Force all contacts to be re-classified on next Classify AI run"):
            _save_memory({})
            # Reset all classifications in current DataFrame
            df = st.session_state.tracker_df
            if not df.empty:
                for idx in df.index:
                    if str(df.at[idx, "ai_reasoning"]).startswith("Auto-excluded"):
                        continue
                    df.at[idx, "bd_relevant"] = None
                    df.at[idx, "contact_type"] = ""
                    df.at[idx, "stage"] = "Pending"
                    df.at[idx, "ai_confidence"] = None
                    df.at[idx, "ai_reasoning"] = ""
                    df.at[idx, "ai_stage_reasoning"] = ""
                    df.at[idx, "next_step"] = "Run AI classification to determine stage."
                st.session_state.tracker_df = df
            st.session_state.last_classify = None
            st.session_state.pipeline_summary = ""
            st.success("Classification memory cleared. Click Classify AI to re-classify all contacts.")


def render_pipeline_bar(df):
    # Build stage definitions legend items
    legend_items = ""
    for i, stage in enumerate(STAGE_ORDER):
        fg = STAGE_STYLES[stage][0]
        defn = _esc(STAGE_DEFINITIONS.get(stage, ""))
        num = i + 1
        arrow = ' <span class="stage-legend-arrow">↓</span>' if i < len(STAGE_ORDER) - 1 else ""
        legend_items += (
            f'<div class="stage-legend-item">'
            f'<span class="stage-legend-num">{num}</span>'
            f'<span class="stage-legend-name" style="color:{fg};">{_esc(stage)}</span>'
            f'<span class="stage-legend-desc">{defn}</span>'
            f'{arrow}'
            f'</div>'
        )

    # Pipeline header + stage definitions as a single block
    # The <details> sits BELOW the title row so it expands full-width
    st.markdown(
        f'<div class="pipeline-header-block">'
        f'<div class="pipeline-header-row">'
        f'<span class="section-header" style="margin-bottom:0;">BD Pipeline Overview</span>'
        f'</div>'
        f'<details class="stage-legend">'
        f'<summary>Stage definitions</summary>'
        f'<div class="stage-legend-grid">{legend_items}</div>'
        f'</details>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Pipeline stage cards (visual only — use stage dropdown in filter bar to filter)
    blocks = []
    for stage in STAGE_ORDER:
        count = int((df["stage"] == stage).sum()) if not df.empty else 0
        fg, bg, border = STAGE_STYLES[stage]
        active = st.session_state.get("pipeline_stage_filter") == stage
        active_style = "outline:2px solid #5ec6c1;outline-offset:2px;" if active else ""
        zero_class = " zero" if count == 0 and not active else ""
        blocks.append(
            f'<div class="pipeline-stage{zero_class}" '
            f'style="border-color:{border};background:{bg};{active_style}">'
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
    in_convo = int((df["stage"] == "In Conversation").sum()) if not df.empty else 0
    proposals = int((df["stage"] == "Proposal").sum()) if not df.empty else 0
    active = int(
        df["stage"].isin(["Commissioned", "Active Project"]).sum()
    ) if not df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Active Contacts", total)
    c2.metric("In Conversation", in_convo)
    c3.metric("Proposals", proposals)
    c4.metric("Commissioned / Active", active)


def render_filter_bar(df):
    today = datetime.now(MELB_TZ).date()
    default_from = today - timedelta(days=30)

    c1, c2, c3, c4, c5 = st.columns([3, 1.5, 1.5, 2, 1.5])
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
                "Most recent",
                "Oldest first",
                "Client A\u2013Z",
                "Stage",
            ],
            label_visibility="collapsed",
        )
    with c4:
        date_selection = st.date_input(
            "Date range",
            value=(default_from, today),
            max_value=today,
            key="date_range",
            label_visibility="collapsed",
        )
    with c5:
        show_excluded = st.checkbox("Show excluded", key="show_excluded")

    # Extract from/to from the date picker (returns tuple when range selected)
    if isinstance(date_selection, (list, tuple)):
        if len(date_selection) == 2:
            date_from, date_to = date_selection
        elif len(date_selection) == 1:
            date_from, date_to = date_selection[0], today
        else:
            date_from, date_to = None, today
    else:
        date_from, date_to = date_selection, today

    return search, stage, sort, show_excluded, date_from, date_to


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
            f'<div class="contact-card-pills">'
            f'{_contact_type_pill(contact_type)}'
            f'{_pill_html(stage)}'
            f'{_days_html(days)}'
            f'</div>'
            f'<div class="contact-card-date">{date_str}</div>'
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


def render_pipeline_summary():
    summary = st.session_state.get("pipeline_summary", "")
    if not summary:
        return
    summary_html = (
        '<div class="pipeline-summary">'
        '<div class="pipeline-summary-header">'
        '<span class="pipeline-summary-icon">\U0001F4A1</span>'
        '<span class="pipeline-summary-title">Pipeline Insight</span>'
        '</div>'
        f'<div class="pipeline-summary-text">{_esc(summary)}</div>'
        '</div>'
    )
    st.markdown(summary_html, unsafe_allow_html=True)


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
    render_pipeline_summary()

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

    search, stage, sort, show_excluded, date_from, date_to = render_filter_bar(df)
    # Pipeline bar click overrides dropdown stage filter
    pipe_filter = st.session_state.get("pipeline_stage_filter")
    if pipe_filter:
        stage = pipe_filter
    filtered = apply_filters(df, search, stage, sort, show_excluded, date_from, date_to)

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    render_contact_cards(filtered)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    render_csv_export(filtered)

    # Status footer
    parts = []
    if st.session_state.last_sync:
        melb_sync = st.session_state.last_sync.astimezone(MELB_TZ)
        parts.append(f"Synced: {melb_sync.strftime('%d %b %H:%M AEST')}")
    if st.session_state.last_classify:
        melb_cls = st.session_state.last_classify.astimezone(MELB_TZ)
        parts.append(f"Classified: {melb_cls.strftime('%d %b %H:%M AEST')}")
    if parts:
        st.markdown(
            f'<div class="sample-notice">{" \u00b7 ".join(parts)}</div>',
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()
