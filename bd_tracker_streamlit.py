"""
BD Tracker -- Business Development Command Centre
Connects to Outlook via Microsoft Graph, tracks client outreach,
and presents a clean pipeline dashboard.
"""

from __future__ import annotations

import html
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import msal
import pandas as pd
import requests
import streamlit as st

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
    CLIENT_ID = "74a06330-3a89-4cf8-871d-9d783c483d9d"  # fallback for local dev

try:
    TENANT_ID = st.secrets["TENANT_ID"]
except Exception:
    TENANT_ID = "a14b16a4-0cbe-435c-a893-78e3e95b09c3"  # fallback for local dev
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

STAGE_STYLES: dict[str, tuple[str, str, str]] = {
    "Outreach Sent":     ("#b9d1ff", "rgba(110,168,254,0.10)", "rgba(110,168,254,0.22)"),
    "Follow-up Needed":  ("#ffe4a8", "rgba(255,212,121,0.10)", "rgba(255,212,121,0.22)"),
    "Engaged":           ("#bbffe3", "rgba(126,240,194,0.10)", "rgba(126,240,194,0.22)"),
    "Meeting Proposed":  ("#ddd1ff", "rgba(176,151,255,0.11)", "rgba(176,151,255,0.22)"),
    "Meeting Booked":    ("#c3fbff", "rgba(93,224,230,0.12)",  "rgba(93,224,230,0.22)"),
    "Proposal Requested":("#ffd0d0", "rgba(255,143,143,0.10)", "rgba(255,143,143,0.22)"),
    "Proposal Sent":     ("#ffe0ba", "rgba(255,173,90,0.11)",  "rgba(255,173,90,0.22)"),
    "Commissioned":      ("#d6ffd8", "rgba(117,243,128,0.11)", "rgba(117,243,128,0.22)"),
    "Closed / Not Now":  ("#dde4f1", "rgba(155,166,190,0.12)", "rgba(155,166,190,0.22)"),
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
    },
    {
        "client_name": "Pacific Retail Group",
        "contact_name": "Tom Nguyen",
        "counterparty_email": "tom.nguyen@pacificretail.com.au",
        "owner": "Caleb",
        "stage": "Outreach Sent",
        "last_touch": "2026-03-31T08:45:00Z",
        "days_since_touch": 1,
        "latest_subject": "Intro — Forethought x Pacific Retail",
        "next_step": "Monitor for reply or send a follow-up in a few days.",
        "notes": "Cold outreach sent. No response yet.",
    },
]


# ─── CSS loader ───────────────────────────────────────────────────────────────

def load_css() -> None:
    """Load external stylesheet and inject into the page."""
    css_path = Path(__file__).parent / "style.css"
    if css_path.exists():
        css_text = css_path.read_text(encoding="utf-8")
    else:
        css_text = ""
    st.markdown(f"<style>{css_text}</style>", unsafe_allow_html=True)


# ─── Session state ────────────────────────────────────────────────────────────

def init_state() -> None:
    defaults: dict = {
        "access_token": None,
        "tracker_df": pd.DataFrame(),
        "authenticated": False,
        "device_flow": None,
        "auth_message": "",
        "user_code": "",
        "auth_result": None,
        "account_label": "",
        "last_sync": None,
        "internal_domains": ", ".join(DEFAULT_INTERNAL_DOMAINS),
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
    st.session_state.last_sync = None


# ─── Microsoft Graph helpers ─────────────────────────────────────────────────

def _graph_get(path: str, params: dict | None = None) -> dict:
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


def _email_addr(obj: dict | None) -> str | None:
    try:
        return obj["emailAddress"]["address"].lower()  # type: ignore[index]
    except Exception:
        return None


def _email_name(obj: dict | None) -> str:
    try:
        return obj["emailAddress"]["name"]  # type: ignore[index]
    except Exception:
        return ""


def _domain_to_client(email: str | None) -> str:
    if not email or "@" not in email:
        return "Unknown"
    domain = email.split("@", 1)[1]
    base = domain.split(".")[0]
    return " ".join(part.capitalize() for part in base.replace("-", " ").split())


def _is_internal(email: str | None, internal_domains: list[str]) -> bool:
    if not email:
        return True
    lower = email.lower()
    return any(lower.endswith(f"@{d}") for d in internal_domains)


# ─── Email fetching & normalisation ──────────────────────────────────────────

def fetch_messages(limit: int = 100) -> tuple[list[dict], list[dict]]:
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


def normalise_messages(
    messages: list[dict], box: str, internal_domains: list[str],
) -> list[dict]:
    rows: list[dict] = []
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


# ─── Stage inference ──────────────────────────────────────────────────────────

def _derive_days(date_str: str | None) -> int | None:
    if not date_str:
        return None
    dt = pd.to_datetime(date_str, utc=True, errors="coerce")
    if pd.isna(dt):
        return None
    return max(0, (datetime.now(timezone.utc) - dt.to_pydatetime()).days)


def _infer_stage(
    last_out: str | None, last_in: str | None, subject: str, preview: str,
) -> str:
    text = f"{subject or ''} {preview or ''}".lower()
    if any(w in text for w in ["proposal", "scope", "pricing", "quote", "fee"]):
        if any(w in text for w in ["attached", "proposal attached", "send through"]):
            return "Proposal Sent"
        return "Proposal Requested"
    if any(w in text for w in [
        "meet", "catch up", "catch-up", "calendar", "invite", "booked", "confirmed",
    ]):
        if any(w in text for w in ["invite", "booked", "confirmed"]):
            return "Meeting Booked"
        return "Meeting Proposed"
    if last_out and (
        not last_in
        or pd.to_datetime(last_in, utc=True) < pd.to_datetime(last_out, utc=True)
    ):
        days = _derive_days(last_out) or 0
        return "Follow-up Needed" if days >= FOLLOW_UP_DAYS else "Outreach Sent"
    return "Engaged"


def _suggest_next_step(stage: str) -> str:
    return {
        "Outreach Sent":     "Monitor for reply or send a follow-up in a few days.",
        "Follow-up Needed":  "Send a short follow-up and propose a next step.",
        "Engaged":           "Keep momentum with a relevant next touchpoint.",
        "Meeting Proposed":  "Confirm a time and send a calendar invite.",
        "Meeting Booked":    "Prepare an agenda and discussion points.",
        "Proposal Requested":"Draft a proposal outline and indicative fee range.",
        "Proposal Sent":     "Follow up on proposal feedback and timing.",
        "Commissioned":      "Confirm scope and kick off delivery.",
        "Closed / Not Now":  "Archive and revisit in a future quarter.",
    }.get(stage, "Review thread and decide next step.")


# ─── Tracker builder ─────────────────────────────────────────────────────────

def build_tracker(messages: list[dict], owner: str) -> pd.DataFrame:
    cols = [
        "client_name", "contact_name", "counterparty_email", "owner",
        "stage", "last_touch", "days_since_touch", "latest_subject",
        "next_step", "notes",
    ]
    if not messages:
        return pd.DataFrame(columns=cols)

    grouped: dict[str, list[dict]] = {}
    for msg in messages:
        grouped.setdefault(msg["counterparty_email"], []).append(msg)

    rows: list[dict] = []
    for email, group in grouped.items():
        group.sort(
            key=lambda x: pd.to_datetime(x["datetime"], utc=True, errors="coerce"),
        )
        inbound = [m for m in group if m["direction"] == "inbound"]
        outbound = [m for m in group if m["direction"] == "outbound"]
        latest = group[-1]
        last_in = inbound[-1]["datetime"] if inbound else None
        last_out = outbound[-1]["datetime"] if outbound else None
        stage = _infer_stage(
            last_out, last_in,
            latest.get("subject", ""), latest.get("preview", ""),
        )
        rows.append({
            "client_name": _domain_to_client(email),
            "contact_name": latest.get("contact_name", ""),
            "counterparty_email": email,
            "owner": owner,
            "stage": stage,
            "last_touch": latest.get("datetime"),
            "days_since_touch": _derive_days(latest.get("datetime")),
            "latest_subject": latest.get("subject", ""),
            "next_step": _suggest_next_step(stage),
            "notes": latest.get("preview", ""),
        })

    df = pd.DataFrame(rows)
    return df.sort_values(
        ["days_since_touch", "client_name"],
        ascending=[False, True],
        na_position="last",
    )


# ─── Sync action ─────────────────────────────────────────────────────────────

def sync_outlook() -> None:
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
    owner = (
        st.session_state.account_label.split(" (")[0]
        if st.session_state.account_label else "Me"
    )
    st.session_state.tracker_df = build_tracker(messages, owner)
    st.session_state.last_sync = datetime.now(timezone.utc)


# ─── HTML helpers ─────────────────────────────────────────────────────────────

def _esc(text: str | None) -> str:
    """HTML-escape a string."""
    return html.escape(str(text)) if text else ""


def _pill_html(stage: str) -> str:
    fg, bg, border = STAGE_STYLES.get(
        stage, ("#b9d1ff", "rgba(110,168,254,0.10)", "rgba(110,168,254,0.22)"),
    )
    return (
        f'<span class="pill" style="color:{fg};background:{bg};'
        f'border-color:{border};">{_esc(stage)}</span>'
    )


def _days_html(days: int | None) -> str:
    if days is None:
        return ""
    cls = "days-badge days-urgent" if days >= FOLLOW_UP_DAYS else "days-badge days-normal"
    label = f"{days}d ago" if days > 0 else "Today"
    return f'<span class="{cls}">{label}</span>'


# ─── Filter logic ─────────────────────────────────────────────────────────────

def apply_filters(
    df: pd.DataFrame, search: str, stage: str, sort: str,
) -> pd.DataFrame:
    if df.empty:
        return df

    filtered = df.copy()

    if search:
        mask = filtered.astype(str).apply(
            lambda col: col.str.contains(search, case=False, na=False),
        )
        filtered = filtered[mask.any(axis=1)]

    if stage != "All stages":
        filtered = filtered[filtered["stage"] == stage]

    sort_map = {
        "Days since touch (newest)": ("days_since_touch", True),
        "Days since touch (oldest)": ("days_since_touch", False),
        "Client A\u2013Z": ("client_name", True),
        "Stage": ("stage", True),
    }
    col, asc = sort_map.get(sort, ("days_since_touch", True))
    # For "newest" we actually want descending days (14 before 1)
    if sort == "Days since touch (newest)":
        asc = False
    filtered = filtered.sort_values(col, ascending=asc, na_position="last")

    return filtered


# ═══════════════════════════════════════════════════════════════════════════════
#  RENDER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def _brand_html() -> str:
    """Forethought brand mark as inline HTML."""
    return (
        '<div class="nav-brand">'
        '<div class="nav-brand-icon">F</div>'
        '<div class="nav-brand-text">Forethought'
        '<span class="accent">Outcomes</span></div>'
        '</div>'
    )


def render_auth_screen() -> None:
    """Centered authentication card with device-code flow."""
    _, center, _ = st.columns([1.3, 1.8, 1.3])

    with center:
        if not st.session_state.device_flow:
            st.markdown(
                f"""
                <div class="auth-outer">
                <div class="auth-card">
                    <div class="auth-logo">
                        <div class="auth-logo-icon">F</div>
                        <div class="auth-logo-text">Forethought</div>
                    </div>
                    <div class="auth-title">BD Tracker</div>
                    <div class="auth-subtitle">
                        Connect your Microsoft account to sync Outlook
                        and track client business development activity.
                    </div>
                </div>
                </div>
                """,
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
                f"""
                <div class="auth-outer">
                <div class="auth-card">
                    <div class="auth-logo">
                        <div class="auth-logo-icon">F</div>
                        <div class="auth-logo-text">Forethought</div>
                    </div>
                    <div class="auth-title">Enter the code below</div>
                    <div class="auth-subtitle">
                        Go to the Microsoft login page, enter this code,
                        and complete sign-in. Then return here.
                    </div>
                    <div class="device-code-display">{_esc(code)}</div>
                    <div class="auth-instruction">
                        Open
                        <a href="https://microsoft.com/devicelogin"
                           target="_blank">microsoft.com/devicelogin</a>
                        and enter the code above.
                    </div>
                </div>
                </div>
                """,
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


def render_top_nav() -> None:
    """Top navigation bar: brand left, sync + sign out right."""
    left, mid, right = st.columns([2.5, 3, 2])

    with left:
        st.markdown(_brand_html(), unsafe_allow_html=True)

    with mid:
        st.markdown(
            f'<div class="nav-account" style="text-align:center;">'
            f'{_esc(st.session_state.account_label)}</div>',
            unsafe_allow_html=True,
        )

    with right:
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Sync Outlook", type="primary", use_container_width=True):
                with st.spinner("Syncing\u2026"):
                    try:
                        sync_outlook()
                        st.rerun()
                    except Exception as exc:
                        st.error(str(exc))
        with c2:
            if st.button("Sign out", use_container_width=True):
                sign_out()
                st.rerun()

    st.markdown('<hr class="subtle-divider">', unsafe_allow_html=True)

    # Settings as a collapsible row below nav (not crammed into nav)
    with st.expander("Settings"):
        new_domains = st.text_input(
            "Internal domains (comma-separated)",
            value=st.session_state.internal_domains,
            help="Email domains to exclude as internal.",
        )
        if new_domains != st.session_state.internal_domains:
            st.session_state.internal_domains = new_domains


def render_pipeline_bar(df: pd.DataFrame) -> None:
    """Visual pipeline summary showing count per stage."""
    st.markdown(
        '<div class="section-header">Pipeline</div>', unsafe_allow_html=True,
    )

    # Build all stage blocks as a single HTML string for consistent rendering
    blocks: list[str] = []
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


def render_kpi_row(df: pd.DataFrame) -> None:
    """Four key metric cards."""
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


def render_filter_bar(df: pd.DataFrame) -> tuple[str, str, str]:
    """Inline filter controls above the contact cards."""
    c1, c2, c3 = st.columns([3, 1.5, 1.5])
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
    return search, stage, sort


def render_contact_cards(df: pd.DataFrame) -> None:
    """Render each contact as an expandable card."""
    if df.empty:
        st.markdown(
            """
            <div class="empty-state">
                <div class="empty-state-icon">\U0001F4ED</div>
                <div class="empty-state-title">No contacts to display</div>
                <div class="empty-state-desc">
                    Sync your Outlook to pull in recent client communications,
                    or adjust your filters above.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    for _, row in df.iterrows():
        stage = row.get("stage", "")
        days = row.get("days_since_touch")
        touch_dt = pd.to_datetime(row.get("last_touch"), utc=True, errors="coerce")
        date_str = touch_dt.strftime("%d %b %Y") if pd.notna(touch_dt) else "\u2014"

        # Truncate notes for the detail view
        notes_text = str(row.get("notes", "")) if row.get("notes") else ""
        if len(notes_text) > 300:
            notes_text = notes_text[:300] + "\u2026"

        card_html = f"""
        <div class="contact-card">
            <div class="contact-card-header">
                <div class="contact-card-left">
                    <span class="contact-card-client">{_esc(row.get('client_name', ''))}</span>
                    <span class="contact-card-name">{_esc(row.get('contact_name', ''))}</span>
                </div>
                <div class="contact-card-right">
                    {_pill_html(stage)}
                    {_days_html(days)}
                </div>
            </div>
            <div class="contact-card-subject">{_esc(row.get('latest_subject', ''))}</div>
            <div class="contact-card-meta">
                <div class="contact-card-next">{_esc(row.get('next_step', ''))}</div>
            </div>
            <details class="card-details">
                <summary>View details</summary>
                <div class="card-detail-row">
                    <span class="card-detail-label">Email</span>
                    <span class="card-detail-value">{_esc(row.get('counterparty_email', ''))}</span>
                </div>
                <div class="card-detail-row">
                    <span class="card-detail-label">Owner</span>
                    <span class="card-detail-value">{_esc(row.get('owner', ''))}</span>
                </div>
                <div class="card-detail-row">
                    <span class="card-detail-label">Last touch</span>
                    <span class="card-detail-value">{date_str}</span>
                </div>
                <div class="card-detail-notes">{_esc(notes_text)}</div>
            </details>
        </div>
        """
        st.markdown(card_html, unsafe_allow_html=True)


def render_csv_export(df: pd.DataFrame) -> None:
    """Download button for CSV export."""
    if df.empty:
        return
    _, right = st.columns([5, 1])
    with right:
        csv_bytes = df.to_csv(index=False).encode("utf-8")
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

def main() -> None:
    init_state()
    load_css()

    # ── Auth gate ──
    if not st.session_state.authenticated:
        render_auth_screen()
        return

    # ── Authenticated dashboard ──
    render_top_nav()

    # Determine working data
    df = st.session_state.tracker_df
    using_sample = False
    if df.empty:
        df = pd.DataFrame(SAMPLE_DATA)
        using_sample = True

    render_pipeline_bar(df)
    render_kpi_row(df)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Sync prompt if using sample data
    if using_sample:
        st.markdown(
            '<div class="sample-notice">'
            'Showing sample data \u2014 click "Sync Outlook" above to load your real activity.'
            '</div>',
            unsafe_allow_html=True,
        )
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # Filters
    search, stage, sort = render_filter_bar(df)
    filtered = apply_filters(df, search, stage, sort)

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    # Contact cards
    render_contact_cards(filtered)

    # Export
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    render_csv_export(filtered)

    # Last sync timestamp
    if st.session_state.last_sync:
        sync_time = st.session_state.last_sync.strftime("%d %b %Y %H:%M UTC")
        st.markdown(
            f'<div class="sample-notice">Last synced: {sync_time}</div>',
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()
