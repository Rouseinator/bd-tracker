import msal
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timezone

st.set_page_config(
    page_title="BD Tracker",
    page_icon="📈",
    layout="wide",
)

CLIENT_ID = "74a06330-3a89-4cf8-871d-9d783c483d9d"
TENANT_ID = "a14b16a4-0cbe-435c-a893-78e3e95b09c3"
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["Mail.Read"]
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
DEFAULT_INTERNAL_DOMAINS = ["forethought.com.au", "forethought.com"]
FOLLOW_UP_DAYS = 5


CUSTOM_CSS = """
<style>
    .stApp {
        background: radial-gradient(circle at top right, rgba(110,168,254,0.10), transparent 30%),
                    radial-gradient(circle at top left, rgba(126,240,194,0.08), transparent 25%),
                    #0b1020;
        color: #e8edf7;
    }
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1450px;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(180deg, rgba(24,34,53,0.98), rgba(18,26,43,0.98));
        border: 1px solid #26324a;
        padding: 14px;
        border-radius: 16px;
    }
    div[data-testid="stSidebar"] {
        background: #121a2b;
        border-right: 1px solid #26324a;
    }
    .card {
        background: linear-gradient(180deg, rgba(24,34,53,0.98), rgba(18,26,43,0.98));
        border: 1px solid #26324a;
        border-radius: 18px;
        padding: 18px 20px;
        margin-bottom: 16px;
    }
    .pill {
        display: inline-block;
        padding: 6px 10px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 600;
        border: 1px solid transparent;
    }
</style>
"""

STAGE_STYLES = {
    "Outreach Sent": ("#b9d1ff", "rgba(110,168,254,0.12)", "rgba(110,168,254,0.25)"),
    "Follow-up Needed": ("#ffe4a8", "rgba(255,212,121,0.12)", "rgba(255,212,121,0.25)"),
    "Engaged": ("#bbffe3", "rgba(126,240,194,0.12)", "rgba(126,240,194,0.25)"),
    "Meeting Proposed": ("#ddd1ff", "rgba(176,151,255,0.13)", "rgba(176,151,255,0.24)"),
    "Meeting Booked": ("#c3fbff", "rgba(93,224,230,0.14)", "rgba(93,224,230,0.24)"),
    "Proposal Requested": ("#ffd0d0", "rgba(255,143,143,0.12)", "rgba(255,143,143,0.24)"),
    "Proposal Sent": ("#ffe0ba", "rgba(255,173,90,0.13)", "rgba(255,173,90,0.24)"),
    "Commissioned": ("#d6ffd8", "rgba(117,243,128,0.13)", "rgba(117,243,128,0.24)"),
    "Closed / Not Now": ("#dde4f1", "rgba(155,166,190,0.14)", "rgba(155,166,190,0.24)"),
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
        "days_since_touch": 1,
        "latest_subject": "Confirmed: coffee next Tuesday",
        "next_step": "Prepare a one-page discussion agenda before the meeting.",
        "notes": "Catch-up locked in for Tuesday morning.",
    },
]


def init_state() -> None:
    defaults = {
        "access_token": None,
        "tracker_df": pd.DataFrame(),
        "authenticated": False,
        "device_flow": None,
        "auth_message": "",
        "user_code": "",
        "auth_result": None,
        "account_label": "",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def get_app() -> msal.PublicClientApplication:
    return msal.PublicClientApplication(client_id=CLIENT_ID, authority=AUTHORITY)


def start_device_flow() -> None:
    app = get_app()
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

    app = get_app()
    result = app.acquire_token_by_device_flow(flow)
    st.session_state.auth_result = result

    if "access_token" not in result:
        raise RuntimeError(f"Auth failed: {result}")

    st.session_state.access_token = result["access_token"]
    st.session_state.authenticated = True
    account = result.get("id_token_claims", {})
    display_name = account.get("name") or account.get("preferred_username") or "Signed in user"
    username = account.get("preferred_username", "")
    st.session_state.account_label = f"{display_name} · {username}" if username else display_name


def graph_get(path: str, params: dict | None = None) -> dict:
    token = st.session_state.access_token
    if not token:
        raise RuntimeError("No access token found. Please authenticate first.")

    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{GRAPH_BASE}{path}", headers=headers, params=params, timeout=30)
    if response.status_code >= 400:
        raise RuntimeError(f"Graph error {response.status_code}: {response.text}")
    return response.json()


def email_addr(obj: dict | None) -> str | None:
    try:
        return obj["emailAddress"]["address"].lower()
    except Exception:
        return None


def email_name(obj: dict | None) -> str:
    try:
        return obj["emailAddress"]["name"]
    except Exception:
        return ""


def domain_to_client(email: str | None) -> str:
    if not email or "@" not in email:
        return "Unknown client"
    domain = email.split("@", 1)[1]
    base = domain.split(".")[0]
    return " ".join(part.capitalize() for part in base.replace("-", " ").split())


def is_internal_email(email: str | None, internal_domains: list[str]) -> bool:
    if not email:
        return True
    lower = email.lower()
    return any(lower.endswith(f"@{domain}") for domain in internal_domains)


def fetch_recent_messages(limit: int) -> tuple[list[dict], list[dict]]:
    select = "id,subject,from,toRecipients,receivedDateTime,sentDateTime,conversationId,bodyPreview"
    inbox = graph_get(
        "/me/mailFolders/Inbox/messages",
        params={
            "$top": limit,
            "$orderby": "receivedDateTime desc",
            "$select": select,
        },
    ).get("value", [])
    sent = graph_get(
        "/me/mailFolders/SentItems/messages",
        params={
            "$top": limit,
            "$orderby": "sentDateTime desc",
            "$select": select,
        },
    ).get("value", [])
    return inbox, sent


def normalise_messages(messages: list[dict], box: str, internal_domains: list[str]) -> list[dict]:
    rows = []
    for m in messages:
        if box == "inbox":
            counterparty = email_addr(m.get("from", {}))
            if is_internal_email(counterparty, internal_domains):
                continue
            rows.append(
                {
                    "message_id": m.get("id"),
                    "conversation_id": m.get("conversationId"),
                    "direction": "inbound",
                    "datetime": m.get("receivedDateTime"),
                    "subject": m.get("subject"),
                    "counterparty_email": counterparty,
                    "contact_name": email_name(m.get("from", {})),
                    "preview": m.get("bodyPreview"),
                }
            )
        else:
            recipients = m.get("toRecipients", [])
            external = None
            for recipient in recipients:
                addr = email_addr(recipient)
                if not is_internal_email(addr, internal_domains):
                    external = recipient
                    break
            if not external and recipients:
                external = recipients[0]
            counterparty = email_addr(external)
            if is_internal_email(counterparty, internal_domains):
                continue
            rows.append(
                {
                    "message_id": m.get("id"),
                    "conversation_id": m.get("conversationId"),
                    "direction": "outbound",
                    "datetime": m.get("sentDateTime"),
                    "subject": m.get("subject"),
                    "counterparty_email": counterparty,
                    "contact_name": email_name(external),
                    "preview": m.get("bodyPreview"),
                }
            )
    return rows


def derive_days(date_str: str | None) -> int | None:
    if not date_str:
        return None
    dt = pd.to_datetime(date_str, utc=True, errors="coerce")
    if pd.isna(dt):
        return None
    now = datetime.now(timezone.utc)
    return max(0, (now - dt.to_pydatetime()).days)


def infer_stage(last_out: str | None, last_in: str | None, subject: str, preview: str) -> str:
    text = f"{subject or ''} {preview or ''}".lower()
    if any(word in text for word in ["proposal", "scope", "pricing", "quote", "fee"]):
        if any(word in text for word in ["attached", "proposal attached", "send through"]):
            return "Proposal Sent"
        return "Proposal Requested"
    if any(word in text for word in ["meet", "catch up", "catch-up", "calendar", "invite", "booked", "confirmed"]):
        if any(word in text for word in ["invite", "booked", "confirmed"]):
            return "Meeting Booked"
        return "Meeting Proposed"
    if last_out and (not last_in or pd.to_datetime(last_in, utc=True) < pd.to_datetime(last_out, utc=True)):
        days = derive_days(last_out) or 0
        return "Follow-up Needed" if days >= FOLLOW_UP_DAYS else "Outreach Sent"
    return "Engaged"


def suggest_next_step(stage: str) -> str:
    mapping = {
        "Follow-up Needed": "Send a short follow-up and propose a next step.",
        "Meeting Proposed": "Confirm a time and send a calendar invite.",
        "Meeting Booked": "Prepare an agenda and meeting notes.",
        "Proposal Requested": "Draft a proposal outline and indicative fee range.",
        "Proposal Sent": "Follow up on proposal feedback and timing.",
        "Engaged": "Keep momentum with a relevant next touchpoint.",
        "Outreach Sent": "Monitor for reply or send a follow-up in a few days.",
    }
    return mapping.get(stage, "Review thread and decide next step.")


def build_tracker_df(messages: list[dict], owner: str) -> pd.DataFrame:
    if not messages:
        return pd.DataFrame(
            columns=[
                "client_name",
                "contact_name",
                "counterparty_email",
                "owner",
                "stage",
                "last_touch",
                "days_since_touch",
                "latest_subject",
                "next_step",
                "notes",
            ]
        )

    grouped: dict[str, list[dict]] = {}
    for msg in messages:
        grouped.setdefault(msg["counterparty_email"], []).append(msg)

    tracker_rows = []
    for email, group in grouped.items():
        group = sorted(group, key=lambda x: pd.to_datetime(x["datetime"], utc=True, errors="coerce"))
        inbound = [m for m in group if m["direction"] == "inbound"]
        outbound = [m for m in group if m["direction"] == "outbound"]
        latest = group[-1]
        last_in = inbound[-1]["datetime"] if inbound else None
        last_out = outbound[-1]["datetime"] if outbound else None
        stage = infer_stage(last_out, last_in, latest.get("subject", ""), latest.get("preview", ""))
        tracker_rows.append(
            {
                "client_name": domain_to_client(email),
                "contact_name": latest.get("contact_name", ""),
                "counterparty_email": email,
                "owner": owner,
                "stage": stage,
                "last_touch": latest.get("datetime"),
                "days_since_touch": derive_days(latest.get("datetime")),
                "latest_subject": latest.get("subject", "—"),
                "next_step": suggest_next_step(stage),
                "notes": latest.get("preview", ""),
            }
        )

    df = pd.DataFrame(tracker_rows)
    return df.sort_values(["days_since_touch", "client_name"], ascending=[False, True], na_position="last")


def pill_html(stage: str) -> str:
    fg, bg, border = STAGE_STYLES.get(stage, ("#b9d1ff", "rgba(110,168,254,0.12)", "rgba(110,168,254,0.25)"))
    return f'<span class="pill" style="color:{fg}; background:{bg}; border-color:{border};">{stage}</span>'


def render_auth_screen() -> None:
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.title("BD Tracker")
    st.write(
        "Authenticate with Microsoft using device code flow, then load recent Inbox and Sent Items directly from Microsoft Graph."
    )
    st.code(f"Client ID: {CLIENT_ID}\nTenant ID: {TENANT_ID}\nScopes: {', '.join(SCOPES)}")

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("Start sign-in", type="primary", use_container_width=True):
            try:
                start_device_flow()
            except Exception as exc:
                st.error(str(exc))

    with col2:
        can_complete = bool(st.session_state.device_flow)
        if st.button("I have authenticated", use_container_width=True, disabled=not can_complete):
            try:
                complete_device_flow()
                st.rerun()
            except Exception as exc:
                st.error(str(exc))

    if st.session_state.auth_message:
        st.info(st.session_state.auth_message)
        if st.session_state.user_code:
            st.code(st.session_state.user_code)

    st.markdown("</div>", unsafe_allow_html=True)


def render_sidebar(df: pd.DataFrame) -> tuple[str, str, str]:
    with st.sidebar:
        st.header("Filters")
        account_label = st.session_state.account_label or "Signed in"
        st.caption(account_label)

        internal_domains = st.text_input(
            "Internal domains",
            value=", ".join(DEFAULT_INTERNAL_DOMAINS),
            help="Comma-separated internal domains to exclude from the tracker.",
        )
        st.session_state["internal_domains_input"] = internal_domains

        search_text = st.text_input("Search", placeholder="Client, contact, notes, subject")
        stages = ["All stages"] + sorted(df["stage"].dropna().unique().tolist()) if not df.empty else ["All stages"]
        stage_filter = st.selectbox("Stage", stages)
        sort_filter = st.selectbox(
            "Sort",
            ["Days since touch ↓", "Days since touch ↑", "Client A–Z", "Stage A–Z"],
        )

        if st.button("Sign out", use_container_width=True):
            for key in ["access_token", "authenticated", "device_flow", "auth_message", "user_code", "auth_result", "account_label"]:
                st.session_state[key] = None if key in ["access_token", "device_flow", "auth_result"] else ""
            st.session_state.authenticated = False
            st.rerun()

    return search_text, stage_filter, sort_filter


def render_dashboard(df: pd.DataFrame) -> None:
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    st.title("Business Development Tracker")
    st.caption("Dark, local-first Streamlit app using Microsoft device code flow and Graph.")

    col_a, col_b = st.columns([3, 1])
    with col_a:
        st.markdown("<div class='card'><strong>Ready to sync</strong><br>Pull recent Inbox and Sent Items from Outlook, classify outreach stage, and export the tracker.</div>", unsafe_allow_html=True)
    with col_b:
        if st.button("Sync Outlook", type="primary", use_container_width=True):
            try:
                internal_domains = [
                    d.strip().lower()
                    for d in st.session_state.get("internal_domains_input", ", ".join(DEFAULT_INTERNAL_DOMAINS)).split(",")
                    if d.strip()
                ]
                inbox, sent = fetch_recent_messages(limit=100)
                messages = normalise_messages(inbox, "inbox", internal_domains) + normalise_messages(sent, "sent", internal_domains)
                owner = st.session_state.account_label.split(" · ")[0] if st.session_state.account_label else "Me"
                st.session_state.tracker_df = build_tracker_df(messages, owner)
                st.success("Outlook sync complete.")
            except Exception as exc:
                st.error(str(exc))

    tracker_df = st.session_state.tracker_df.copy()
    search_text, stage_filter, sort_filter = render_sidebar(tracker_df)

    if tracker_df.empty:
        tracker_df = pd.DataFrame(SAMPLE_DATA)

    filtered = tracker_df.copy()

    if search_text:
        mask = filtered.astype(str).apply(lambda col: col.str.contains(search_text, case=False, na=False))
        filtered = filtered[mask.any(axis=1)]

    if stage_filter != "All stages":
        filtered = filtered[filtered["stage"] == stage_filter]

    if sort_filter == "Days since touch ↑":
        filtered = filtered.sort_values("days_since_touch", ascending=True, na_position="last")
    elif sort_filter == "Client A–Z":
        filtered = filtered.sort_values("client_name", ascending=True)
    elif sort_filter == "Stage A–Z":
        filtered = filtered.sort_values("stage", ascending=True)
    else:
        filtered = filtered.sort_values("days_since_touch", ascending=False, na_position="last")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total clients", int(len(filtered)))
    c2.metric("Need follow-up", int((filtered["stage"] == "Follow-up Needed").sum()))
    c3.metric("Meetings booked", int((filtered["stage"] == "Meeting Booked").sum()))
    c4.metric("Proposal stage", int(filtered["stage"].isin(["Proposal Requested", "Proposal Sent"]).sum()))

    if filtered.empty:
        st.info("No matching records.")
        return

    display_df = filtered.copy()
    display_df["stage"] = display_df["stage"].apply(pill_html)
    display_df["last_touch"] = pd.to_datetime(display_df["last_touch"], utc=True, errors="coerce").dt.strftime("%d %b %Y")
    display_df = display_df.rename(
        columns={
            "client_name": "Client",
            "contact_name": "Contact",
            "counterparty_email": "Email",
            "owner": "Owner",
            "stage": "Stage",
            "last_touch": "Last touch",
            "days_since_touch": "Days",
            "latest_subject": "Latest subject",
            "next_step": "Next step",
            "notes": "Notes",
        }
    )

    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.subheader("Tracker")
    st.write(
        display_df.to_html(escape=False, index=False),
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    csv_bytes = filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name="bd_tracker_export.csv",
        mime="text/csv",
    )


def main() -> None:
    init_state()
    if not st.session_state.authenticated:
        render_auth_screen()
    else:
        render_dashboard(st.session_state.tracker_df)


if __name__ == "__main__":
    main()
