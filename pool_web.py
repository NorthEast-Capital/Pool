import json
import os
import calendar
from datetime import datetime, timedelta
from io import StringIO
from typing import Dict, Any, List
import uuid
import smtplib
from email.message import EmailMessage

import streamlit as st
import pandas as pd
import hashlib
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# FILE PATHS & CONSTANTS
# ============================================================

USERS_FILE = "users.json"
DATA_FILE = "pool_data.json"
NEWS_DIR = "news_files"

os.makedirs(NEWS_DIR, exist_ok=True)

DEFAULT_CURRENCY = "USD "
COMPANY_INVESTOR_NAME = "Company"
DEFAULT_FEE_RATE = 0.30  # 30%
ADMIN_EMAIL = "northeast1.capital@gmail.com"

# ============================================================
# DEFAULT USER DATA
# ============================================================

DEFAULT_USERS = {
    "admin": {
        "password": "188209e1a05f534cbbabe055a14ea1b7b1d940033a7647483b04f18d58c0a87a",
        "role": "admin",
        "investor_name": None,
        "active": True,
        "phone": "",
        "email": ADMIN_EMAIL,
        "investor_name_locked": False,
        "username_locked": False,
        "last_login": None,
        "prev_login": None,
        "failed_attempts": 0,
        "locked_until": None,
    },
}

# ============================================================
# SECURITY HELPERS (PASSWORD HASHING & LOGIN LOCKOUT)
# ============================================================

MAX_FAILED_ATTEMPTS = 5
LOCKOUT_MINUTES = 10  # minutes to lock account after too many failures


def hash_password(plain: str) -> str:
    """Return a SHA-256 hash for the given plain-text password."""
    return hashlib.sha256(plain.encode("utf-8")).hexdigest()


def is_probably_hashed(value: str) -> bool:
    """Heuristic: check if a stored password looks like a SHA-256 hex hash."""
    if not isinstance(value, str):
        return False
    if len(value) != 64:
        return False
    for ch in value:
        if ch not in "0123456789abcdef":
            return False
    return True


def verify_password(plain: str, stored: str) -> bool:
    """
    Check if the plain password matches the stored value.
    Supports both hashed and legacy plain-text passwords.
    """
    if is_probably_hashed(stored):
        return hash_password(plain) == stored
    # legacy behaviour: stored as plain text
    return plain == stored


def is_account_locked(user_rec: Dict[str, Any]) -> bool:
    """
    Return True if account is currently locked.
    Also auto-clears lock if the lock time has passed.
    """
    locked_until_str = user_rec.get("locked_until")
    if not locked_until_str:
        return False

    try:
        locked_until = datetime.strptime(locked_until_str, "%Y-%m-%d %H:%M")
    except Exception:
        # if format is broken, clear the lock
        user_rec["locked_until"] = None
        user_rec["failed_attempts"] = 0
        return False

    now = datetime.now()
    if now < locked_until:
        return True

    # lock expired -> clear it
    user_rec["locked_until"] = None
    user_rec["failed_attempts"] = 0
    return False


def register_failed_login(user_rec: Dict[str, Any]) -> None:
    """Increase failed attempts counter and lock account if threshold reached."""
    failed = int(user_rec.get("failed_attempts") or 0) + 1
    user_rec["failed_attempts"] = failed

    if failed >= MAX_FAILED_ATTEMPTS:
        lock_until = datetime.now() + timedelta(minutes=LOCKOUT_MINUTES)
        user_rec["locked_until"] = lock_until.strftime("%Y-%m-%d %H:%M")


def reset_login_attempts(user_rec: Dict[str, Any]) -> None:
    """Reset failed attempts counter and clear lock."""
    user_rec["failed_attempts"] = 0
    user_rec["locked_until"] = None

# ============================================================
# GOOGLE SHEETS "DATABASE" HELPERS
# ============================================================

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def get_gs_client():
    """Authorise Google Sheets client using service account from st.secrets."""
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=SCOPE
    )
    return gspread.authorize(creds)

@st.cache_resource
def get_db_sheet():
    """Open the Google Sheet defined by st.secrets['sheet_id']."""
    client = get_gs_client()
    return client.open_by_key(st.secrets["sheet_id"])

def _get_or_create_ws(title: str):
    """Get a worksheet by title, create if missing."""
    sh = get_db_sheet()
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=10, cols=2)
    return ws

def load_users() -> Dict[str, Any]:
    """Load users from Google Sheets (USERS_JSON!A1)."""
    ws = _get_or_create_ws("USERS_JSON")
    raw = ws.acell("A1").value
    if not raw:
        # initialise with default users (admin + sample investor)
        ws.update("A1", [[json.dumps(DEFAULT_USERS)]])
        return DEFAULT_USERS.copy()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return DEFAULT_USERS.copy()
    return data

def save_users(users: Dict[str, Any]) -> None:
    """Save users into Google Sheets (USERS_JSON!A1)."""
    if is_demo_mode():
        return
    ws = _get_or_create_ws("USERS_JSON")
    ws.update("A1", [[json.dumps(users)]])

def load_data() -> Dict[str, Any]:
    """Load main pool data from Google Sheets (DATA_JSON!A1)."""
    ws = _get_or_create_ws("DATA_JSON")
    raw = ws.acell("A1").value
    if not raw:
        data = {
            "equity": 0.0,
            "investors": {},
            "transactions": [],
            "closings": [],
            "notifications": [],
            "news": [],
            "settings": {},
            "audit_log": [],
            "pl_calendar": [],
        }
        ws.update("A1", [[json.dumps(data)]])
        return data
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = {
            "equity": 0.0,
            "investors": {},
            "transactions": [],
            "closings": [],
            "notifications": [],
            "news": [],
            "settings": {},
            "audit_log": [],
            "pl_calendar": [],
        }

    # Make sure keys exist
    data.setdefault("equity", 0.0)
    data.setdefault("investors", {})
    data.setdefault("transactions", [])
    data.setdefault("closings", [])
    data.setdefault("notifications", [])
    data.setdefault("news", [])
    data.setdefault("settings", {})
    data.setdefault("audit_log", [])
    data.setdefault("pl_calendar", [])
    return data

def save_data(data: Dict[str, Any]) -> None:
    """Save main pool data into Google Sheets (DATA_JSON!A1)."""
    if is_demo_mode():
        return
    ws = _get_or_create_ws("DATA_JSON")
    ws.update("A1", [[json.dumps(data)]])

def generate_unique_username(base: str, users: Dict[str, Any]) -> str:
    """
    Generate a unique username based on `base`.
    If base is taken, appends a number: base1, base2, etc.
    """
    base_clean = "".join(c.lower() for c in base if c.isalnum())
    if not base_clean:
        base_clean = "investor"
    username = base_clean
    i = 1
    while username in users:
        username = f"{base_clean}{i}"
        i += 1
    return username

def is_demo_mode() -> bool:
    """Return True if the app is running in demo mode (no saving)."""
    return bool(st.session_state.get("demo_mode", False))


def get_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    if "settings" not in data or not isinstance(data["settings"], dict):
        data["settings"] = {}
    return data["settings"]


def format_currency(amount: float, symbol: str = DEFAULT_CURRENCY) -> str:
    return f"{symbol}{amount:,.2f}"


def is_strong_password(pw: str) -> bool:
    if len(pw) < 8:
        return False
    has_letter = any(c.isalpha() for c in pw)
    has_digit = any(c.isdigit() for c in pw)
    return has_letter and has_digit

# ============================================================
# EMAIL HELPERS (OPTIONAL – FAIL-SAFE)
# ============================================================

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")


def send_email_safe(to_addr: str, subject: str, body: str) -> None:
    if not to_addr:
        return
    if not SMTP_USER or not SMTP_PASSWORD:
        return
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = SMTP_USER
        msg["To"] = to_addr
        msg.set_content(body)

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
    except Exception:
        # Do not crash app if email fails
        pass


def find_investor_email(users: Dict[str, Any], investor_name: str) -> str:
    for uname, u in users.items():
        if u.get("role") == "investor" and u.get("investor_name") == investor_name:
            em = (u.get("email") or "").strip()
            if em:
                return em
    return ""


def notify_admin_new_request(investor_name: str, tx_type: str, amount: float,
                             date_str: str, bank: str, note: str) -> None:
    subject = f"[Northeast Capitol] New {tx_type.replace('_', ' ')} from {investor_name}"
    body = f"""A new {tx_type.replace('_', ' ')} has been submitted.

Investor: {investor_name}
Amount: {amount:,.2f} USD
Date: {date_str}
Type: {tx_type}

Bank / payment details:
{bank or '-'}

Remark:
{note or '-'}

Please log in to the Northeast Capitol admin panel to review this request.
"""
    send_email_safe(ADMIN_EMAIL, subject, body)


def notify_investor_request_decision(users: Dict[str, Any], investor_name: str,
                                     tx_type: str, amount: float, status: str,
                                     date_str: str, decision_note: str) -> None:
    email = find_investor_email(users, investor_name)
    if not email:
        return
    subject = f"[Northeast Capitol] Your {tx_type.replace('_', ' ')} has been {status}"
    body = f"""Dear {investor_name},

Your {tx_type.replace('_', ' ')} request has been {status}.

Amount: {amount:,.2f} USD
Decision date: {date_str}

Remark from admin:
{decision_note or '-'}

This is an automated notification from Northeast Capitol.
"""
    send_email_safe(email, subject, body)

# ============================================================
# CORE POOL CALCULATIONS
# ============================================================

def get_nav(data: Dict[str, Any]) -> float:
    total_units = sum(inv.get("units", 0.0) for inv in data["investors"].values())
    equity = data["equity"]
    if total_units <= 0 or equity <= 0:
        return 1.0
    return equity / total_units


def get_net_deposits(data: Dict[str, Any]) -> Dict[str, float]:
    net = {name: 0.0 for name in data["investors"].keys()}
    for tx in data["transactions"]:
        if tx["type"] in ("deposit", "withdrawal"):
            inv = tx["investor"]
            amt = tx["amount"]
            if tx["type"] == "deposit":
                net[inv] = net.get(inv, 0.0) + amt
            else:
                net[inv] = net.get(inv, 0.0) - amt
    return net


def get_total_deposits(data: Dict[str, Any]) -> Dict[str, float]:
    total = {name: 0.0 for name in data["investors"].keys()}
    for tx in data["transactions"]:
        if tx["type"] == "deposit":
            inv = tx["investor"]
            amt = tx["amount"]
            total[inv] = total.get(inv, 0.0) + amt
    return total


def get_current_period_pl_by_investor(data: Dict[str, Any]) -> Dict[str, float]:
    """
    Each investor's P&L for the current open period (since last closing).
    """
    nav = get_nav(data)
    if nav <= 0:
        nav = 1.0

    net_deposits = get_net_deposits(data)
    result: Dict[str, float] = {}

    for name, info in data["investors"].items():
        if name == COMPANY_INVESTOR_NAME:
            continue

        units = info.get("units", 0.0)
        balance = units * nav
        net_dep = net_deposits.get(name, 0.0)
        total_pl = balance - net_dep
        prev_base = info.get("pl_base", 0.0)
        period_pl = total_pl - prev_base
        result[name] = period_pl

    return result


def get_investor_table(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    nav = get_nav(data)
    if nav <= 0:
        nav = 1.0

    net_deposits = get_net_deposits(data)
    period_pl = get_current_period_pl_by_investor(data)

    rows = []
    for name, info in data["investors"].items():
        units = info.get("units", 0.0)
        balance = units * nav
        net_dep = net_deposits.get(name, 0.0)
        pl_period = period_pl.get(name, 0.0)

        rows.append(
            {
                "Investor": name,
                "Units": round(units, 4),
                "Balance": round(balance, 2),
                "Net Deposit": round(net_dep, 2),
                "P/L": round(pl_period, 2),  # P/L for current period
            }
        )
    return rows


def get_equity_history(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    hist = []
    for tx in data["transactions"]:
        if tx["type"] == "equity_update":
            hist.append(
                {
                    "date": tx["date"],
                    "equity": tx["new_equity"],
                }
            )
    hist.sort(key=lambda x: x["date"])
    return hist

# ============================================================
# DAILY P&L CALENDAR
# ============================================================

def set_daily_pl(data: Dict[str, Any], date_str: str, pl_value: float) -> None:
    cal = data.get("pl_calendar", [])
    cal = [rec for rec in cal if rec.get("date") != date_str]
    cal.append({"date": date_str, "pl": float(pl_value)})
    data["pl_calendar"] = cal
    save_data(data)


def get_month_pl(data: Dict[str, Any], year: int, month: int):
    total = 0.0
    daily_map: Dict[str, float] = {}
    for rec in data.get("pl_calendar", []):
        try:
            dt = datetime.strptime(rec["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if dt.year == year and dt.month == month:
            daily_map[rec["date"]] = daily_map.get(rec["date"], 0.0) + float(rec.get("pl", 0.0))
            total += float(rec.get("pl", 0.0))
    return total, daily_map


def get_range_pl(data: Dict[str, Any], start_date, end_date) -> float:
    total = 0.0
    for rec in data.get("pl_calendar", []):
        try:
            dt = datetime.strptime(rec["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if start_date <= dt <= end_date:
            total += float(rec.get("pl", 0.0))
    return total


def get_year_pl(data: Dict[str, Any], year: int) -> float:
    total = 0.0
    for rec in data.get("pl_calendar", []):
        try:
            dt = datetime.strptime(rec["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if dt.year == year:
            total += float(rec.get("pl", 0.0))
    return total


def get_all_time_pl(data: Dict[str, Any]) -> float:
    return sum(float(rec.get("pl", 0.0)) for rec in data.get("pl_calendar", []))


def draw_month_calendar(year: int, month: int, daily_map: Dict[str, float]) -> None:
    cal_obj = calendar.Calendar(firstweekday=0)  # Monday
    weeks = cal_obj.monthdayscalendar(year, month)

    st.markdown("##### Calendar")
    weekday_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    cols = st.columns(7)
    for i, label in enumerate(weekday_labels):
        cols[i].markdown(
            f"<div style='text-align:center; font-weight:600'>{label}</div>",
            unsafe_allow_html=True,
        )

    for week in weeks:
        cols = st.columns(7)
        for i, day in enumerate(week):
            if day == 0:
                cols[i].markdown(
                    "<div style='padding:10px; text-align:center; border-radius:10px; "
                    "background-color:#111827; color:#9CA3AF;'>—</div>",
                    unsafe_allow_html=True,
                )
            else:
                date_str = f"{year}-{month:02d}-{day:02d}"
                pl_val = daily_map.get(date_str)
                if pl_val is None:
                    cols[i].markdown(
                        f"<div style='padding:10px; text-align:center; border-radius:10px; "
                        f"background-color:#111827; color:#E5E7EB;'>{day}<br>—</div>",
                        unsafe_allow_html=True,
                    )
                else:
                    sign = "+" if pl_val > 0 else ""
                    if pl_val > 0:
                        bg = "#14532D"
                    elif pl_val < 0:
                        bg = "#7F1D1D"
                    else:
                        bg = "#1F2937"
                    cols[i].markdown(
                        f"<div style='padding:10px; text-align:center; border-radius:10px; "
                        f"background-color:{bg}; color:#F9FAFB;'>"
                        f"{day}<br>{sign}{pl_val:,.2f}</div>",
                        unsafe_allow_html=True,
                    )

# ============================================================
# AUDIT LOG
# ============================================================

def ensure_audit_log(data: Dict[str, Any]) -> None:
    if "audit_log" not in data or not isinstance(data["audit_log"], list):
        data["audit_log"] = []


def add_audit_entry(data: Dict[str, Any], actor: str, action: str, details: str = "") -> None:
    ensure_audit_log(data)
    entry = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "actor": actor,
        "action": action,
        "details": details,
    }
    data["audit_log"].append(entry)
    save_data(data)

# ============================================================
# NOTIFICATIONS
# ============================================================

def ensure_notifications(data: Dict[str, Any]) -> None:
    if "notifications" not in data:
        data["notifications"] = []


def add_notification(
    data: Dict[str, Any],
    from_user: str,
    to_usernames: List[str],
    title: str,
    message: str,
    ntype: str = "system",
) -> None:
    ensure_notifications(data)
    notif_id = len(data["notifications"]) + 1
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    notif = {
        "id": notif_id,
        "datetime": now_str,
        "from": from_user,
        "to_usernames": to_usernames,
        "title": title,
        "message": message,
        "type": ntype,
        "read_by": [],
    }
    data["notifications"].append(notif)
    save_data(data)


def get_user_notifications(data: Dict[str, Any], username: str) -> List[Dict[str, Any]]:
    ensure_notifications(data)
    return [n for n in data["notifications"] if username in n.get("to_usernames", [])]


def count_unread_notifications(data: Dict[str, Any], username: str) -> int:
    ensure_notifications(data)
    return sum(
        1
        for n in data["notifications"]
        if username in n.get("to_usernames", []) and username not in n.get("read_by", [])
    )


def mark_all_notifications_read_for_user(data: Dict[str, Any], username: str) -> None:
    ensure_notifications(data)
    changed = False
    for n in data["notifications"]:
        if username in n.get("to_usernames", []) and username not in n.get("read_by", []):
            n.setdefault("read_by", []).append(username)
            changed = True
    if changed:
        save_data(data)

# ============================================================
# PERFORMANCE FEE & CLOSING
# ============================================================

def ensure_company_investor(data: Dict[str, Any]) -> None:
    if COMPANY_INVESTOR_NAME not in data["investors"]:
        data["investors"][COMPANY_INVESTOR_NAME] = {"units": 0.0}


def get_monthly_closing_preview(
    data: Dict[str, Any],
    fee_rate: float,
):
    nav = get_nav(data)
    if nav <= 0:
        nav = 1.0

    investor_rows = get_investor_table(data)
    net_deposits = get_net_deposits(data)
    preview_rows = []

    for row in investor_rows:
        inv = row["Investor"]
        if inv == COMPANY_INVESTOR_NAME:
            continue

        units = data["investors"][inv]["units"]
        balance = units * nav
        net_dep = net_deposits.get(inv, 0.0)
        pl_total = balance - net_dep
        pl_prev_base = data["investors"][inv].get("pl_base", 0.0)
        pl_period = pl_total - pl_prev_base

        fee_amount = max(pl_period, 0.0) * fee_rate
        fee_units = fee_amount / nav if fee_amount > 0 else 0.0

        preview_rows.append(
            {
                "Investor": inv,
                "P/L this period": round(pl_period, 2),
                "Proposed fee amount": round(fee_amount, 2),
                "Proposed fee units": round(fee_units, 4),
            }
        )

    return preview_rows, nav


def apply_monthly_closing(
    data: Dict[str, Any],
    fee_rate: float,
    closing_date_str: str,
    period_start_str: str,
    period_end_str: str,
    pool_period_pl: float,
):
    """
    Apply month-end closing for a declared period.
    """
    ensure_company_investor(data)
    nav = get_nav(data)
    if nav <= 0:
        nav = 1.0

    investor_rows = get_investor_table(data)
    net_deposits = get_net_deposits(data)

    total_fee_amount = 0.0
    total_fee_units = 0.0
    total_pl_period_investors = 0.0
    closing_details = {}

    for row in investor_rows:
        inv = row["Investor"]
        if inv == COMPANY_INVESTOR_NAME:
            continue

        units_before = data["investors"][inv]["units"]
        balance_before = units_before * nav
        net_dep = net_deposits.get(inv, 0.0)

        pl_total_before = balance_before - net_dep
        pl_prev_base = data["investors"][inv].get("pl_base", 0.0)
        pl_period = pl_total_before - pl_prev_base
        total_pl_period_investors += pl_period

        fee_amount = max(pl_period, 0.0) * fee_rate
        fee_units = fee_amount / nav if fee_amount > 0 else 0.0

        if fee_units > units_before:
            fee_units = units_before
            fee_amount = fee_units * nav

        if fee_units > 0:
            data["investors"][inv]["units"] = units_before - fee_units
            data["investors"][COMPANY_INVESTOR_NAME]["units"] += fee_units

            tx = {
                "date": closing_date_str,
                "type": "fee",
                "investor": inv,
                "amount": fee_amount,
                "units": -fee_units,
                "nav": nav,
                "fee_rate": fee_rate,
                "company_investor": COMPANY_INVESTOR_NAME,
                "pl_period": pl_period,
            }
            data["transactions"].append(tx)

        units_after = data["investors"][inv]["units"]
        balance_after = units_after * nav
        pl_total_after = balance_after - net_dep

        data["investors"][inv]["pl_base"] = pl_total_after

        closing_details[inv] = {
            "units_before": units_before,
            "balance_before": balance_before,
            "net_deposit": net_dep,
            "pl_prev_base": pl_prev_base,
            "pl_total_before": pl_total_before,
            "pl_period": pl_period,
            "fee_amount": fee_amount,
            "fee_units": fee_units,
            "units_after": units_after,
            "balance_after": balance_after,
            "pl_total_after": pl_total_after,
            "pl_base_new": pl_total_after,
        }

        total_fee_amount += fee_amount
        total_fee_units += fee_units

    data.setdefault("closings", [])
    closing_record = {
        "closing_date": closing_date_str,
        "period_start": period_start_str,
        "period_end": period_end_str,
        "pool_pl_period": pool_period_pl,
        "investors_pl_period_sum": total_pl_period_investors,
        "fee_rate": fee_rate,
        "nav": nav,
        "investors": closing_details,
    }
    data["closings"].append(closing_record)
    save_data(data)

    return {
        "total_fee_amount": total_fee_amount,
        "total_fee_units": total_fee_units,
        "closing": closing_record,
    }

# ============================================================
# INVESTOR RENAME (PROFILE CHANGE)
# ============================================================

def rename_investor_everywhere(data: Dict[str, Any], old: str, new: str) -> None:
    if old == new:
        return
    if new in data["investors"]:
        raise ValueError("New investor name already exists in pool data.")

    data["investors"][new] = data["investors"].pop(old)

    for tx in data["transactions"]:
        if tx.get("investor") == old:
            tx["investor"] = new
        if tx.get("from_investor") == old:
            tx["from_investor"] = new

    for closing in data.get("closings", []):
        inv_map = closing.get("investors", {})
        if old in inv_map:
            inv_map[new] = inv_map.pop(old)

    save_data(data)

# ============================================================
# LOGIN & FORGOT PASSWORD
# ============================================================

def login_screen():
    if "users" not in st.session_state:
        st.session_state["users"] = load_users()
    users = st.session_state["users"]

    if "login_mode" not in st.session_state:
        st.session_state["login_mode"] = "login"
    mode = st.session_state["login_mode"]

    # ---------------------------
    # LOGIN MODE
    # ---------------------------
    if mode == "login":
        st.title("Northeast Capitol - Login")

        st.markdown("#### Login")
        with st.form("login_form"):
            login_id = st.text_input("Email or investor name")
            password = st.text_input("Password", type="password")
            col1, col2 = st.columns(2)
            with col1:
                login_clicked = st.form_submit_button("Login", type="primary")
            with col2:
                register_clicked = st.form_submit_button("Register as new investor")

        if register_clicked:
            st.session_state["login_mode"] = "register"
            st.rerun()

        if login_clicked:
            id_clean = (login_id or "").strip()
            id_lower = id_clean.lower()
            if not id_clean:
                st.error("Email or investor name is required.")
            else:
                email_matches = []
                inv_matches = []

                for uname, u in users.items():
                    stored_email = (u.get("email") or "").strip().lower()
                    inv_name = (u.get("investor_name") or "").strip().lower()
                    if stored_email and stored_email == id_lower:
                        email_matches.append((uname, u))
                    elif inv_name and inv_name == id_lower:
                        inv_matches.append((uname, u))

                matched = email_matches or inv_matches

                if not matched:
                    st.error("No account found with this email or investor name.")
                elif len(matched) > 1:
                    st.error("More than one account matches this login ID. Please contact admin.")
                else:
                    uname, user_rec = matched[0]

                    # Ensure security fields exist
                    if "failed_attempts" not in user_rec:
                        user_rec["failed_attempts"] = 0
                    if "locked_until" not in user_rec:
                        user_rec["locked_until"] = None

                    # Check for temporary lock
                    if is_account_locked(user_rec):
                        locked_until_str = user_rec.get("locked_until")
                        if locked_until_str:
                            st.error(
                                f"This account is temporarily locked until {locked_until_str}. "
                                "Please try again later or contact admin."
                            )
                        else:
                            st.error(
                                "This account is temporarily locked. "
                                "Please try again later or contact admin."
                            )
                    elif not user_rec.get("active", True):
                        st.error("This account is inactive. Please contact admin.")
                    else:
                        stored_pw = user_rec.get("password") or ""
                        if not verify_password(password, stored_pw):
                            # Wrong password: increment failed attempts and maybe lock
                            register_failed_login(user_rec)
                            users[uname] = user_rec
                            save_users(users)
                            st.session_state["users"] = users

                            if user_rec.get("locked_until"):
                                st.error(
                                    f"Too many failed attempts. This account is locked until {user_rec['locked_until']}."
                                )
                            else:
                                remaining = MAX_FAILED_ATTEMPTS - int(user_rec.get("failed_attempts") or 0)
                                st.error(
                                    f"Invalid password. {remaining} attempts remaining before temporary lock."
                                )
                        else:
                            # Correct password: upgrade to hashed if needed, reset attempts, log in
                            if not is_probably_hashed(stored_pw):
                                user_rec["password"] = hash_password(password)
                            reset_login_attempts(user_rec)

                            now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                            prev_login = user_rec.get("last_login")
                            user_rec["prev_login"] = prev_login
                            user_rec["last_login"] = now_str
                            users[uname] = user_rec
                            save_users(users)
                            st.session_state["users"] = users

                            st.session_state["user"] = {
                                "username": uname,
                                "role": user_rec["role"],
                                "investor_name": user_rec.get("investor_name"),
                            }
                            st.experimental_set_query_params()
                            st.rerun()

        if st.button("Forgot password?"):
            st.session_state["login_mode"] = "forgot"
            st.rerun()

    # ---------------------------
    # REGISTER MODE
    # ---------------------------
    elif mode == "register":
        st.title("Northeast Capitol - Register")
        st.markdown("#### New investor registration")

        with st.form("register_form"):
            investor_name = st.text_input("Investor name (will appear in reports)")
            email = st.text_input("Email (used for login & notifications)")
            phone = st.text_input("Phone (optional)")
            password = st.text_input("Password", type="password")
            confirm_pw = st.text_input("Confirm password", type="password")

            col1, col2 = st.columns(2)
            with col1:
                create_clicked = st.form_submit_button("Create account", type="primary")
            with col2:
                back_clicked = st.form_submit_button("Back to login")

        if back_clicked:
            st.session_state["login_mode"] = "login"
            st.rerun()

        if create_clicked:
            # ---- basic validation ----
            name_clean = (investor_name or "").strip()
            email_clean = (email or "").strip()
            pw = password or ""
            pw2 = confirm_pw or ""

            if not name_clean:
                st.error("Investor name is required.")
                return
            if not email_clean:
                st.error("Email is required.")
                return
            if pw != pw2:
                st.error("Passwords do not match.")
                return
            if not is_strong_password(pw):
                st.error("Password must be at least 8 characters and contain letters and numbers.")
                return

            # check email / investor name not already used
            email_lower = email_clean.lower()
            name_lower = name_clean.lower()
            for uname, u in users.items():
                existing_email = (u.get("email") or "").strip().lower()
                existing_name = (u.get("investor_name") or "").strip().lower()
                if existing_email and existing_email == email_lower:
                    st.error("This email is already registered. Please log in or contact admin.")
                    return
                if existing_name and existing_name == name_lower:
                    st.error("This investor name is already used. Please choose a slightly different name.")
                    return

            # ---- create new investor user ----
            new_username = generate_unique_username(email_clean or name_clean, users)

            users[new_username] = {
                "password": hash_password(pw),   # store hashed
                "role": "investor",
                "investor_name": name_clean,
                "active": False,                 # admin must activate
                "phone": phone or "",
                "email": email_clean,
                "investor_name_locked": False,
                "username_locked": False,
                "last_login": None,
                "prev_login": None,
                "failed_attempts": 0,
                "locked_until": None,
            }

            save_users(users)
            st.session_state["users"] = users

            st.success("Account created. Admin needs to activate your account before you can log in.")
            st.session_state["login_mode"] = "login"
            st.rerun()

    # ---------------------------
    # FORGOT PASSWORD MODE
    # ---------------------------
    elif mode == "forgot":
        st.title("Northeast Capitol - Forgot Password")
        st.markdown("Enter your registered email and choose a new password.")

        with st.form("forgot_password_form"):
            fp_email = st.text_input("Registered email")
            fp_new_pwd = st.text_input("New password", type="password")
            fp_new_pwd2 = st.text_input("Confirm new password", type="password")
            submitted = st.form_submit_button("Reset password")

        if submitted:
            email_clean = (fp_email or "").strip().lower()
            if not email_clean:
                st.error("Email is required.")
            elif not fp_new_pwd:
                st.error("New password cannot be empty.")
            elif fp_new_pwd != fp_new_pwd2:
                st.error("Password confirmation does not match.")
            elif not is_strong_password(fp_new_pwd):
                st.error("Password must be at least 8 characters and include both letters and numbers.")
            else:
                matched_usernames = []
                for uname, u in users.items():
                    stored_email = (u.get("email") or "").strip().lower()
                    if stored_email and stored_email == email_clean:
                        matched_usernames.append(uname)

                if not matched_usernames:
                    st.error("This email is not registered.")
                elif len(matched_usernames) > 1:
                    st.error("More than one account uses this email. Please contact admin.")
                else:
                    uname = matched_usernames[0]
                    u = users[uname]
                    u["password"] = hash_password(fp_new_pwd)
                    reset_login_attempts(u)
                    users[uname] = u
                    save_users(users)
                    st.session_state["users"] = users
                    st.success("Password has been reset. You can now log in.")

        if st.button("Back to login"):
            st.session_state["login_mode"] = "login"
            st.rerun()

    st.stop()



# ============================================================
# APPROVAL HELPERS
# ============================================================

def get_pending_requests(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    pending = []
    for idx, tx in enumerate(data["transactions"]):
        if tx["type"] in ("deposit_request", "withdrawal_request") and tx.get("status", "pending") == "pending":
            pending.append({"index": idx, "tx": tx})
    return pending


def approve_request(
    data: Dict[str, Any],
    users: Dict[str, Any],
    idx: int,
    admin_username: str,
    decision_note: str,
    approval_date_str: str,
) -> None:
    tx = data["transactions"][idx]
    investor_name = tx["investor"]
    amount = tx["amount"]
    nav = get_nav(data)
    if nav <= 0:
        nav = 1.0

    if investor_name not in data["investors"]:
        raise ValueError(f"Investor '{investor_name}' not found.")

    if tx["type"] == "deposit_request":
        units = amount / nav if nav != 0 else 0.0
        data["investors"][investor_name]["units"] += units
        data["equity"] += amount

        deposit_tx = {
            "date": approval_date_str,
            "type": "deposit",
            "investor": investor_name,
            "amount": amount,
            "units": units,
            "nav": nav,
            "approved_from_request": idx,
            "approved_by": admin_username,
        }
        data["transactions"].append(deposit_tx)

    elif tx["type"] == "withdrawal_request":
        current_units = data["investors"][investor_name]["units"]
        units_to_redeem = amount / nav if nav != 0 else 0.0
        if units_to_redeem > current_units + 1e-9:
            raise ValueError(
                f"Not enough units for withdrawal. {investor_name} has only {current_units:.4f} units."
            )
        data["investors"][investor_name]["units"] = current_units - units_to_redeem
        data["equity"] -= amount

        withdrawal_tx = {
            "date": approval_date_str,
            "type": "withdrawal",
            "investor": investor_name,
            "amount": amount,
            "units": -units_to_redeem,
            "nav": nav,
            "approved_from_request": idx,
            "approved_by": admin_username,
        }
        data["transactions"].append(withdrawal_tx)

    tx["status"] = "approved"
    tx["approved_by"] = admin_username
    tx["approved_date"] = approval_date_str
    tx["decision_note"] = decision_note

    save_data(data)

    add_audit_entry(
        data,
        admin_username,
        "approve_request",
        f"idx={idx}, investor={investor_name}, type={tx['type']}, amount={amount}",
    )

    notify_investor_request_decision(
        users=users,
        investor_name=investor_name,
        tx_type=tx["type"],
        amount=amount,
        status="approved",
        date_str=approval_date_str,
        decision_note=decision_note,
    )


def reject_request(
    data: Dict[str, Any],
    users: Dict[str, Any],
    idx: int,
    admin_username: str,
    decision_note: str,
    decision_date_str: str,
) -> None:
    tx = data["transactions"][idx]
    investor_name = tx["investor"]
    amount = tx["amount"]

    tx["status"] = "rejected"
    tx["approved_by"] = admin_username
    tx["approved_date"] = decision_date_str
    tx["decision_note"] = decision_note
    save_data(data)

    add_audit_entry(
        data,
        admin_username,
        "reject_request",
        f"idx={idx}, investor={investor_name}, type={tx['type']}, amount={amount}",
    )

    notify_investor_request_decision(
        users=users,
        investor_name=investor_name,
        tx_type=tx["type"],
        amount=amount,
        status="rejected",
        date_str=decision_date_str,
        decision_note=decision_note,
    )

# ============================================================
# STREAMLIT APP
# ============================================================

st.set_page_config(page_title="Northeast Capitol", layout="wide")

if "users" not in st.session_state:
    st.session_state["users"] = load_users()
if "user" not in st.session_state:
    st.session_state["user"] = None
if "demo_mode" not in st.session_state:
    st.session_state["demo_mode"] = False

if st.session_state["user"] is None:
    login_screen()

users = st.session_state["users"]
data = load_data()
st.session_state["data"] = data
settings = get_settings(data)

user = st.session_state["user"]
role = user["role"]
investor_name_for_user = user.get("investor_name")
currency_symbol = DEFAULT_CURRENCY

# Header
st.title("Northeast Capitol")

u_rec = users.get(user["username"], {})
prev_login = u_rec.get("prev_login")
if prev_login:
    st.caption(f"Last login (previous): {prev_login}")
elif u_rec.get("last_login"):
    st.caption(f"Last login: {u_rec.get('last_login')}")

# Sidebar
st.sidebar.write(f"Logged in as: **{user['username']}** ({role})")

if st.sidebar.button("Refresh app"):
    st.rerun()

if st.sidebar.button("Logout"):
    st.session_state["user"] = None
    st.experimental_set_query_params()
    st.rerun()

unread_count = count_unread_notifications(data, user["username"])
st.sidebar.markdown(f"🔔 Notifications: **{unread_count}**")

st.sidebar.header("Session")
demo_checkbox = st.sidebar.checkbox("Demo mode (no saving this session)", value=is_demo_mode())
st.session_state["demo_mode"] = demo_checkbox

# Navigation
if role == "admin":
    menu_options = [
        "Dashboard / Report",
        "Approvals",
        "P&L Calendar",
        "Month-end closing (30% fee)",
        "Company transfer / withdraw",
        "Transactions",
        "News / Advertisement board",
        "Messages / Notifications",
        "User management",
        "System settings",
        "Activity log",
    ]
else:
    menu_options = [
        "My dashboard",
        "My P&L calendar",
        "News / Announcement board",
        "My profile",
        "My transactions",
        "Deposit / Withdraw",
        "Messages / Chat",
    ]

nav_page = st.sidebar.radio("Menu", menu_options)

# ============================================================
# ADMIN PAGES
# ============================================================

if role == "admin" and nav_page == "Dashboard / Report":
    st.subheader("Admin Dashboard / Report")

    equity = data["equity"]
    nav_value = get_nav(data)
    investor_rows = get_investor_table(data)
    total_pl_all = sum(row["P/L"] for row in investor_rows)

    pending = get_pending_requests(data)
    num_dep_req = sum(1 for r in pending if r["tx"]["type"] == "deposit_request")
    num_wd_req = sum(1 for r in pending if r["tx"]["type"] == "withdrawal_request")

    last_backup = settings.get("last_backup")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Equity (USD)", format_currency(equity, currency_symbol))
    c2.metric("Current NAV (internal)", f"{nav_value:,.4f}")
    c3.metric("Total P/L this period (all investors)", format_currency(total_pl_all, currency_symbol))
    c4.metric("Pending requests", f"{num_dep_req} deposit / {num_wd_req} withdraw")

    if last_backup:
        st.caption(f"Last backup (manual mark): {last_backup}")
    else:
        st.caption("Last backup: not recorded (mark it in System settings).")

    st.markdown("### Investors")
    if not investor_rows:
        st.info("No investors yet. Create investor users under 'User management'.")
    else:
        df = pd.DataFrame(investor_rows)
        df_display = df.drop(columns=["Units"])
        st.dataframe(df_display, use_container_width=True)

        csv_buffer = StringIO()
        headers = ["Investor", "Balance", "Net Deposit", "P/L"]
        csv_buffer.write(",".join(headers) + "\n")
        for _, r in df_display.iterrows():
            csv_buffer.write(",".join(str(r[h]) for h in headers) + "\n")

        st.download_button(
            label="Download investor summary CSV",
            data=csv_buffer.getvalue(),
            file_name="investors_summary.csv",
            mime="text/csv",
        )

    st.markdown("### Equity History")
    eq_history = get_equity_history(data)
    if not eq_history:
        st.info("No equity history yet. Use P&L Calendar to record P&L and equity updates.")
    else:
        df_eq = pd.DataFrame(eq_history)
        df_eq["date"] = pd.to_datetime(df_eq["date"])
        df_eq = df_eq.set_index("date")
        st.line_chart(df_eq["equity"])

elif role == "admin" and nav_page == "Approvals":
    st.subheader("Approve Deposit / Withdrawal Requests")

    pending = get_pending_requests(data)
    if not pending:
        st.info("No pending requests.")
    else:
        options = list(range(len(pending)))

        def fmt(i: int) -> str:
            rec = pending[i]
            idx = rec["index"]
            tx = rec["tx"]
            return f"#{idx} | {tx['date']} | {tx['type']} | {tx['investor']} | {tx['amount']}"

        selected = st.selectbox(
            "Select a request",
            options=options,
            format_func=fmt,
        )
        rec = pending[selected]
        idx = rec["index"]
        tx = rec["tx"]

        st.markdown("#### Request details")
        st.write(f"- Index: **{idx}**")
        st.write(f"- Type: **{tx['type']}**")
        st.write(f"- Investor: **{tx['investor']}**")
        st.write(f"- Amount: **{format_currency(tx['amount'], currency_symbol)}**")
        st.write(f"- Date requested: **{tx['date']}**")
        st.write(f"- Bank details: {tx.get('bank_details') or '-'}")
        st.write(f"- Remark: {tx.get('note') or '-'}")

        with st.form("approval_form"):
            decision_note = st.text_area("Decision remark (optional)")
            decision_date = st.date_input("Approval / decision date", datetime.now())
            col_a, col_b = st.columns(2)
            approve_btn = col_a.form_submit_button("Approve")
            reject_btn = col_b.form_submit_button("Reject")

        if approve_btn:
            try:
                approve_request(
                    data=data,
                    users=users,
                    idx=idx,
                    admin_username=user["username"],
                    decision_note=decision_note,
                    approval_date_str=decision_date.strftime("%Y-%m-%d"),
                )
                st.success("Request approved and transaction recorded.")
                st.rerun()
            except Exception as e:
                st.error(f"Error approving request: {e}")

        if reject_btn:
            try:
                reject_request(
                    data=data,
                    users=users,
                    idx=idx,
                    admin_username=user["username"],
                    decision_note=decision_note,
                    decision_date_str=decision_date.strftime("%Y-%m-%d"),
                )
                st.success("Request rejected.")
                st.rerun()
            except Exception as e:
                st.error(f"Error rejecting request: {e}")

elif role == "admin" and nav_page == "P&L Calendar":
    st.subheader("P&L Calendar")

    today = datetime.now().date()

    st.markdown("### Record daily P&L (and update equity)")
    with st.form("daily_pl_form"):
        pl_date = st.date_input("Date", value=today)
        daily_pl = st.number_input(
            "P&L for this date (USD, negative for loss)",
            min_value=-1_000_000_000.0,
            max_value=1_000_000_000.0,
            value=0.0,
            step=100.0,
            format="%.2f",
        )
        submitted_pl = st.form_submit_button("Apply P&L and update equity")

    if submitted_pl:
        d_str = pl_date.strftime("%Y-%m-%d")
        old_equity = data["equity"]
        new_equity = old_equity + daily_pl
        data["equity"] = new_equity

        set_daily_pl(data, d_str, daily_pl)

        tx = {
            "date": d_str,
            "type": "equity_update",
            "old_equity": old_equity,
            "new_equity": new_equity,
            "profit_loss": daily_pl,
        }
        data["transactions"].append(tx)
        save_data(data)

        add_audit_entry(
            data,
            user["username"],
            "update_equity_by_pl",
            f"date={d_str}, old={old_equity}, pl={daily_pl}, new={new_equity}",
        )

        st.success(
            f"P&L of {format_currency(daily_pl, currency_symbol)} applied.\n\n"
            f"Equity changed from {format_currency(old_equity, currency_symbol)} "
            f"to {format_currency(new_equity, currency_symbol)}."
        )
        st.rerun()

    st.markdown("---")
    tab_week, tab_month, tab_year, tab_all = st.tabs(["Week", "Month", "Year", "All time"])

    with tab_week:
        st.markdown("#### Weekly P&L")
        base_date = st.date_input("Any date in week", value=today, key="week_date")
        start_week = base_date - timedelta(days=base_date.weekday())
        end_week = start_week + timedelta(days=6)
        total_week_pl = get_range_pl(data, start_week, end_week)
        st.metric("Total P&L (this week)", format_currency(total_week_pl, currency_symbol))
        st.caption(f"From {start_week} to {end_week}")

    with tab_month:
        st.markdown("#### Monthly P&L calendar")
        this_year = today.year
        years = list(range(this_year - 5, this_year + 2))
        col_y, col_m = st.columns(2)
        with col_y:
            year_sel = st.selectbox("Year", years, index=years.index(this_year))
        with col_m:
            month_sel = st.selectbox(
                "Month",
                list(range(1, 13)),
                index=today.month - 1,
                format_func=lambda m: datetime(2000, m, 1).strftime("%B"),
            )

        total_month_pl, daily_map = get_month_pl(data, year_sel, month_sel)
        monthly_goal = float(settings.get("pl_monthly_goal", 0.0))

        c1, c2 = st.columns(2)
        c1.metric("Total P&L (this month)", format_currency(total_month_pl, currency_symbol))
        with c2:
            new_goal = st.number_input(
                "Monthly goal (USD)",
                min_value=0.0,
                value=float(monthly_goal),
                step=100.0,
            )
            if st.button("Save monthly goal", key="save_goal_month"):
                settings["pl_monthly_goal"] = float(new_goal)
                save_data(data)
                add_audit_entry(
                    data,
                    user["username"],
                    "update_pl_goal",
                    f"monthly_goal={new_goal}",
                )
                st.success("Monthly goal updated.")
                st.rerun()

        if monthly_goal > 0:
            progress = total_month_pl / monthly_goal
            progress = max(0.0, min(progress, 1.0))
            st.progress(
                progress,
                text=f"{format_currency(total_month_pl, currency_symbol)} / "
                f"{format_currency(monthly_goal, currency_symbol)}",
            )

        draw_month_calendar(year_sel, month_sel, daily_map)

    with tab_year:
        st.markdown("#### Yearly P&L")
        this_year = today.year
        years = list(range(this_year - 5, this_year + 2))
        year_sel_y = st.selectbox("Year", years, index=years.index(this_year), key="year_sel_y")
        total_year_pl = get_year_pl(data, year_sel_y)
        st.metric("Total P&L (this year)", format_currency(total_year_pl, currency_symbol))

    with tab_all:
        st.markdown("#### All-time P&L")
        all_pl = get_all_time_pl(data)
        st.metric("Total P&L (all time)", format_currency(all_pl, currency_symbol))

elif role == "admin" and nav_page == "Month-end closing (30% fee)":
    st.subheader("Month-end Closing – Declare period & 30% processing fee")

    investor_rows = get_investor_table(data)
    if not investor_rows:
        st.info("No investors yet.")
    else:
        default_fee = settings.get("default_fee_rate", DEFAULT_FEE_RATE)
        fee_rate = st.number_input(
            "Fee rate for this month (e.g. 0.30 = 30%)",
            min_value=0.0,
            max_value=1.0,
            value=float(default_fee),
            step=0.05,
        )

        today = datetime.now().date()
        first_of_month = today.replace(day=1)

        c1, c2, c3 = st.columns(3)
        with c1:
            period_start = st.date_input("Period start (current month FROM)", value=first_of_month)
        with c2:
            period_end = st.date_input("Period end (current month TO)", value=today)
        with c3:
            closing_date = st.date_input("Closing date", value=today)

        if period_end < period_start:
            st.error("Period end date cannot be before start date.")
        else:
            pool_period_pl = get_range_pl(data, period_start, period_end)
            st.metric(
                "Pool P&L for this period",
                format_currency(pool_period_pl, currency_symbol),
            )
            st.caption(
                f"From {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')} "
                "based on the daily P&L records."
            )

            preview_rows, nav_value = get_monthly_closing_preview(data, fee_rate)
            if not preview_rows:
                st.info("No investors to charge.")
            else:
                st.markdown("#### Preview – P&L this period and fee")
                df_prev = pd.DataFrame(preview_rows)
                st.dataframe(df_prev, use_container_width=True)

                total_fee_amt = float(df_prev["Proposed fee amount"].sum())
                st.write(
                    f"**Total fee to Company ({COMPANY_INVESTOR_NAME}) at {fee_rate*100:.0f}% "
                    f"on this period's positive P&L:** {format_currency(total_fee_amt, currency_symbol)} "
                    f"(NAV used: {nav_value:.4f})"
                )

                apply = st.button("Apply month-end closing now")

                if apply:
                    summary = apply_monthly_closing(
                        data=data,
                        fee_rate=fee_rate,
                        closing_date_str=closing_date.strftime("%Y-%m-%d"),
                        period_start_str=period_start.strftime("%Y-%m-%d"),
                        period_end_str=period_end.strftime("%Y-%m-%d"),
                        pool_period_pl=pool_period_pl,
                    )
                    add_audit_entry(
                        data,
                        user["username"],
                        "month_end_closing",
                        f"closing_date={closing_date}, "
                        f"period={period_start}..{period_end}, "
                        f"fee_rate={fee_rate}, total_fee={summary['total_fee_amount']}",
                    )
                    st.success(
                        "Month-end closing applied.\n\n"
                        f"Period: {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}\n"
                        f"Pool P&L this period: {format_currency(pool_period_pl, currency_symbol)}\n"
                        f"- Total fee amount: {format_currency(summary['total_fee_amount'], currency_symbol)}\n"
                        f"- Total fee units moved to '{COMPANY_INVESTOR_NAME}': {summary['total_fee_units']:.4f}\n"
                        "- Next month starts with P&L reset (net-of-fee) for each investor."
                    )
                    st.rerun()

elif role == "admin" and nav_page == "Company transfer / withdraw":
    st.subheader("Company Money: Transfer / Withdraw")

    ensure_company_investor(data)
    nav_value = get_nav(data)
    if nav_value <= 0:
        nav_value = 1.0

    company_units = data["investors"][COMPANY_INVESTOR_NAME]["units"]
    company_balance = company_units * nav_value

    c1, c2 = st.columns(2)
    c1.metric("Company internal units", f"{company_units:,.4f}")
    c2.metric("Company balance (USD est.)", format_currency(company_balance, currency_symbol))

    tab1, tab2 = st.tabs(["Transfer to investor", "Withdraw company money"])

    with tab1:
        st.markdown("#### Transfer company money to an investor")

        investor_rows = get_investor_table(data)
        target_names = [r["Investor"] for r in investor_rows if r["Investor"] != COMPANY_INVESTOR_NAME]

        if not target_names:
            st.info("No investors available to receive transfers.")
        else:
            with st.form("company_transfer_form"):
                target_investor = st.selectbox("Transfer to investor", target_names)
                amount = st.number_input(
                    "Transfer amount (USD)",
                    min_value=0.0,
                    step=100.0,
                )
                note = st.text_area("Remark (optional)")
                date_obj = st.date_input("Transfer date", value=datetime.now())
                submitted = st.form_submit_button("Transfer from Company")

            if submitted:
                if amount <= 0:
                    st.error("Amount must be positive.")
                else:
                    nav_now = get_nav(data)
                    if nav_now <= 0:
                        nav_now = 1.0
                    units_to_give = amount / nav_now if nav_now != 0 else 0.0

                    ensure_company_investor(data)
                    company_units_now = data["investors"][COMPANY_INVESTOR_NAME]["units"]

                    if units_to_give > company_units_now + 1e-9:
                        st.error(
                            f"Not enough Company units. Tried to give {units_to_give:.4f}, "
                            f"but Company only has {company_units_now:.4f} units."
                        )
                    else:
                        data["investors"][COMPANY_INVESTOR_NAME]["units"] -= units_to_give
                        data["investors"][target_investor]["units"] += units_to_give

                        date_str = date_obj.strftime("%Y-%m-%d")
                        tx = {
                            "date": date_str,
                            "type": "company_transfer",
                            "from_investor": COMPANY_INVESTOR_NAME,
                            "investor": target_investor,
                            "amount": amount,
                            "units": units_to_give,
                            "nav": nav_now,
                            "note": note,
                            "created_by": user["username"],
                        }
                        data["transactions"].append(tx)
                        save_data(data)

                        add_audit_entry(
                            data,
                            user["username"],
                            "company_transfer",
                            f"to={target_investor}, amount={amount}, units={units_to_give}, nav={nav_now}",
                        )

                        st.success(
                            f"Transferred {format_currency(amount, currency_symbol)} "
                            f"({units_to_give:.4f} internal units) "
                            f"from Company to {target_investor}."
                        )
                        st.rerun()

    with tab2:
        st.markdown("#### Withdraw company money from the pool")

        with st.form("company_withdraw_form"):
            amount_w = st.number_input(
                "Withdrawal amount (USD)",
                min_value=0.0,
                step=100.0,
            )
            bank_info = st.text_input("Bank / reference (optional)")
            note_w = st.text_area("Remark (optional)")
            date_obj_w = st.date_input("Withdrawal date", value=datetime.now())
            submitted_w = st.form_submit_button("Record company withdrawal")

        if submitted_w:
            if amount_w <= 0:
                st.error("Amount must be positive.")
            else:
                nav_now = get_nav(data)
                if nav_now <= 0:
                    nav_now = 1.0
                ensure_company_investor(data)
                company_units_now = data["investors"][COMPANY_INVESTOR_NAME]["units"]
                units_to_deduct = amount_w / nav_now if nav_now != 0 else 0.0

                if units_to_deduct > company_units_now + 1e-9:
                    st.error(
                        f"Not enough Company units for this withdrawal. "
                        f"Needed {units_to_deduct:.4f}, "
                        f"but only {company_units_now:.4f} are available."
                    )
                else:
                    data["investors"][COMPANY_INVESTOR_NAME]["units"] = company_units_now - units_to_deduct
                    data["equity"] -= amount_w

                    date_str_w = date_obj_w.strftime("%Y-%m-%d")
                    tx_w = {
                        "date": date_str_w,
                        "type": "company_withdrawal",
                        "from_investor": COMPANY_INVESTOR_NAME,
                        "amount": amount_w,
                        "units": -units_to_deduct,
                        "nav": nav_now,
                        "bank_details": bank_info,
                        "note": note_w,
                        "created_by": user["username"],
                    }
                    data["transactions"].append(tx_w)
                    save_data(data)

                    add_audit_entry(
                        data,
                        user["username"],
                        "company_withdrawal",
                        f"amount={amount_w}, units={units_to_deduct}, nav={nav_now}",
                    )

                    st.success(
                        f"Recorded company withdrawal of {format_currency(amount_w, currency_symbol)} "
                        f"({units_to_deduct:.4f} internal units)."
                    )
                    st.rerun()

elif role == "admin" and nav_page == "Transactions":
    st.subheader("Transactions Log")

    txs = data["transactions"]
    if not txs:
        st.info("No transactions yet.")
    else:
        df_tx = pd.DataFrame(txs)

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("From date", value=datetime(2024, 1, 1))
        with col2:
            end_date = st.date_input("To date", value=datetime.now())

        def within_range(row_date_str: str) -> bool:
            try:
                d = datetime.strptime(row_date_str, "%Y-%m-%d").date()
            except Exception:
                return True
            return start_date <= d <= end_date

        if "date" in df_tx.columns:
            mask = df_tx["date"].apply(within_range)
            df_tx = df_tx[mask]

        st.dataframe(df_tx, use_container_width=True)

        csv_buffer = StringIO()
        df_tx.to_csv(csv_buffer, index=False)
        st.download_button(
            label="Download filtered transactions (CSV)",
            data=csv_buffer.getvalue(),
            file_name="transactions_filtered.csv",
            mime="text/csv",
        )

elif role == "admin" and nav_page == "News / Advertisement board":
    st.subheader("News / Advertisement board (Admin)")

    st.markdown("Upload PNG / JPG / PDF files to show as announcements for investors.")

    uploaded_files = st.file_uploader(
        "Upload files",
        type=["png", "jpg", "jpeg", "pdf"],
        accept_multiple_files=True,
    )

    if uploaded_files:
        if st.button("Save uploaded news items"):
            for f in uploaded_files:
                file_id = uuid.uuid4().hex
                ext = os.path.splitext(f.name)[1]
                save_name = f"{file_id}{ext}"
                save_path = os.path.join(NEWS_DIR, save_name)
                with open(save_path, "wb") as out:
                    out.write(f.getbuffer())

                data["news"].append(
                    {
                        "id": file_id,
                        "title": f.name,
                        "filename": save_name,
                        "uploaded_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    }
                )
            save_data(data)
            st.success("News items saved.")
            st.rerun()

    if not data.get("news"):
        st.info("No news uploaded yet.")
    else:
        st.markdown("### Existing news")
        for item in sorted(data["news"], key=lambda x: x.get("uploaded_at", ""), reverse=True):
            title = item.get("title", "(no title)")
            uploaded_at = item.get("uploaded_at", "-")
            st.write(f"**{title}**  (uploaded: {uploaded_at})")

            filename = item.get("filename")
            if not filename:
                text_content = item.get("content")
                if text_content:
                    st.write(text_content)
                st.markdown("---")
                continue

            file_path = os.path.join(NEWS_DIR, filename)
            if os.path.exists(file_path):
                ext = os.path.splitext(file_path)[1].lower()
                with open(file_path, "rb") as f:
                    content = f.read()
                if ext in [".png", ".jpg", ".jpeg"]:
                    st.image(content, use_container_width=True)
                else:
                    st.download_button(
                        label="Download file",
                        data=content,
                        file_name=title or filename,
                    )
            else:
                st.warning("File not found on server.")
            st.markdown("---")

elif role == "admin" and nav_page == "Messages / Notifications":
    st.subheader("Messages / Notifications (Admin)")

    all_usernames = list(users.keys())
    investor_usernames = [u for u, rec in users.items() if rec.get("role") == "investor"]

    tab1, tab2, tab3 = st.tabs(["Broadcast to all investors", "Direct message", "Inbox"])

    with tab1:
        st.markdown("#### Broadcast announcement to all investors")
        with st.form("broadcast_form"):
            title = st.text_input("Title")
            msg = st.text_area("Message")
            send = st.form_submit_button("Send broadcast")
        if send:
            if not title or not msg:
                st.error("Title and message are required.")
            else:
                add_notification(
                    data,
                    from_user=user["username"],
                    to_usernames=investor_usernames,
                    title=title,
                    message=msg,
                    ntype="broadcast",
                )
                st.success("Broadcast sent.")

    with tab2:
        st.markdown("#### Direct message to a specific user")
        with st.form("direct_form"):
            target = st.selectbox("Send to", all_usernames)
            title = st.text_input("Title", key="dm_title")
            msg = st.text_area("Message", key="dm_msg")
            send = st.form_submit_button("Send message")
        if send:
            if not title or not msg:
                st.error("Title and message are required.")
            else:
                add_notification(
                    data,
                    from_user=user["username"],
                    to_usernames=[target],
                    title=title,
                    message=msg,
                    ntype="chat",
                )
                st.success("Message sent.")

    with tab3:
        st.markdown("#### Inbox")
        inbox = get_user_notifications(data, user["username"])
        if not inbox:
            st.info("No messages yet.")
        else:
            if st.button("Mark all as read"):
                mark_all_notifications_read_for_user(data, user["username"])
                st.rerun()
            for n in sorted(inbox, key=lambda x: x["datetime"], reverse=True):
                read = user["username"] in n.get("read_by", [])
                prefix = "" if read else "🆕 "
                st.markdown(
                    f"{prefix}**{n['title']}**  "
                    f"({n['datetime']} from {n['from']})"
                )
                st.write(n["message"])
                st.markdown("---")

elif role == "admin" and nav_page == "User management":
    st.subheader("User management")

    # -----------------------
    # Existing users table
    # -----------------------
    st.markdown("### Existing users")
    rows = []
    for uname, u in users.items():
        rows.append(
            {
                "username": uname,
                "role": u.get("role"),
                "investor_name": u.get("investor_name"),
                "email": u.get("email"),
                "phone": u.get("phone"),
                "active": u.get("active", True),
            }
        )
    if rows:
        df_users = pd.DataFrame(rows)
        st.dataframe(df_users, use_container_width=True)
    else:
        st.info("No users yet. Use the form below to create one.")

    st.markdown("---")

    # =========================================================
    # A. CREATE NEW USER
    # =========================================================
    st.markdown("### Create new user")

    with st.form("create_user_form"):
        c_username = st.text_input("New username (for login display)")
        c_password = st.text_input("Password", type="password")
        c_role = st.selectbox("Role", ["admin", "investor"], key="create_role")
        c_investor_name = st.text_input("Investor name (for investor role)")
        c_email = st.text_input("Email (login)")
        c_phone = st.text_input("Phone number")
        c_active = st.checkbox("Active", value=True, key="create_active")
        c_submitted = st.form_submit_button("Create user")

    if c_submitted:
        if not c_username:
            st.error("Username is required.")
        elif c_username in users:
            st.error("This username already exists. Use the 'Update user' section below.")
        elif c_role == "investor" and not c_investor_name:
            st.error("Investor name is required for investor role.")
        elif not c_email:
            st.error("Email is required.")
        elif not c_phone:
            st.error("Phone number is required.")
        elif not c_password:
            st.error("Password is required for new user.")
        elif not is_strong_password(c_password):
            st.error("Password must be at least 8 characters and contain letters and numbers.")
        else:
            new_user = {
                "password": c_password,  # (plain for now; you can switch to hash later)
                "role": c_role,
                "investor_name": c_investor_name if c_role == "investor" else None,
                "email": c_email,
                "phone": c_phone,
                "active": c_active,
                "investor_name_locked": False,
                "username_locked": False,
                "last_login": None,
                "prev_login": None,
            }
            users[c_username] = new_user
            save_users(users)
            st.session_state["users"] = users

            if c_role == "investor" and c_investor_name:
                data["investors"].setdefault(c_investor_name, {"units": 0.0})
                save_data(data)

            st.success(f"User '{c_username}' created.")
            st.rerun()

    st.markdown("---")

    # =========================================================
    # B. UPDATE EXISTING USER
    # =========================================================
    st.markdown("### Update existing user")

    if not users:
        st.info("No users to update.")
    else:
        # pick an existing user to edit
        usernames_sorted = sorted(users.keys())
        u_selected_name = st.selectbox("Select user to update", usernames_sorted)

        u_selected = users[u_selected_name]

        with st.form("update_user_form"):
            u_password = st.text_input(
                "New password (leave blank to keep current)",
                type="password",
            )
            u_role = st.selectbox(
                "Role",
                ["admin", "investor"],
                index=0 if u_selected.get("role") == "admin" else 1,
                key="update_role",
            )
            u_investor_name = st.text_input(
                "Investor name (for investor role)",
                value=u_selected.get("investor_name") or "",
            )
            u_email = st.text_input(
                "Email (login)",
                value=u_selected.get("email") or "",
            )
            u_phone = st.text_input(
                "Phone number",
                value=u_selected.get("phone") or "",
            )
            u_active = st.checkbox(
                "Active",
                value=u_selected.get("active", True),
                key="update_active",
            )

            u_submitted = st.form_submit_button("Save changes")

        if u_submitted:
            if u_role == "investor" and not u_investor_name:
                st.error("Investor name is required for investor role.")
            elif not u_email:
                st.error("Email is required.")
            elif not u_phone:
                st.error("Phone number is required.")
            elif u_password and not is_strong_password(u_password):
                st.error("Password must be at least 8 characters and contain letters and numbers.")
            else:
                # update existing record
                if u_password:
                    u_selected["password"] = u_password

                u_selected["role"] = u_role
                u_selected["investor_name"] = u_investor_name if u_role == "investor" else None
                u_selected["email"] = u_email
                u_selected["phone"] = u_phone
                u_selected["active"] = u_active
                u_selected.setdefault("investor_name_locked", False)
                u_selected.setdefault("username_locked", False)

                users[u_selected_name] = u_selected
                save_users(users)
                st.session_state["users"] = users

                if u_role == "investor" and u_investor_name:
                    data["investors"].setdefault(u_investor_name, {"units": 0.0})
                    save_data(data)

                st.success(f"User '{u_selected_name}' updated.")
                st.rerun()




elif role == "admin" and nav_page == "System settings":
    st.subheader("System settings")

    st.markdown("### General")
    with st.form("settings_form"):
        default_fee_rate = st.number_input(
            "Default performance fee rate (e.g. 0.30 = 30%)",
            min_value=0.0,
            max_value=1.0,
            value=float(settings.get("default_fee_rate", DEFAULT_FEE_RATE)),
            step=0.05,
        )
        last_backup = settings.get("last_backup")
        if last_backup:
            st.caption(f"Currently recorded last backup date: {last_backup}")
        backup_date = st.date_input("Mark backup done on date", value=datetime.now())
        submitted = st.form_submit_button("Save settings")

    if submitted:
        settings["default_fee_rate"] = float(default_fee_rate)
        settings["last_backup"] = backup_date.strftime("%Y-%m-%d")
        save_data(data)
        st.success("Settings saved.")

elif role == "admin" and nav_page == "Activity log":
    st.subheader("Activity log")

    ensure_audit_log(data)
    if not data["audit_log"]:
        st.info("No activity recorded yet.")
    else:
        df_log = pd.DataFrame(data["audit_log"])
        df_log = df_log.sort_values("time", ascending=False)
        st.dataframe(df_log, use_container_width=True)

# ============================================================
# INVESTOR PAGES
# ============================================================

if role == "investor" and nav_page == "My dashboard":
    st.subheader("My dashboard")

    if not investor_name_for_user or investor_name_for_user not in data["investors"]:
        st.warning("Your investor profile is not linked yet. Please contact admin.")
    else:
        nav_value = get_nav(data)
        units = data["investors"][investor_name_for_user]["units"]
        balance = units * nav_value

        net_deposits = get_net_deposits(data)
        net_dep = net_deposits.get(investor_name_for_user, 0.0)

        period_pl_all = get_current_period_pl_by_investor(data)
        pl = period_pl_all.get(investor_name_for_user, 0.0)

        c1, c2, c3 = st.columns(3)
        c1.metric("Balance", format_currency(balance, currency_symbol))
        c2.metric("Net deposit", format_currency(net_dep, currency_symbol))
        c3.metric("P/L this period", format_currency(pl, currency_symbol))

        st.markdown("### Balance vs Net deposit")
        df_chart = pd.DataFrame(
            {
                "Metric": ["Net Deposit", "Balance"],
                "Amount": [net_dep, balance],
            }
        )
        df_chart = df_chart.set_index("Metric")
        st.bar_chart(df_chart)

        st.markdown("### Recent transactions")
        my_txs = [
            tx for tx in data["transactions"]
            if tx.get("investor") == investor_name_for_user
            or tx.get("from_investor") == investor_name_for_user
        ]
        my_txs = sorted(my_txs, key=lambda x: x.get("date", ""), reverse=True)[:20]
        if not my_txs:
            st.info("No transactions yet.")
        else:
            st.dataframe(pd.DataFrame(my_txs), use_container_width=True)

elif role == "investor" and nav_page == "My P&L calendar":
    st.subheader("My P&L Calendar (view only)")

    if not investor_name_for_user or investor_name_for_user not in data["investors"]:
        st.warning("Your account is not linked to an investor profile. Please contact admin.")
    else:
        today = datetime.now().date()

        nav_value = get_nav(data)
        my_units = data["investors"][investor_name_for_user]["units"]
        my_balance = my_units * nav_value
        equity = data["equity"]
        share_ratio = my_balance / equity if equity > 0 else 0.0

        if share_ratio <= 0:
            st.info("No active units found for your account, so P&L share is currently zero.")
        else:
            tab_week, tab_month, tab_year, tab_all = st.tabs(
                ["Week", "Month", "Year", "All time"]
            )

            with tab_week:
                st.markdown("#### Weekly P&L (my share)")
                base_date = st.date_input("Any date in week", value=today, key="my_week_date")
                start_week = base_date - timedelta(days=base_date.weekday())
                end_week = start_week + timedelta(days=6)
                pool_week_pl = get_range_pl(data, start_week, end_week)
                my_week_pl = pool_week_pl * share_ratio
                st.metric("My P&L this week", format_currency(my_week_pl, currency_symbol))
                st.caption(f"Calculated from pool P&L × your share of equity ({share_ratio:.2%}).")

            with tab_month:
                st.markdown("#### Monthly P&L calendar (my share)")
                this_year = today.year
                years = list(range(this_year - 5, this_year + 2))
                col_y, col_m = st.columns(2)
                with col_y:
                    year_sel = st.selectbox(
                        "Year", years, index=years.index(this_year), key="my_pl_year"
                    )
                with col_m:
                    month_sel = st.selectbox(
                        "Month",
                        list(range(1, 13)),
                        index=today.month - 1,
                        format_func=lambda m: datetime(2000, m, 1).strftime("%B"),
                        key="my_pl_month",
                    )

                pool_month_pl, pool_daily_map = get_month_pl(data, year_sel, month_sel)
                my_daily_map = {d: v * share_ratio for d, v in pool_daily_map.items()}
                my_month_pl = pool_month_pl * share_ratio

                st.metric(
                    "My total P&L this month",
                    format_currency(my_month_pl, currency_symbol),
                )
                draw_month_calendar(year_sel, month_sel, my_daily_map)

            with tab_year:
                st.markdown("#### Yearly P&L (my share)")
                this_year = today.year
                years = list(range(this_year - 5, this_year + 2))
                year_sel_y = st.selectbox(
                    "Year", years, index=years.index(this_year), key="my_pl_year_only"
                )
                pool_year_pl = get_year_pl(data, year_sel_y)
                my_year_pl = pool_year_pl * share_ratio
                st.metric(
                    "My total P&L this year",
                    format_currency(my_year_pl, currency_symbol),
                )

            with tab_all:
                st.markdown("#### All-time P&L (my share)")
                pool_all_pl = get_all_time_pl(data)
                my_all_pl = pool_all_pl * share_ratio
                st.metric(
                    "My total P&L (since records started)",
                    format_currency(my_all_pl, currency_symbol),
                )

elif role == "investor" and nav_page == "News / Announcement board":
    st.subheader("News / Announcement board")

    if not data.get("news"):
        st.info("No announcements yet.")
    else:
        for item in sorted(data["news"], key=lambda x: x.get("uploaded_at", ""), reverse=True):
            title = item.get("title", "(no title)")
            uploaded_at = item.get("uploaded_at", "-")
            st.write(f"**{title}**  (uploaded: {uploaded_at})")

            filename = item.get("filename")
            if not filename:
                text_content = item.get("content")
                if text_content:
                    st.write(text_content)
                st.markdown("---")
                continue

            file_path = os.path.join(NEWS_DIR, filename)
            if os.path.exists(file_path):
                ext = os.path.splitext(file_path)[1].lower()
                with open(file_path, "rb") as f:
                    content = f.read()
                if ext in [".png", ".jpg", ".jpeg"]:
                    st.image(content, use_container_width=True)
                else:
                    st.download_button(
                        label="Download file",
                        data=content,
                        file_name=title or filename,
                    )
            else:
                st.warning("File not found on server.")
            st.markdown("---")

elif role == "investor" and nav_page == "My profile":
    st.subheader("My profile")

    uname = user["username"]
    u = users.get(uname, {})

    st.write(f"**Username:** {uname}")
    st.write(f"**Role:** {u.get('role')}")
    st.write(f"**Current investor name:** {u.get('investor_name') or '-'}")

    with st.form("profile_form"):
        email_new = st.text_input("Email (login)", value=u.get("email") or "")
        phone_new = st.text_input("Phone", value=u.get("phone") or "")

        investor_name_locked = u.get("investor_name_locked", False)
        username_locked = u.get("username_locked", False)

        investor_name_new = st.text_input(
            "Investor name (you can change one time)",
            value=u.get("investor_name") or "",
            disabled=investor_name_locked,
        )
        username_new = st.text_input(
            "Username (you can change one time)",
            value=uname,
            disabled=username_locked,
        )

        submitted = st.form_submit_button("Save profile")

    if submitted:
        if not email_new:
            st.error("Email cannot be empty.")
        elif not phone_new:
            st.error("Phone cannot be empty.")
        else:
            old_investor_name = u.get("investor_name")

            if not username_locked and username_new != uname:
                if username_new in users:
                    st.error("New username already exists.")
                    st.stop()
                users[username_new] = users.pop(uname)
                uname = username_new
                user["username"] = username_new
                u = users[username_new]
                u["username_locked"] = True

            if u.get("role") == "investor" and not investor_name_locked and investor_name_new != old_investor_name:
                try:
                    if old_investor_name and old_investor_name in data["investors"]:
                        rename_investor_everywhere(data, old_investor_name, investor_name_new)
                    else:
                        data["investors"].setdefault(investor_name_new, {"units": 0.0})
                        save_data(data)
                    u["investor_name"] = investor_name_new
                    u["investor_name_locked"] = True
                    user["investor_name"] = investor_name_new
                except Exception as e:
                    st.error(f"Error renaming investor: {e}")
                    st.stop()

            u["email"] = email_new
            u["phone"] = phone_new
            users[uname] = u
            save_users(users)
            st.session_state["users"] = users
            st.session_state["user"] = user

            st.success("Profile updated.")
            st.rerun()

elif role == "investor" and nav_page == "My transactions":
    st.subheader("My transactions")

    if not investor_name_for_user:
        st.warning("Your account is not linked to an investor name. Please contact admin.")
    else:
        my_txs = [
            tx for tx in data["transactions"]
            if tx.get("investor") == investor_name_for_user
            or tx.get("from_investor") == investor_name_for_user
        ]
        if not my_txs:
            st.info("No transactions found.")
        else:
            df_my = pd.DataFrame(my_txs)

            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("From date", value=datetime(2024, 1, 1))
            with col2:
                end_date = st.date_input("To date", value=datetime.now())

            def within_range(row_date_str: str) -> bool:
                try:
                    d = datetime.strptime(row_date_str, "%Y-%m-%d").date()
                except Exception:
                    return True
                return start_date <= d <= end_date

            if "date" in df_my.columns:
                mask = df_my["date"].apply(within_range)
                df_my = df_my[mask]

            st.dataframe(df_my, use_container_width=True)

            csv_buffer = StringIO()
            df_my.to_csv(csv_buffer, index=False)
            st.download_button(
                label="Download statement (CSV)",
                data=csv_buffer.getvalue(),
                file_name="my_transactions.csv",
                mime="text/csv",
            )

elif role == "investor" and nav_page == "Deposit / Withdraw":
    st.subheader("Deposit / Withdraw Requests")

    if not investor_name_for_user:
        st.warning("Your account is not linked to an investor name. Please contact admin.")
    else:
        tab_dep, tab_wd, tab_status = st.tabs(["Deposit request", "Withdrawal request", "My requests"])

        with tab_dep:
            st.markdown("#### Submit deposit request")
            with st.form("dep_req_form"):
                amount = st.number_input("Deposit amount (USD)", min_value=0.0, step=100.0)
                date_obj = st.date_input("Deposit date", value=datetime.now())
                bank = st.text_input("Bank / payment details")
                note = st.text_area("Remark (optional)")
                submitted = st.form_submit_button("Submit deposit request")

            if submitted:
                if amount <= 0:
                    st.error("Amount must be positive.")
                else:
                    date_str = date_obj.strftime("%Y-%m-%d")
                    tx = {
                        "date": date_str,
                        "type": "deposit_request",
                        "investor": investor_name_for_user,
                        "amount": amount,
                        "status": "pending",
                        "bank_details": bank,
                        "note": note,
                        "created_by": user["username"],
                    }
                    data["transactions"].append(tx)
                    save_data(data)
                    add_audit_entry(
                        data,
                        user["username"],
                        "deposit_request",
                        f"amount={amount}, investor={investor_name_for_user}",
                    )
                    notify_admin_new_request(investor_name_for_user, "deposit_request", amount, date_str, bank, note)
                    st.success("Deposit request submitted.")
                    st.rerun()

        with tab_wd:
            st.markdown("#### Submit withdrawal request")
            with st.form("wd_req_form"):
                amount = st.number_input("Withdrawal amount (USD)", min_value=0.0, step=100.0)
                date_obj = st.date_input("Withdrawal date", value=datetime.now())
                bank = st.text_input("Bank / receiving bank")
                note = st.text_area("Remark (optional)")
                submitted = st.form_submit_button("Submit withdrawal request")

            if submitted:
                if amount <= 0:
                    st.error("Amount must be positive.")
                else:
                    date_str = date_obj.strftime("%Y-%m-%d")
                    tx = {
                        "date": date_str,
                        "type": "withdrawal_request",
                        "investor": investor_name_for_user,
                        "amount": amount,
                        "status": "pending",
                        "bank_details": bank,
                        "note": note,
                        "created_by": user["username"],
                    }
                    data["transactions"].append(tx)
                    save_data(data)
                    add_audit_entry(
                        data,
                        user["username"],
                        "withdrawal_request",
                        f"amount={amount}, investor={investor_name_for_user}",
                    )
                    notify_admin_new_request(investor_name_for_user, "withdrawal_request", amount, date_str, bank, note)
                    st.success("Withdrawal request submitted.")
                    st.rerun()

        with tab_status:
            st.markdown("#### My requests status")
            my_reqs = [
                tx
                for tx in data["transactions"]
                if tx.get("investor") == investor_name_for_user
                and tx["type"] in ("deposit_request", "withdrawal_request")
            ]
            if not my_reqs:
                st.info("No requests yet.")
            else:
                df_reqs = pd.DataFrame(my_reqs)
                st.dataframe(df_reqs, use_container_width=True)

elif role == "investor" and nav_page == "Messages / Chat":
    st.subheader("Messages / Chat")

    admins = [u for u, rec in users.items() if rec.get("role") == "admin"]

    tab_inbox, tab_send = st.tabs(["Inbox", "Send message to admin"])

    with tab_inbox:
        inbox = get_user_notifications(data, user["username"])
        if not inbox:
            st.info("No messages yet.")
        else:
            if st.button("Mark all as read"):
                mark_all_notifications_read_for_user(data, user["username"])
                st.rerun()
            for n in sorted(inbox, key=lambda x: x["datetime"], reverse=True):
                read = user["username"] in n.get("read_by", [])
                prefix = "" if read else "🆕 "
                st.markdown(
                    f"{prefix}**{n['title']}**  "
                    f"({n['datetime']} from {n['from']})"
                )
                st.write(n["message"])
                st.markdown("---")

    with tab_send:
        with st.form("send_admin_msg"):
            title = st.text_input("Title")
            msg = st.text_area("Message")
            submitted = st.form_submit_button("Send to admin")
        if submitted:
            if not title or not msg:
                st.error("Title and message are required.")
            else:
                add_notification(
                    data,
                    from_user=user["username"],
                    to_usernames=admins,
                    title=title,
                    message=msg,
                    ntype="chat",
                )
                st.success("Message sent to admin.")

