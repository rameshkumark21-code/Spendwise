import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
import json, uuid, re, calendar, time
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO

# ── PAGE CONFIG ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ClearSpend",
    page_icon="💳",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ── DESIGN TOKENS ──────────────────────────────────────────────────────────────
C = {
    "bg":          "#0d1117",
    "surface":     "#161b22",
    "surface2":    "#1c2333",
    "border":      "#30363d",
    "primary":     "#7c6df8",
    "primary_dim": "rgba(124,109,248,0.12)",
    "income":      "#00c896",
    "expense":     "#ff4f6d",
    "warning":     "#f0a500",
    "info":        "#58a6ff",
    "text":        "#e6edf3",
    "muted":       "#8b949e",
    "success":     "#3fb950",
}

# ── GOOGLE SHEETS CONSTANTS ────────────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SPREADSHEET_NAME = "ClearSpend"

HEADERS = {
    "Transactions": ["RowID","Date","Merchant","Amount","Type","Category",
                     "Subcategory","PaymentMethod","Tags","Notes","Source","AutoCat",
                     "RawTime","UPIRef","Account","OrderID","Remarks"],
    "Categories":   ["Category","Subcategory","Keywords","Icon"],
    "Budgets":      ["Category","MonthlyBudget"],
    "Settings":     ["Key","Value"],
    "EmailRules":   ["RuleName","Sender","SubjectContains","BodyTemplate",
                     "DateFormat","DefaultType","AccountLabel","Active",
                     "DryRun","LookbackDays","LastRun","LastImported"],
    "ParseErrors":  ["Timestamp","RuleName","Sender","Subject","BodySnippet","ErrorReason"],
}

PAYMENT_METHODS = ["UPI","Credit Card","Debit Card","Cash","Net Banking","Wallet","BNPL"]

DEFAULT_CATEGORIES = [
    ["Food & Dining",     "Restaurants",   "restaurant,cafe,dhaba,biryani,pizza,burger,shawarma,mcdonalds,kfc,subway,dominos,haldirams,barbeque", "🍽️"],
    ["Food & Dining",     "Delivery",      "swiggy,zomato,dunzo,magicpin,delivery,eatsure,licious", "🛵"],
    ["Food & Dining",     "Groceries",     "bigbasket,blinkit,zepto,grofers,dmart,reliance fresh,more,supermarket,grocery,vegetables,kiranas,jiomart", "🛒"],
    ["Food & Dining",     "Coffee & Tea",  "starbucks,cafe coffee,ccd,barista,third wave,chai,tea,coffee", "☕"],
    ["Transport",         "Fuel",          "bpcl,hp petrol,indian oil,iocl,shell,fuel,petrol,diesel,cng,pump,bharat petroleum,hindustan petro,vriddhi", "⛽"],
    ["Transport",         "Cab & Auto",    "ola,uber,rapido,namma yatri,auto,taxi,cab,meru,indrive,bluemart", "🚕"],
    ["Transport",         "Public",        "metro,bmrc,bus,bmtc,ksrtc,msrtc,local train,suburban,railway", "🚌"],
    ["Transport",         "Parking & Toll","parking,toll,fastag,fasttag,car park", "🅿️"],
    ["Shopping",          "Online",        "amazon,flipkart,meesho,snapdeal,ajio,myntra,nykaa,purplle,tata cliq,reliance,jio", "📦"],
    ["Shopping",          "Clothing",      "lifestyle,westside,pantaloons,max fashion,h&m,zara,uniqlo,clothing,apparel,fabindia", "👗"],
    ["Shopping",          "Electronics",   "croma,vijay sales,reliance digital,samsung,apple,asus,lenovo,electronics,gadget", "📱"],
    ["Shopping",          "Home & Kitchen","ikea,pepperfry,urban ladder,kitchen,household,home decor,furniture", "🏡"],
    ["Bills & Utilities", "Electricity",   "bescom,tata power,adani electricity,msedcl,electricity,current bill,eb,tneb,wbsedcl", "⚡"],
    ["Bills & Utilities", "Mobile & Net",  "jio,airtel,vi,vodafone,bsnl,act,hathway,recharge,broadband,internet,postpaid,prepaid", "📡"],
    ["Bills & Utilities", "Water & Gas",   "water,bwssb,gas,lpg,cylinder,indane,bharat gas,mahanagar gas", "🚿"],
    ["Bills & Utilities", "OTT & Subs",    "netflix,prime,hotstar,jiocinema,sonyliv,zee5,spotify,youtube,apple music,discord,notion,adobe", "📺"],
    ["Health",            "Pharmacy",      "apollo,medplus,1mg,pharmeasy,netmeds,medicine,pharmacy,tablet,injection,wellness", "💊"],
    ["Health",            "Doctor & Lab",  "hospital,clinic,doctor,diagnostics,blood test,scan,lab,consultation,pathology,fortis,manipal", "🏥"],
    ["Health",            "Fitness",       "gym,cult fit,crossfit,yoga,fitness,sports,swimming,fbb,gold's gym", "🏋️"],
    ["Entertainment",     "Movies",        "pvr,inox,bookmyshow,movie,cinepolis,carnival cinema", "🎬"],
    ["Entertainment",     "Events & Fun",  "concert,event,show,ticket,amusement,theme park,bowling,escape room", "🎟️"],
    ["Travel",            "Flights",       "indigo,air india,spicejet,goair,vistara,airline,flight,aviation,goindigo,akasa", "✈️"],
    ["Travel",            "Hotels",        "oyo,treebo,fab hotel,hotel,airbnb,stay,booking,marriott,ibis,lemon tree,taj,oberoi", "🏨"],
    ["Travel",            "Train & Bus",   "irctc,train,redbus,abhibus,yatra,makemytrip,goibibo,cleartrip", "🚂"],
    ["Education",         "Courses",       "udemy,coursera,unacademy,vedantu,byjus,course,class,workshop,training,skillshare,pluralsight", "📚"],
    ["Education",         "Books & Tools", "amazon kindle,crossword,landmark,book,kindle,novel,stationery", "📖"],
    ["Personal Care",     "Salon & Spa",   "salon,spa,haircut,beauty,grooming,nails,waxing,parlour,jawed habib,yash", "💇"],
    ["Rent & Housing",    "Rent",          "rent,pg,hostel,landlord,society maintenance,house rent,lease,security deposit", "🏠"],
    ["Investments",       "Mutual Funds",  "zerodha,groww,kuvera,coin,mirae,hdfc mf,sip,mutual fund,etf,paytm money,angel", "📈"],
    ["Investments",       "Deposits",      "fd,ppf,nsc,recurring deposit,rd,fixed deposit,post office", "🏦"],
    ["Gifts & Social",    "Gifts",         "gift,present,birthday,anniversary,party,wedding,wrapping", "🎁"],
    ["Gifts & Social",    "Donations",     "donation,charity,ngo,temple,church,mosque,trust,pm relief", "🤲"],
    ["Others",            "Miscellaneous", "", "📌"],
]

DEFAULT_SETTINGS = [
    ["currency_symbol", "₹"],
    ["currency_code",   "INR"],
    ["monthly_budget",  "30000"],
    ["app_name",        "ClearSpend"],
]


# ═══════════════════════════════════════════════════════════════════════════════
#  GOOGLE SHEETS LAYER
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_client():
    creds_dict = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

@st.cache_resource
def get_ss():
    client = get_client()
    try:
        return client.open(SPREADSHEET_NAME)
    except gspread.SpreadsheetNotFound:
        ss = client.create(SPREADSHEET_NAME)
        return ss

def _ensure_columns(ws, required_headers: list):
    """Append any missing column headers after the last existing column.
    Never modifies existing data rows — only appends to header row 1."""
    existing = ws.row_values(1)
    for h in required_headers:
        if h not in existing:
            col = len(existing) + 1
            ws.update_cell(1, col, h)
            existing.append(h)

def ensure_sheets():
    ss = get_ss()
    existing = [ws.title for ws in ss.worksheets()]
    for name, hdrs in HEADERS.items():
        if name not in existing:
            ws = ss.add_worksheet(title=name, rows=2000, cols=len(hdrs))
            ws.append_row(hdrs)
        else:
            # Safely add any new columns that did not exist before
            _ensure_columns(ss.worksheet(name), hdrs)
    cats = ss.worksheet("Categories")
    if len(cats.get_all_values()) <= 1:
        cats.append_rows(DEFAULT_CATEGORIES)
    setts = ss.worksheet("Settings")
    if len(setts.get_all_values()) <= 1:
        setts.append_rows(DEFAULT_SETTINGS)
    for title in ["Sheet1"]:
        try:
            ss.del_worksheet(ss.worksheet(title))
        except Exception:
            pass


# ── CRUD ───────────────────────────────────────────────────────────────────────

def _parse_dates(series):
    def parse_one(v):
        s = str(v).strip()
        if not s or s in ("nan","None","NaT",""):
            return pd.NaT
        if len(s) == 10 and s[4] == "-":
            try: return pd.Timestamp(s)
            except: pass
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%Y/%m/%d"):
            try: return pd.Timestamp(pd.to_datetime(s, format=fmt))
            except: pass
        try: return pd.Timestamp(pd.to_datetime(s, dayfirst=True))
        except: return pd.NaT
    return series.apply(parse_one)

@st.cache_data(ttl=20)
def load_transactions():
    ss = get_ss()
    data = ss.worksheet("Transactions").get_all_records()
    if not data:
        return pd.DataFrame(columns=HEADERS["Transactions"])
    df = pd.DataFrame(data)
    df["Date"]   = _parse_dates(df["Date"])
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
    return df

@st.cache_data(ttl=60)
def load_importlog():
    ss = get_ss()
    try:
        data = ss.worksheet("ImportLog").get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=120)
def load_categories():
    ss = get_ss()
    data = ss.worksheet("Categories").get_all_records()
    return pd.DataFrame(data) if data else pd.DataFrame(columns=HEADERS["Categories"])

@st.cache_data(ttl=120)
def load_budgets():
    ss = get_ss()
    data = ss.worksheet("Budgets").get_all_records()
    return pd.DataFrame(data) if data else pd.DataFrame(columns=HEADERS["Budgets"])

@st.cache_data(ttl=300)
def load_settings():
    ss = get_ss()
    data = ss.worksheet("Settings").get_all_records()
    if not data:
        return {k: v for k, v in DEFAULT_SETTINGS}
    return {r["Key"]: r["Value"] for r in data}

@st.cache_data(ttl=60)
def load_email_rules():
    ss = get_ss()
    try:
        data = ss.worksheet("EmailRules").get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame(columns=HEADERS["EmailRules"])
    except Exception:
        return pd.DataFrame(columns=HEADERS["EmailRules"])

@st.cache_data(ttl=30)
def load_parse_errors():
    ss = get_ss()
    try:
        data = ss.worksheet("ParseErrors").get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame(columns=HEADERS["ParseErrors"])
    except Exception:
        return pd.DataFrame(columns=HEADERS["ParseErrors"])

def trigger_run_now():
    """Write RUN flag to Settings sheet so Code.gs picks it up on next trigger."""
    ss  = get_ss()
    ws  = ss.worksheet("Settings")
    all_v = ws.get_all_values()
    found = False
    for i, row in enumerate(all_v[1:], start=2):
        if row and row[0] == "trigger_queue":
            ws.update_cell(i, 2, "RUN")
            found = True; break
    if not found:
        ws.append_row(["trigger_queue", "RUN"])
    st.cache_data.clear()

def _write_txn(row_dict):
    ss = get_ss()
    ws = ss.worksheet("Transactions")
    row = [row_dict.get(h, "") for h in HEADERS["Transactions"]]
    ws.append_row(row, value_input_option="USER_ENTERED")
    st.cache_data.clear()

def _bulk_write_txns(rows):
    ss = get_ss()
    ws = ss.worksheet("Transactions")
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    st.cache_data.clear()

def _update_txn(row_id, upd):
    ss = get_ss()
    ws = ss.worksheet("Transactions")
    all_vals = ws.get_all_values()
    hdrs = all_vals[0]
    for i, row in enumerate(all_vals[1:], start=2):
        if row[0] == row_id:
            new_row = [upd.get(h, row[j]) for j, h in enumerate(hdrs)]
            ws.update(f"A{i}:{chr(64+len(hdrs))}{i}", [new_row])
            break
    time.sleep(1)
    st.cache_data.clear()

def _delete_txn(row_id):
    ss = get_ss()
    ws = ss.worksheet("Transactions")
    all_vals = ws.get_all_values()
    for i, row in enumerate(all_vals[1:], start=2):
        if row[0] == row_id:
            ws.delete_rows(i)
            break
    st.cache_data.clear()

def _write_email_rule(rule_dict):
    ss = get_ss()
    ws = ss.worksheet("EmailRules")
    row = [rule_dict.get(h, "") for h in HEADERS["EmailRules"]]
    ws.append_row(row)
    st.cache_data.clear()

def _delete_email_rule(rule_name):
    ss = get_ss()
    ws = ss.worksheet("EmailRules")
    all_vals = ws.get_all_values()
    for i, row in enumerate(all_vals[1:], start=2):
        if row and row[0] == rule_name:
            ws.delete_rows(i)
            break
    st.cache_data.clear()

def _update_email_rule(rule_name, upd):
    ss = get_ss()
    ws = ss.worksheet("EmailRules")
    all_vals = ws.get_all_values()
    hdrs = all_vals[0]
    for i, row in enumerate(all_vals[1:], start=2):
        if row and row[0] == rule_name:
            new_row = list(row)
            for j, h in enumerate(hdrs):
                if h in upd:
                    if j < len(new_row):
                        new_row[j] = upd[h]
                    else:
                        new_row.append(upd[h])
            ws.update(f"A{i}:{chr(64+len(hdrs))}{i}", [new_row])
            break
    st.cache_data.clear()


# ═══════════════════════════════════════════════════════════════════════════════
#  SMART CATEGORISATION
# ═══════════════════════════════════════════════════════════════════════════════

def auto_cat(merchant: str, cats_df: pd.DataFrame):
    m = merchant.lower().strip()
    for _, row in cats_df.iterrows():
        kws = str(row.get("Keywords", "")).lower()
        if not kws:
            continue
        for kw in kws.split(","):
            kw = kw.strip()
            if kw and kw in m:
                return row["Category"], row["Subcategory"], "high"
    return "Others", "Miscellaneous", "low"


# ═══════════════════════════════════════════════════════════════════════════════
#  MULTI-ACCOUNT HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def extract_accounts(df: pd.DataFrame) -> list:
    """Return sorted unique non-empty Tags values as account list."""
    if df.empty or "Tags" not in df.columns:
        return []
    tags = df["Tags"].dropna().astype(str)
    tags = tags[tags.str.strip().str.len() > 0]
    return sorted(tags.unique().tolist())

def account_badge_html(account: str, inline: bool = False) -> str:
    """Return a styled HTML badge for the given account label."""
    acc = str(account).upper()
    if any(x in acc for x in ["CC","CREDIT","CARD"]):
        color, bg = C["warning"], "rgba(240,165,0,0.18)"
    elif any(x in acc for x in ["UPI","PAYTM","GPAY","PHONEPE"]):
        color, bg = C["info"], "rgba(88,166,255,0.18)"
    elif any(x in acc for x in ["DEBIT","SB","SAVINGS"]):
        color, bg = C["income"], "rgba(0,200,150,0.18)"
    elif any(x in acc for x in ["WALLET","CASH"]):
        color, bg = C["success"], "rgba(63,185,80,0.18)"
    else:
        color, bg = C["muted"], C["surface2"]
    style = (
        f"background:{bg};color:{color};"
        f"font-size:.58rem;font-weight:800;letter-spacing:.4px;"
        f"padding:2px 7px;border-radius:20px;text-transform:uppercase;"
        f"white-space:nowrap;{'display:inline-block;' if inline else ''}"
    )
    return f'<span style="{style}">{account}</span>'

def filter_by_account(df: pd.DataFrame, acct: str) -> pd.DataFrame:
    """Filter df by Tags == acct. 'All' returns full df."""
    if acct == "All" or df.empty:
        return df
    return df[df["Tags"].astype(str) == acct]


# ═══════════════════════════════════════════════════════════════════════════════
#  EMAIL TEMPLATE PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def parse_email_body(template: str, body: str) -> dict | None:
    """
    Parse email body using a template with {tag} placeholders.
    Supported tags: {amt} {act} {tdetails} {date} {skip}
    Returns dict with matching values or None if no match.

    Example template:
      "Rs.{amt} is debited from your {act} towards {tdetails} on {date}"
    """
    TAGS = ["amt", "act", "tdetails", "date", "skip"]

    # Escape literal text but mark our placeholders first
    MARKER = "\x00"
    safe_tmpl = template
    for tag in TAGS:
        safe_tmpl = safe_tmpl.replace(f"{{{tag}}}", f"{MARKER}{tag}{MARKER}")

    # Re-escape only the non-marker parts
    parts = safe_tmpl.split(MARKER)
    # parts alternates: [literal, tag, literal, tag, ...]
    regex_parts = []
    named_groups: list[str] = []
    skip_idx = 0

    for i, part in enumerate(parts):
        if i % 2 == 0:
            # literal segment
            regex_parts.append(re.escape(part))
        else:
            tag = part
            if tag == "skip":
                skip_idx += 1
                regex_parts.append(f"(?:.*?)")
            elif tag in named_groups:
                regex_parts.append(f"(?:.*?)")
            else:
                named_groups.append(tag)
                regex_parts.append(f"(?P<{tag}>.*?)")

    if not named_groups:
        return None

    final_regex = "".join(regex_parts)

    # Make the very last lazy quantifier greedy so it captures trailing text
    last_lazy = final_regex.rfind(".*?")
    if last_lazy >= 0:
        final_regex = final_regex[:last_lazy] + ".*" + final_regex[last_lazy + 3:]

    try:
        m = re.search(final_regex, body, re.DOTALL | re.IGNORECASE)
        if not m:
            return None
        result = {}
        for tag in named_groups:
            try:
                val = m.group(tag)
                if val is not None:
                    result[tag] = val.strip()
            except Exception:
                pass
        return result if result else None
    except Exception:
        return None

def clean_amount(raw: str) -> float | None:
    """Strip currency symbols, commas, spaces and convert to float."""
    cleaned = re.sub(r"[₹$,\s]", "", str(raw))
    try:
        return float(cleaned)
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  MISC HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def month_range(year, month):
    s = date(year, month, 1)
    e = date(year, month, calendar.monthrange(year, month)[1])
    return s, e

def cat_icon(cat):
    mapping = {row[0]: row[3] for row in DEFAULT_CATEGORIES}
    return mapping.get(cat, "📌")

def fmt(n, sym="₹"):
    return f"{sym}{abs(n):,.0f}"


# ═══════════════════════════════════════════════════════════════════════════════
#  CSS — MOBILE FIRST
# ═══════════════════════════════════════════════════════════════════════════════

def inject_css():
    st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

*, *::before, *::after {{ box-sizing: border-box; margin:0; padding:0; }}

html, body,
[data-testid="stAppViewContainer"],
[data-testid="stApp"] {{
    background: {C["bg"]} !important;
    color: {C["text"]};
    font-family: 'Nunito', sans-serif;
}}

[data-testid="stAppViewContainer"] > .main {{
    max-width: 480px;
    margin: 0 auto;
    padding: 0 0 16px 0 !important;
}}

.block-container {{
    padding: 0 12px 16px !important;
    max-width: 480px !important;
}}

[data-testid="stHeader"],
[data-testid="stToolbar"],
[data-testid="collapsedControl"],
[data-testid="stSidebar"],
footer, #MainMenu {{ display:none !important; }}

/* ── BOTTOM NAV ── */
.bottom-nav {{
    position: fixed; bottom: 0; left: 50%;
    transform: translateX(-50%);
    width: 100%; max-width: 480px;
    background: {C["surface"]};
    border-top: 1px solid {C["border"]};
    display: flex; align-items: center; justify-content: space-around;
    padding: 8px 0 18px;
    z-index: 9999;
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
}}

/* ── CARDS ── */
.card {{
    background: {C["surface"]};
    border: 1px solid {C["border"]};
    border-radius: 16px;
    padding: 16px;
    margin: 8px 0;
}}
.card-sm {{
    background: {C["surface"]};
    border: 1px solid {C["border"]};
    border-radius: 12px;
    padding: 12px 14px;
    margin: 4px 0;
}}
.acct-row {{
    display: flex; align-items: center; justify-content: space-between;
    padding: 7px 4px;
    border-top: 1px solid {C["border"]};
    font-size: .8rem;
}}

/* ── TYPOGRAPHY ── */
.page-title {{
    font-size: 1.5rem; font-weight: 900;
    color: {C["text"]}; padding: 16px 4px 4px;
}}
.section-label {{
    font-size: 0.65rem; font-weight: 800;
    letter-spacing: 1.5px; text-transform: uppercase;
    color: {C["muted"]}; margin: 16px 0 8px 2px;
}}
.hero-num {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 2.6rem; font-weight: 700;
    color: {C["primary"]}; line-height: 1;
}}
.mono {{ font-family: 'JetBrains Mono', monospace; font-weight: 600; }}

/* ── PROGRESS ── */
.bar-wrap {{
    background: {C["surface2"]};
    border-radius: 100px; height: 7px; overflow: hidden; margin: 5px 0;
}}
.bar-fill {{
    height: 100%; border-radius: 100px; transition: width .4s ease;
}}

/* ── BADGES ── */
.badge-auto {{
    background: rgba(0,200,150,.15); color: {C["income"]};
    font-size: .6rem; font-weight: 800; letter-spacing: .5px;
    padding: 2px 7px; border-radius: 20px; text-transform: uppercase;
}}
.badge-review {{
    background: rgba(240,165,0,.15); color: {C["warning"]};
    font-size: .6rem; font-weight: 800; letter-spacing: .5px;
    padding: 2px 7px; border-radius: 20px; text-transform: uppercase;
}}
.badge-cat {{
    background: rgba(124,109,248,.12); color: {C["primary"]};
    font-size: .7rem; font-weight: 600;
    padding: 2px 8px; border-radius: 20px;
}}

/* ── TRANSACTION ROW ── */
.txn-row {{
    display: flex; align-items: center; gap: 11px;
    padding: 11px 12px;
    background: {C["surface"]};
    border: 1px solid {C["border"]};
    border-radius: 12px; margin: 3px 0;
}}
.txn-icon {{
    width: 40px; height: 40px; border-radius: 10px;
    background: {C["surface2"]};
    display: flex; align-items: center; justify-content: center;
    font-size: 1.15rem; flex-shrink: 0;
}}

/* ── NAV BUTTONS ── */
[data-testid="stButton"] > button {{
    background: transparent !important;
    border: none !important;
    color: {C["muted"]} !important;
    font-family: 'Nunito', sans-serif !important;
    font-size: .68rem !important; font-weight: 700 !important;
    padding: 4px 6px !important;
    border-radius: 10px !important;
    width: 100% !important;
    line-height: 1.4 !important;
    white-space: nowrap !important;
    box-shadow: none !important;
    transition: color .2s, background .2s !important;
}}
[data-testid="stButton"] > button:hover {{
    color: {C["primary"]} !important;
    background: {C["primary_dim"]} !important;
}}

.nav-on [data-testid="stButton"] > button {{
    color: {C["primary"]} !important;
    background: {C["primary_dim"]} !important;
}}

.home-cat-btn [data-testid="stButton"] > button {{
    background: {C["surface"]} !important;
    border: 1px solid {C["border"]} !important;
    border-radius: 10px !important;
    color: {C["text"]} !important;
    font-size: .82rem !important; font-weight: 700 !important;
    text-align: left !important; justify-content: flex-start !important;
    padding: 8px 12px !important; margin-bottom: 0 !important;
}}
.home-cat-btn [data-testid="stButton"] > button:hover {{
    border-color: {C["primary"]} !important;
    background: rgba(124,109,248,0.08) !important;
}}

/* FAB */
.fab [data-testid="stButton"] > button {{
    background: {C["primary"]} !important;
    color: white !important;
    font-size: 1.5rem !important;
    width: 52px !important; height: 52px !important;
    border-radius: 50% !important;
    padding: 0 !important;
    box-shadow: 0 4px 20px rgba(124,109,248,.55) !important;
    min-height: unset !important;
}}

/* Pill filter buttons */
.pill-on [data-testid="stButton"] > button {{
    background: rgba(124,109,248,.18) !important;
    color: {C["primary"]} !important;
    border: 1px solid {C["primary"]} !important;
    border-radius: 20px !important;
    font-size: .72rem !important; padding: 3px 10px !important;
}}
.pill-off [data-testid="stButton"] > button {{
    background: {C["surface2"]} !important;
    color: {C["muted"]} !important;
    border: 1px solid {C["border"]} !important;
    border-radius: 20px !important;
    font-size: .72rem !important; padding: 3px 10px !important;
}}

/* Primary action buttons */
[data-testid="stFormSubmitButton"] > button,
[data-testid="stButton"] > button[kind="primary"] {{
    background: {C["primary"]} !important;
    color: white !important;
    border-radius: 12px !important;
    font-size: .9rem !important; font-weight: 800 !important;
    padding: 10px 16px !important;
    box-shadow: 0 3px 12px rgba(124,109,248,.4) !important;
}}

/* Top nav dropdown */
[data-testid="stSelectbox"][aria-label="top_nav_dd"] > div > div,
div[data-key="top_nav_dd"] > div > div > div {{
    background: rgba(124,109,248,0.12) !important;
    border: 1px solid #7c6df8 !important;
    border-radius: 10px !important;
    font-weight: 800 !important;
    font-size: .82rem !important;
}}

/* Inputs */
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stDateInput"] input,
[data-testid="stTextArea"] textarea {{
    background: {C["surface2"]} !important;
    border: 1px solid {C["border"]} !important;
    border-radius: 10px !important;
    color: {C["text"]} !important;
    font-family: 'Nunito', sans-serif !important;
    font-size: .9rem !important;
}}
[data-testid="stSelectbox"] > div > div {{
    background: {C["surface2"]} !important;
    border: 1px solid {C["border"]} !important;
    border-radius: 10px !important;
    color: {C["text"]} !important;
}}

/* Radio */
[data-testid="stRadio"] label {{
    color: {C["text"]} !important;
    font-weight: 600 !important;
    font-size: .85rem !important;
}}

/* Expander */
[data-testid="stExpander"] {{
    background: {C["surface"]} !important;
    border: 1px solid {C["border"]} !important;
    border-radius: 12px !important;
}}
[data-testid="stExpander"] summary {{
    color: {C["text"]} !important; font-weight: 700 !important;
}}

/* Divider */
hr {{ border-color: {C["border"]} !important; margin: 14px 0 !important; }}

/* Dialog */
[data-testid="stDialog"] > div {{
    background: {C["surface"]} !important;
    border: 1px solid {C["border"]} !important;
    border-radius: 20px !important;
}}

/* Scrollbar */
::-webkit-scrollbar {{ width: 3px; }}
::-webkit-scrollbar-thumb {{ background: {C["border"]}; border-radius: 2px; }}

/* Alerts */
[data-testid="stAlert"] {{ border-radius: 12px !important; border: none !important; }}

/* Download button */
[data-testid="stDownloadButton"] > button {{
    background: {C["surface2"]} !important;
    border: 1px solid {C["border"]} !important;
    color: {C["text"]} !important;
    border-radius: 10px !important;
}}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  SESSION STATE
# ═══════════════════════════════════════════════════════════════════════════════

def init_state():
    defaults = {
        "nav":              "home",
        "edit_txn":         None,
        "filter_cat":       "All",
        "acct_filter":      "All",
        "f_month":          0,
        "f_year":           0,
        "search":           "",
        "search_all":       False,
        "preview_rows":     None,
        "setup_ok":         False,
        "cat_view":         "Category",
        "show_acct_breakdown": False,
        "ana_acct_filter":  "All",
        "email_parse_result": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ═══════════════════════════════════════════════════════════════════════════════
#  TOP BAR + NAV
# ═══════════════════════════════════════════════════════════════════════════════

def render_top_bar():
    NAV_LABELS = {
        "home":         "🏠 Home",
        "transactions": "📋 Spends",
        "add":          "➕ Add",
        "analytics":    "📊 Insights",
        "settings":     "⚙️ Settings",
    }
    c1, c2, c3 = st.columns([4, 1, 1])
    with c1:
        current_label = NAV_LABELS.get(st.session_state.nav, "🏠 Home")
        choice = st.selectbox("", list(NAV_LABELS.values()),
                              index=list(NAV_LABELS.values()).index(current_label),
                              key="top_nav_dd", label_visibility="collapsed")
        chosen_key = [k for k, v in NAV_LABELS.items() if v == choice][0]
        if chosen_key != st.session_state.nav:
            st.session_state.nav = chosen_key; st.rerun()
    with c2:
        if st.button("🔄", key="top_reload", help="Refresh data"):
            st.cache_data.clear(); st.rerun()
    with c3:
        if st.button("⚡", key="top_sync", help="Pull latest from Sheets"):
            st.cache_data.clear()
            log = load_importlog()
            if not log.empty and "Imported" in log.columns:
                last = log.iloc[-1]
                st.toast(f"Last import: {last.get('Imported','?')} · {last.get('Skipped','?')}", icon="✅")
            else:
                st.toast("Cache cleared — data reloaded", icon="✅")
            st.rerun()

NAV_ITEMS = [
    ("home",         "🏠", "Home"),
    ("transactions", "📋", "Spends"),
    ("add",          "➕", "Add"),
    ("analytics",    "📊", "Insights"),
    ("settings",     "⚙️",  "Settings"),
]

def render_nav():
    st.markdown('<div class="bottom-nav">', unsafe_allow_html=True)
    cols = st.columns(5)
    for i, (key, icon, label) in enumerate(NAV_ITEMS):
        with cols[i]:
            active = st.session_state.nav == key
            if key == "add":
                st.markdown('<div class="fab">', unsafe_allow_html=True)
                if st.button("➕", key="nav_add"):
                    st.session_state.nav = "add"; st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                wrap = "nav-on" if active else ""
                st.markdown(f'<div class="{wrap}">', unsafe_allow_html=True)
                if st.button(f"{icon}\n{label}", key=f"nav_{key}"):
                    st.session_state.nav = key; st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  DIALOGS
# ═══════════════════════════════════════════════════════════════════════════════

@st.dialog("✏️ Edit Transaction", width="small")
def dlg_edit(txn):
    cats_df = load_categories()
    df_all  = load_transactions()
    cats = cats_df["Category"].unique().tolist()
    cat_idx = cats.index(txn["Category"]) if txn["Category"] in cats else 0

    ttype = st.radio("", ["💸 Expense","💰 Income"], horizontal=True,
                     index=0 if txn.get("Type","Expense") == "Expense" else 1,
                     key="dlg_type")
    amount = st.number_input("Amount (₹)", value=abs(float(txn["Amount"])),
                              min_value=0.0, step=1.0, format="%.0f", key="dlg_amt")
    merch  = st.text_input("Merchant", value=txn["Merchant"], key="dlg_merch")

    dlg_cat_opts = cats + ["➕ New category…"]
    sel_cat_r = st.selectbox("Category", dlg_cat_opts, index=cat_idx, key="dlg_cat")
    if sel_cat_r == "➕ New category…":
        nc = st.text_input("New category", key="dlg_nc")
        ns = st.text_input("First subcategory", key="dlg_ns")
        if st.button("✅ Create", key="dlg_create_cat"):
            if nc.strip() and ns.strip():
                get_ss().worksheet("Categories").append_row([nc.strip(), ns.strip(),"","📌"])
                st.cache_data.clear(); st.rerun()
        sel_cat = cats[0] if cats else "Others"
    else:
        sel_cat = sel_cat_r

    subs = cats_df[cats_df["Category"]==sel_cat]["Subcategory"].tolist()
    sub_idx = subs.index(txn.get("Subcategory","")) if txn.get("Subcategory","") in subs else 0
    dlg_sub_opts = subs + ["➕ New subcategory…"] if subs else ["➕ New subcategory…"]
    sel_sub_r = st.selectbox("Subcategory", dlg_sub_opts, index=sub_idx, key="dlg_sub")
    if sel_sub_r == "➕ New subcategory…":
        ns2 = st.text_input("New subcategory name", key="dlg_ns2")
        if st.button("✅ Add Sub", key="dlg_create_sub"):
            if ns2.strip():
                get_ss().worksheet("Categories").append_row([sel_cat, ns2.strip(),"","📌"])
                st.cache_data.clear(); st.rerun()
        sel_sub = subs[0] if subs else ""
    else:
        sel_sub = sel_sub_r

    pm_idx = PAYMENT_METHODS.index(txn.get("PaymentMethod","UPI")) if txn.get("PaymentMethod","UPI") in PAYMENT_METHODS else 0
    pm = st.selectbox("Payment Method", PAYMENT_METHODS, index=pm_idx, key="dlg_pm")

    # ── Account (Tags) field
    existing_accounts = extract_accounts(df_all)
    cur_tag = str(txn.get("Tags",""))
    acct_opts = existing_accounts + (["✏️ New account…"] if cur_tag not in existing_accounts else [])
    acct_def  = existing_accounts.index(cur_tag) if cur_tag in existing_accounts else len(acct_opts)-1
    dlg_acct_raw = st.selectbox("Account", acct_opts, index=acct_def, key="dlg_acct")
    if dlg_acct_raw == "✏️ New account…":
        dlg_acct = st.text_input("Account label", value=cur_tag, key="dlg_acct_new",
                                  placeholder="e.g. HDFC CC 7500")
    else:
        dlg_acct = dlg_acct_raw

    txn_dt = st.date_input("Date",
                            value=txn["Date"].date() if hasattr(txn["Date"],"date") else date.today(),
                            key="dlg_date")
    notes  = st.text_input("Notes", value=txn.get("Notes",""), key="dlg_notes")

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("💾 Update", use_container_width=True, type="primary"):
            if amount > 0 and merch.strip():
                upd = {
                    "RowID": txn["RowID"], "Date": txn_dt.strftime("%Y-%m-%d"),
                    "Merchant": merch.strip(),
                    "Type": "Expense" if "Expense" in ttype else "Income",
                    "Amount": -abs(amount) if "Expense" in ttype else abs(amount),
                    "Category": sel_cat, "Subcategory": sel_sub,
                    "PaymentMethod": pm, "Tags": dlg_acct,
                    "Notes": notes, "Source": txn.get("Source","manual"), "AutoCat": "no",
                }
                _update_txn(txn["RowID"], upd)
                st.session_state.edit_txn = None
                st.success("✅ Updated!")
                st.rerun()
    with c2:
        if st.button("🗑️ Delete", use_container_width=True):
            _delete_txn(txn["RowID"])
            st.session_state.edit_txn = None
            st.rerun()
    with c3:
        if st.button("✕ Close", use_container_width=True):
            st.session_state.edit_txn = None
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN — HOME
# ═══════════════════════════════════════════════════════════════════════════════

def screen_home():
    df       = load_transactions()
    settings = load_settings()
    sym      = settings.get("currency_symbol","₹")
    budget   = float(settings.get("monthly_budget", 30000))
    now      = datetime.today()

    ms, me = month_range(now.year, now.month)
    mdf = df[(df["Date"].dt.date >= ms) & (df["Date"].dt.date <= me)] if not df.empty else df.copy()

    spent  = abs(mdf[mdf["Amount"] < 0]["Amount"].sum())
    income = mdf[mdf["Amount"] > 0]["Amount"].sum()

    lm  = now.month - 1 or 12
    ly  = now.year if now.month > 1 else now.year - 1
    lms, lme = month_range(ly, lm)
    lm_spent = abs(df[(df["Date"].dt.date >= lms) & (df["Date"].dt.date <= lme) & (df["Amount"] < 0)]["Amount"].sum()) if not df.empty else 0

    pct    = ((spent - lm_spent) / lm_spent * 100) if lm_spent > 0 else 0
    b_used = min(spent / budget * 100, 100) if budget > 0 else 0
    bar_c  = C["expense"] if b_used > 90 else C["warning"] if b_used > 70 else C["income"]
    d_c    = C["expense"] if pct > 0 else C["income"]
    d_arr  = "▲" if pct > 0 else "▼"

    st.markdown(f"""
    <div style="padding:14px 4px 4px">
        <div style="color:{C['muted']};font-size:.8rem;font-weight:600">{now.strftime('%B %Y')}</div>
        <div style="font-size:1.5rem;font-weight:900;color:{C['text']}">Overview 👋</div>
    </div>""", unsafe_allow_html=True)

    # ── HERO CARD (tap to expand account breakdown)
    st.markdown(f"""
    <div class="card" style="background:linear-gradient(135deg,{C['surface']},{C['surface2']})">
        <div style="color:{C['muted']};font-size:.65rem;font-weight:800;letter-spacing:1.2px;text-transform:uppercase;margin-bottom:6px">Total Spent This Month</div>
        <div class="hero-num">{sym}{spent:,.0f}</div>
        <div style="margin-top:6px;font-size:.78rem;color:{d_c};font-weight:700">{d_arr} {abs(pct):.1f}% vs last month</div>
        <div style="margin-top:12px">
            <div style="display:flex;justify-content:space-between;font-size:.72rem;color:{C['muted']};margin-bottom:3px">
                <span>Monthly Budget</span>
                <span style="color:{C['text']};font-family:'JetBrains Mono',monospace">{b_used:.0f}% of {sym}{budget:,.0f}</span>
            </div>
            <div class="bar-wrap"><div class="bar-fill" style="width:{b_used:.0f}%;background:{bar_c}"></div></div>
        </div>
    </div>""", unsafe_allow_html=True)

    # ── PER-ACCOUNT BREAKDOWN (expandable)
    accounts = extract_accounts(mdf) if not mdf.empty else []
    if accounts:
        btn_label = "▲ Hide account breakdown" if st.session_state.show_acct_breakdown else "▼ By account"
        st.markdown('<div class="pill-off">', unsafe_allow_html=True)
        if st.button(btn_label, key="toggle_acct_breakdown", use_container_width=True):
            st.session_state.show_acct_breakdown = not st.session_state.show_acct_breakdown
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

        if st.session_state.show_acct_breakdown:
            exp_mdf = mdf[mdf["Amount"] < 0]
            acct_totals = {}
            for acct in accounts:
                acct_totals[acct] = abs(exp_mdf[exp_mdf["Tags"].astype(str) == acct]["Amount"].sum())
            # Also show untagged if any
            untagged = abs(exp_mdf[exp_mdf["Tags"].astype(str).str.strip() == ""]["Amount"].sum())

            st.markdown(f"""
            <div class="card" style="padding:10px 14px;margin-top:4px">
                <div style="font-size:.6rem;font-weight:800;letter-spacing:1px;text-transform:uppercase;color:{C['muted']};margin-bottom:8px">Account Breakdown — {now.strftime('%B')}</div>""",
                unsafe_allow_html=True)
            for acct, amt in sorted(acct_totals.items(), key=lambda x: -x[1]):
                pct_of_total = (amt / spent * 100) if spent > 0 else 0
                st.markdown(f"""
                <div class="acct-row">
                    <span>{account_badge_html(acct, inline=True)}</span>
                    <span>
                        <span style="font-family:'JetBrains Mono',monospace;color:{C['expense']};font-weight:600">{sym}{amt:,.0f}</span>
                        <span style="color:{C['muted']};font-size:.65rem;margin-left:5px">{pct_of_total:.0f}%</span>
                    </span>
                </div>""", unsafe_allow_html=True)
            if untagged > 0:
                st.markdown(f"""
                <div class="acct-row">
                    <span style="color:{C['muted']};font-size:.75rem">Untagged</span>
                    <span style="font-family:'JetBrains Mono',monospace;color:{C['muted']};font-weight:600">{sym}{untagged:,.0f}</span>
                </div>""", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

    # ── INCOME / SAVINGS
    savings = income - spent
    s_rate  = (savings / income * 100) if income > 0 else 0
    s_color = C["income"] if savings >= 0 else C["expense"]

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"""<div class="card-sm">
            <div style="font-size:.62rem;color:{C['muted']};font-weight:800;letter-spacing:.8px;text-transform:uppercase">Income</div>
            <div class="mono" style="font-size:1.1rem;color:{C['income']}">{sym}{income:,.0f}</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="card-sm">
            <div style="font-size:.62rem;color:{C['muted']};font-weight:800;letter-spacing:.8px;text-transform:uppercase">Savings Rate</div>
            <div class="mono" style="font-size:1.1rem;color:{s_color}">{s_rate:.1f}%</div>
        </div>""", unsafe_allow_html=True)

    # ── TOP CATEGORIES
    if not mdf.empty:
        st.markdown(f'<div class="section-label">Top Categories <span style="font-size:.65rem;color:{C["muted"]}">tap to explore</span></div>', unsafe_allow_html=True)
        exp_df = mdf[mdf["Amount"] < 0]
        if not exp_df.empty:
            top = exp_df.groupby("Category")["Amount"].sum().abs().sort_values(ascending=False).head(5)
            mx  = top.max()
            for cat, amt in top.items():
                ico = cat_icon(cat)
                w   = (amt / mx * 100) if mx > 0 else 0
                st.markdown('<div class="home-cat-btn">', unsafe_allow_html=True)
                if st.button(f"{ico}  {cat}   {sym}{amt:,.0f}", key=f"home_cat_{cat}", use_container_width=True):
                    st.session_state.nav        = "transactions"
                    st.session_state.filter_cat  = cat
                    st.session_state.f_month     = now.month
                    st.session_state.f_year      = now.year
                    st.session_state.search      = ""
                    st.session_state.acct_filter = "All"
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)
                st.markdown(f"""<div class="bar-wrap" style="margin:-2px 0 6px">
                    <div class="bar-fill" style="width:{w:.0f}%;background:{C['primary']}"></div>
                </div>""", unsafe_allow_html=True)

    # ── RECENT TRANSACTIONS
    st.markdown('<div class="section-label">Recent</div>', unsafe_allow_html=True)
    if df.empty:
        st.markdown(f"""<div class="card" style="text-align:center;padding:36px">
            <div style="font-size:2.5rem">💳</div>
            <div style="font-weight:800;margin:8px 0">No transactions yet</div>
            <div style="color:{C['muted']};font-size:.85rem">Tap ➕ to add your first expense</div>
        </div>""", unsafe_allow_html=True)
    else:
        recent = df.sort_values("Date", ascending=False).head(6)
        for _, row in recent.iterrows():
            amt  = row["Amount"]
            ac   = C["income"] if amt > 0 else C["expense"]
            sg   = "+" if amt > 0 else "−"
            ico  = cat_icon(row["Category"])
            ds   = row["Date"].strftime("%d %b") if pd.notna(row["Date"]) else ""
            tag  = str(row.get("Tags","")).strip()
            acct_html = f"&nbsp;{account_badge_html(tag, inline=True)}" if tag else ""
            st.markdown(f"""
            <div class="txn-row">
                <div class="txn-icon">{ico}</div>
                <div style="flex:1;min-width:0;overflow:hidden">
                    <div style="font-weight:700;font-size:.88rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{row['Merchant']}</div>
                    <div style="font-size:.72rem;color:{C['muted']}">{row['Category']} · {ds}{acct_html}</div>
                </div>
                <div class="mono" style="color:{ac};font-size:.9rem;flex-shrink:0">{sg}{sym}{abs(amt):,.0f}</div>
            </div>""", unsafe_allow_html=True)

        if st.button("View All Transactions →", use_container_width=True):
            st.session_state.nav = "transactions"; st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN — TRANSACTIONS (SPENDS)
# ═══════════════════════════════════════════════════════════════════════════════

def screen_transactions():
    df       = load_transactions()
    settings = load_settings()
    sym      = settings.get("currency_symbol","₹")

    c_t, c_r = st.columns([5,1])
    with c_t:
        st.markdown('<div class="page-title">Spends 📋</div>', unsafe_allow_html=True)
    with c_r:
        if st.button("🔄", key="txn_reload", help="Reload data"):
            st.cache_data.clear(); st.rerun()

    q = st.text_input("", placeholder="🔍  Search all transactions...", key="txn_q",
                      label_visibility="collapsed")

    MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    if (st.session_state.f_month == 0 or st.session_state.f_year == 0) and not df.empty:
        latest = df.dropna(subset=["Date"]).sort_values("Date",ascending=False).iloc[0]["Date"]
        st.session_state.f_month = int(latest.month)
        st.session_state.f_year  = int(latest.year)
    elif st.session_state.f_month == 0:
        st.session_state.f_month = datetime.today().month
        st.session_state.f_year  = datetime.today().year

    years = sorted(df["Date"].dropna().dt.year.unique().astype(int).tolist()) if not df.empty else [datetime.today().year]
    if st.session_state.f_year not in years: st.session_state.f_year = years[-1]

    if not q:
        c1t, c2t = st.columns(2)
        with c1t:
            sel_m = st.selectbox("", MONTHS, index=st.session_state.f_month-1,
                                 key="t_month", label_visibility="collapsed")
            st.session_state.f_month = MONTHS.index(sel_m) + 1
        with c2t:
            sel_y = st.selectbox("", years, index=years.index(st.session_state.f_year),
                                 key="t_year", label_visibility="collapsed")
            st.session_state.f_year = int(sel_y)

    # ── ACCOUNT FILTER DROPDOWN
    all_accounts = extract_accounts(df)
    if all_accounts:
        acct_opts = ["All"] + all_accounts
        cur_acct_idx = acct_opts.index(st.session_state.acct_filter) if st.session_state.acct_filter in acct_opts else 0
        sel_acct = st.selectbox("", acct_opts, index=cur_acct_idx,
                                key="txn_acct_dd", label_visibility="collapsed",
                                format_func=lambda x: f"💳 {x}" if x != "All" else "🏦 All Accounts")
        if sel_acct != st.session_state.acct_filter:
            st.session_state.acct_filter = sel_acct; st.rerun()

    # ── FILTER
    filtered = df.copy()
    if not filtered.empty:
        if q:
            filtered = filtered[filtered["Merchant"].str.contains(q, case=False, na=False)]
        else:
            ms, me = month_range(st.session_state.f_year, st.session_state.f_month)
            filtered = filtered[(filtered["Date"].dt.date >= ms) & (filtered["Date"].dt.date <= me)]

        # Account filter
        filtered = filter_by_account(filtered, st.session_state.acct_filter)

    # ── SUMMARY STRIP
    if not filtered.empty:
        tot_exp = abs(filtered[filtered["Amount"]<0]["Amount"].sum())
        tot_inc = filtered[filtered["Amount"]>0]["Amount"].sum()
        label   = f"Search: {q}" if q else f"{MONTHS[st.session_state.f_month-1]} {st.session_state.f_year}"
        acct_suffix = f" · {st.session_state.acct_filter}" if st.session_state.acct_filter != "All" else ""
        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;
             padding:7px 10px;background:{C['surface2']};border-radius:10px;margin:6px 0;
             font-size:.75rem;font-weight:700">
            <span style="color:{C['muted']}">{label}{acct_suffix}</span>
            <span>
                <span style="color:{C['expense']};font-family:'JetBrains Mono',monospace">−{sym}{tot_exp:,.0f}</span>
                &nbsp;·&nbsp;
                <span style="color:{C['income']};font-family:'JetBrains Mono',monospace">+{sym}{tot_inc:,.0f}</span>
            </span>
        </div>""", unsafe_allow_html=True)

    # ── CATEGORY PILLS
    if not filtered.empty:
        cats = ["All"] + sorted(filtered["Category"].dropna().unique().tolist())
        if st.session_state.filter_cat not in cats: st.session_state.filter_cat = "All"
        CAT_SHORT = {
            "Food & Dining":"Food","Bills & Utilities":"Bills",
            "Transport":"Transit","Health":"Health","Shopping":"Shop",
            "Entertainment":"Fun","Travel":"Travel","Personal Care":"Care",
            "Investments":"Invest","Gifts & Social":"Gifts",
            "Rent & Housing":"Rent","Others":"Other",
        }
        ncols = min(len(cats), 5)
        pill_cols = st.columns(ncols)
        for i, cat in enumerate(cats[:ncols]):
            with pill_cols[i]:
                lbl = CAT_SHORT.get(cat, cat[:6]) if cat != "All" else "All"
                on  = cat == st.session_state.filter_cat
                st.markdown(f'<div class="{"pill-on" if on else "pill-off"}">', unsafe_allow_html=True)
                if st.button(lbl, key=f"pill_{cat}"):
                    st.session_state.filter_cat = cat; st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)
        if st.session_state.filter_cat != "All":
            filtered = filtered[filtered["Category"]==st.session_state.filter_cat]

    # ── EMPTY STATE
    if filtered.empty:
        st.markdown(f"""<div class="card" style="text-align:center;padding:28px;margin-top:8px">
            <div style="font-size:1.8rem">🔍</div>
            <div style="font-weight:800;margin:6px 0">No transactions</div>
            <div style="color:{C['muted']};font-size:.82rem">{"No results for "" + q + """ if q else "Try a different month or account"}</div>
        </div>""", unsafe_allow_html=True)
        return

    # ── TRANSACTION LIST grouped by day
    for day, grp in sorted(
        filtered.sort_values("Date", ascending=False).groupby(filtered["Date"].dt.date),
        reverse=True
    ):
        day_total = grp["Amount"].sum()
        dc        = C["income"] if day_total >= 0 else C["expense"]
        day_str   = pd.Timestamp(day).strftime("%a %d %b" + (" %Y" if day.year != datetime.today().year else ""))
        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;padding:5px 2px 2px;
             border-bottom:1px solid {C['border']}">
            <span style="font-size:.62rem;font-weight:800;color:{C['muted']};
                   letter-spacing:.6px;text-transform:uppercase">{day_str}</span>
            <span style="font-family:'JetBrains Mono',monospace;font-size:.7rem;
                   color:{dc}">{"+" if day_total>=0 else "−"}{sym}{abs(day_total):,.0f}</span>
        </div>""", unsafe_allow_html=True)

        for _, row in grp.iterrows():
            amt  = row["Amount"]
            ac   = C["income"] if amt>0 else C["expense"]
            sg   = "+" if amt>0 else "−"
            ico  = cat_icon(row["Category"])
            sub  = str(row.get("Subcategory",""))
            pm   = str(row.get("PaymentMethod",""))
            tag  = str(row.get("Tags","")).strip()
            auto_badge = ' <span class="badge-auto">A</span>' if str(row.get("AutoCat","")).lower()=="yes" else ""
            acct_badge = f" {account_badge_html(tag, inline=True)}" if tag else ""
            merch = str(row["Merchant"])[:32]

            c1, c2 = st.columns([5,1])
            with c1:
                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:8px;padding:5px 0;
                     border-bottom:1px solid {C['surface2']}">
                    <span style="font-size:.95rem;flex-shrink:0">{ico}</span>
                    <div style="flex:1;min-width:0">
                        <div style="font-weight:700;font-size:.8rem;white-space:nowrap;
                             overflow:hidden;text-overflow:ellipsis">{merch}</div>
                        <div style="font-size:.64rem;color:{C['muted']};margin-top:1px">
                            {sub}{(" · " + pm) if pm else ""}{acct_badge}{auto_badge}
                        </div>
                    </div>
                    <div style="font-family:'JetBrains Mono',monospace;color:{ac};
                         font-size:.82rem;flex-shrink:0;font-weight:600">
                         {sg}{sym}{abs(amt):,.0f}</div>
                </div>""", unsafe_allow_html=True)
            with c2:
                if st.button("✏️", key=f"e_{row['RowID']}", help="Edit"):
                    st.session_state.edit_txn = row.to_dict(); st.rerun()

    if st.session_state.edit_txn:
        dlg_edit(st.session_state.edit_txn)


# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN — ADD TRANSACTION
# ═══════════════════════════════════════════════════════════════════════════════

def screen_add():
    cats_df  = load_categories()
    df_all   = load_transactions()
    settings = load_settings()
    sym      = settings.get("currency_symbol","₹")
    cats     = cats_df["Category"].unique().tolist()

    st.markdown('<div class="page-title">Add Transaction ➕</div>', unsafe_allow_html=True)

    ttype  = st.radio("", ["💸 Expense","💰 Income"], horizontal=True, key="add_type")
    is_exp = "Expense" in ttype

    # ── CATEGORY (outside form for dynamic reaction)
    CAT_OPTIONS = cats + ["➕ New category…"]
    sel_cat_raw = st.selectbox("Category", CAT_OPTIONS, key="add_cat_sel")
    if sel_cat_raw == "➕ New category…":
        new_cat_name = st.text_input("New category name", key="add_cat_new", placeholder="e.g. Pet Care")
        new_sub_name = st.text_input("First subcategory name", key="add_sub_new", placeholder="e.g. Vet & Medicine")
        if st.button("✅ Create Category", key="create_cat"):
            if new_cat_name.strip() and new_sub_name.strip():
                get_ss().worksheet("Categories").append_row([new_cat_name.strip(), new_sub_name.strip(),"","📌"])
                st.cache_data.clear()
                st.success(f"✅ Created {new_cat_name} › {new_sub_name}")
                st.rerun()
            else:
                st.error("Enter both names.")
        sel_cat = cats[0] if cats else "Others"
    else:
        sel_cat = sel_cat_raw

    subs_list = cats_df[cats_df["Category"]==sel_cat]["Subcategory"].tolist()
    SUB_OPTIONS = subs_list + ["➕ New subcategory…"] if subs_list else ["➕ New subcategory…"]
    sel_sub_raw = st.selectbox("Subcategory", SUB_OPTIONS, key="add_sub_sel")
    if sel_sub_raw == "➕ New subcategory…":
        new_sub2 = st.text_input("New subcategory name", key="add_sub2_new", placeholder="e.g. Train & Bus")
        if st.button("✅ Add Subcategory", key="create_sub"):
            if new_sub2.strip():
                get_ss().worksheet("Categories").append_row([sel_cat, new_sub2.strip(),"","📌"])
                st.cache_data.clear()
                st.success(f"✅ Added {sel_cat} › {new_sub2}")
                st.rerun()
            else:
                st.error("Enter subcategory name.")
        sel_sub = subs_list[0] if subs_list else ""
    else:
        sel_sub = sel_sub_raw

    # ── ACCOUNT (Tags) — derived from existing data
    existing_accounts = extract_accounts(df_all)
    ACCT_OPTIONS = existing_accounts + ["✏️ New account…"]
    if existing_accounts:
        sel_acct_raw = st.selectbox("Account", ACCT_OPTIONS, key="add_acct_sel")
        if sel_acct_raw == "✏️ New account…":
            sel_acct = st.text_input("Account label", key="add_acct_new",
                                      placeholder="e.g. HDFC CC 7500, SBI CC 4996, Paytm UPI")
        else:
            sel_acct = sel_acct_raw
    else:
        sel_acct = st.text_input("Account (optional)", key="add_acct_new",
                                  placeholder="e.g. HDFC CC 7500, Paytm UPI")

    with st.form("add_form", clear_on_submit=True):
        amount   = st.number_input(f"Amount ({sym})", min_value=0.0, step=1.0, format="%.0f")
        merch    = st.text_input("Merchant / Description", placeholder="e.g. Swiggy, BESCOM, Salary...")

        c3, c4 = st.columns(2)
        with c3:
            pm = st.selectbox("Payment Method", PAYMENT_METHODS)
        with c4:
            txn_date = st.date_input("Date", value=date.today())

        notes = st.text_input("Notes (optional)", placeholder="Quick note…")

        if st.form_submit_button("💾  Save Transaction", use_container_width=True, type="primary"):
            if amount > 0 and merch.strip():
                _write_txn({
                    "RowID":         str(uuid.uuid4())[:8],
                    "Date":          txn_date.strftime("%Y-%m-%d"),
                    "Merchant":      merch.strip().title(),
                    "Amount":        -abs(amount) if is_exp else abs(amount),
                    "Type":          "Expense" if is_exp else "Income",
                    "Category":      sel_cat,
                    "Subcategory":   sel_sub,
                    "PaymentMethod": pm,
                    "Tags":          sel_acct,
                    "Notes":         notes,
                    "Source":        "manual",
                    "AutoCat":       "no",
                })
                st.success(f"✅ {'Expense' if is_exp else 'Income'} of {sym}{amount:,.0f} saved!")
            else:
                st.error("Please enter an amount and merchant name.")

    # ── IMPORT SECTION
    st.markdown("---")
    st.markdown('<div class="section-label">Import from File</div>', unsafe_allow_html=True)
    st.markdown(f"<div style='color:{C['muted']};font-size:.82rem;margin-bottom:10px'>Upload a bank statement (XLSX or CSV). Map the columns once, preview, then confirm.</div>", unsafe_allow_html=True)

    uploaded = st.file_uploader("", type=["xlsx","csv","xls"], label_visibility="collapsed")

    if uploaded:
        try:
            raw = pd.read_csv(uploaded) if uploaded.name.lower().endswith(".csv") else pd.read_excel(uploaded)
            st.success(f"✅  {len(raw)} rows detected in {uploaded.name}")
            st.dataframe(raw.head(3), use_container_width=True)

            all_cols = ["— skip —"] + raw.columns.tolist()
            c1, c2 = st.columns(2)
            with c1:
                date_col  = st.selectbox("📅 Date column",             all_cols, key="imp_date")
                merch_col = st.selectbox("🏪 Merchant/Description",    all_cols, key="imp_merch")
            with c2:
                amt_col   = st.selectbox("💰 Amount column",           all_cols, key="imp_amt")
                type_col  = st.selectbox("↕️ Dr/Cr column (optional)", all_cols, key="imp_type")

            # Import account tag
            imp_accounts = extract_accounts(df_all)
            imp_acct_opts = imp_accounts + ["✏️ New…"]
            if imp_accounts:
                imp_acct_raw = st.selectbox("💳 Tag as Account", imp_acct_opts, key="imp_acct")
                imp_acct = st.text_input("Account label", key="imp_acct_new", placeholder="e.g. HDFC CC 7500") if imp_acct_raw == "✏️ New…" else imp_acct_raw
            else:
                imp_acct = st.text_input("💳 Tag as Account (optional)", key="imp_acct_new", placeholder="e.g. HDFC CC 7500")

            if st.button("🔍  Preview Categorised Rows", use_container_width=True):
                if "— skip —" in [date_col, merch_col, amt_col]:
                    st.error("Map Date, Merchant, and Amount columns.")
                else:
                    cats_df2  = load_categories()
                    prev_rows = []
                    for _, r in raw.iterrows():
                        raw_m = str(r.get(merch_col,"")).strip()
                        try:
                            raw_a = float(str(r.get(amt_col,0)).replace(",","").replace("₹","").replace("$",""))
                        except:
                            raw_a = 0
                        if type_col != "— skip —":
                            tv     = str(r.get(type_col,"")).upper()
                            signed = abs(raw_a) if ("CR" in tv or "CREDIT" in tv) else -abs(raw_a)
                            tval   = "Income" if signed > 0 else "Expense"
                        else:
                            signed = raw_a
                            tval   = "Income" if raw_a > 0 else "Expense"
                        cat, sub, conf = auto_cat(raw_m, cats_df2)
                        prev_rows.append({
                            "Date": str(r.get(date_col,"")),
                            "Merchant": raw_m, "Amount": signed,
                            "Category": cat, "Subcategory": sub, "Type": tval,
                            "Account": imp_acct,
                            "Confidence": "✅ Auto" if conf=="high" else "⚠️ Review",
                        })
                    st.session_state.preview_rows = prev_rows
                    st.rerun()

        except Exception as ex:
            st.error(f"Could not read file: {ex}")

    # ── CONFIRM IMPORT
    if st.session_state.preview_rows:
        prev  = st.session_state.preview_rows
        pv_df = pd.DataFrame(prev)
        st.markdown(f"<div style='font-weight:700;margin:10px 0 4px'>{len(prev)} transactions ready to import</div>", unsafe_allow_html=True)
        st.dataframe(pv_df[["Date","Merchant","Amount","Category","Account","Confidence"]],
                     use_container_width=True, height=260)

        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅  Confirm Import", use_container_width=True, type="primary"):
                existing = load_transactions()
                rows_to_save, skipped = [], 0
                for pr in prev:
                    is_dup = False
                    if not existing.empty:
                        mask = (existing["Merchant"].str.lower() == pr["Merchant"].lower()) & \
                               (existing["Amount"] == pr["Amount"])
                        if mask.any():
                            is_dup = True
                    if not is_dup:
                        rows_to_save.append([
                            str(uuid.uuid4())[:8], pr["Date"], pr["Merchant"], pr["Amount"],
                            pr["Type"], pr["Category"], pr["Subcategory"],
                            "Imported", pr.get("Account",""), "", "import",
                            "yes" if pr["Confidence"]=="✅ Auto" else "no",
                        ])
                    else:
                        skipped += 1
                if rows_to_save:
                    _bulk_write_txns(rows_to_save)
                st.session_state.preview_rows = None
                st.success(f"✅  Imported {len(rows_to_save)} rows. {skipped} duplicates skipped.")
                st.rerun()
        with c2:
            if st.button("✕  Cancel", use_container_width=True):
                st.session_state.preview_rows = None; st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN — ANALYTICS (INSIGHTS)
# ═══════════════════════════════════════════════════════════════════════════════

def screen_analytics():
    df       = load_transactions()
    settings = load_settings()
    budgets  = load_budgets()
    sym      = settings.get("currency_symbol","₹")

    c_t, c_r = st.columns([5,1])
    with c_t:
        st.markdown('<div class="page-title">Insights 📊</div>', unsafe_allow_html=True)
    with c_r:
        if st.button("🔄", key="ana_reload", help="Reload"):
            st.cache_data.clear(); st.rerun()

    if df.empty:
        st.markdown(f"""<div class="card" style="text-align:center;padding:36px">
            <div style="font-size:2.5rem">📊</div>
            <div style="font-weight:800;margin:10px 0">No data yet</div>
        </div>""", unsafe_allow_html=True)
        return

    MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    latest_a  = df.dropna(subset=["Date"]).sort_values("Date",ascending=False).iloc[0]["Date"]
    def_am    = latest_a.month - 1
    years_a   = sorted(df["Date"].dropna().dt.year.unique().astype(int).tolist())
    def_ay_i  = years_a.index(latest_a.year) if latest_a.year in years_a else len(years_a)-1

    c1, c2 = st.columns(2)
    with c1:
        a_m  = st.selectbox("", MONTHS, index=def_am, key="a_m", label_visibility="collapsed")
        a_mn = MONTHS.index(a_m) + 1
    with c2:
        a_y = st.selectbox("", years_a, index=def_ay_i, key="a_y", label_visibility="collapsed")

    # ── ACCOUNT FILTER (pill buttons)
    all_accounts = extract_accounts(df)
    if all_accounts:
        st.markdown(f'<div class="section-label">Account Filter</div>', unsafe_allow_html=True)
        acct_filter_opts = ["All"] + all_accounts
        acct_cols = st.columns(min(len(acct_filter_opts), 5))
        for i, acct in enumerate(acct_filter_opts[:5]):
            with acct_cols[i]:
                on = (st.session_state.ana_acct_filter == acct)
                lbl = acct if acct != "All" else "All"
                # Truncate long labels
                display_lbl = lbl[:8] + "…" if len(lbl) > 9 and lbl != "All" else lbl
                st.markdown(f'<div class="{"pill-on" if on else "pill-off"}">', unsafe_allow_html=True)
                if st.button(display_lbl, key=f"ana_acct_{acct}"):
                    st.session_state.ana_acct_filter = acct; st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

    ms, me = month_range(int(a_y), int(a_mn))
    mdf    = df[(df["Date"].dt.date >= ms) & (df["Date"].dt.date <= me)]

    # Apply account filter
    mdf    = filter_by_account(mdf, st.session_state.ana_acct_filter)
    exp_df = mdf[mdf["Amount"] < 0].copy()

    if exp_df.empty:
        filter_label = f"{st.session_state.ana_acct_filter} · " if st.session_state.ana_acct_filter != "All" else ""
        st.info(f"No expense data for {filter_label}{a_m} {a_y}.")
        return

    exp_df["Abs"] = exp_df["Amount"].abs()
    total_exp = exp_df["Abs"].sum()

    # Show active filter badge
    if st.session_state.ana_acct_filter != "All":
        st.markdown(f"""
        <div style="margin:6px 0;font-size:.75rem;color:{C['muted']}">
            Showing: {account_badge_html(st.session_state.ana_acct_filter, inline=True)}
            &nbsp;<span style="color:{C['expense']};font-family:'JetBrains Mono',monospace;font-weight:700">{sym}{total_exp:,.0f}</span> total
        </div>""", unsafe_allow_html=True)

    # ── CAT / SUBCAT TOGGLE
    c_tog1, c_tog2 = st.columns(2)
    with c_tog1:
        st.markdown(f'<div class="{"pill-on" if st.session_state.cat_view=="Category" else "pill-off"}">', unsafe_allow_html=True)
        if st.button("By Category", key="tog_cat"):
            st.session_state.cat_view = "Category"; st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
    with c_tog2:
        st.markdown(f'<div class="{"pill-on" if st.session_state.cat_view=="Subcategory" else "pill-off"}">', unsafe_allow_html=True)
        if st.button("By Subcategory", key="tog_sub"):
            st.session_state.cat_view = "Subcategory"; st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    group_col = st.session_state.cat_view

    # ── DONUT
    st.markdown('<div class="section-label">Spending Breakdown</div>', unsafe_allow_html=True)
    grp_tot = exp_df.groupby(group_col)["Abs"].sum().reset_index()
    grp_tot.columns = ["Label","Amount"]
    grp_tot = grp_tot.sort_values("Amount", ascending=False)

    PALETTE = ["#7c6df8","#00c896","#ff4f6d","#f0a500","#58a6ff",
               "#a78bfa","#34d399","#fb7185","#fbbf24","#60a5fa","#c084fc","#2dd4bf"]

    fig_d = px.pie(grp_tot, values="Amount", names="Label",
                   hole=0.55, color_discrete_sequence=PALETTE)
    fig_d.update_traces(textposition="outside", textinfo="label+percent", textfont_size=9)
    fig_d.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color=C["text"], showlegend=False,
        margin=dict(l=2,r=2,t=6,b=2), height=240,
    )
    st.plotly_chart(fig_d, use_container_width=True, config={"displayModeBar":False})

    # ── TABLE
    for i, (_, row) in enumerate(grp_tot.iterrows()):
        lbl   = str(row["Label"])
        ico   = cat_icon(lbl)
        pct   = row["Amount"] / total_exp * 100
        bar_w = (row["Amount"] / grp_tot["Amount"].max() * 100) if grp_tot["Amount"].max() > 0 else 0
        st.markdown(f"""
        <div style="padding:5px 2px 4px;border-bottom:1px solid {C['border']}">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:3px">
                <div style="display:flex;align-items:center;gap:6px;flex:1;min-width:0">
                    <span style="font-size:.85rem">{ico}</span>
                    <span style="font-weight:700;font-size:.8rem;white-space:nowrap;overflow:hidden;
                           text-overflow:ellipsis">{lbl}</span>
                </div>
                <div style="text-align:right;flex-shrink:0;margin-left:8px">
                    <span style="font-family:'JetBrains Mono',monospace;color:{C['expense']};
                           font-size:.82rem;font-weight:600">{sym}{row['Amount']:,.0f}</span>
                    <span style="color:{C['muted']};font-size:.65rem;margin-left:4px">{pct:.1f}%</span>
                </div>
            </div>
            <div class="bar-wrap" style="height:4px"><div class="bar-fill"
                 style="width:{bar_w:.0f}%;background:{PALETTE[i % len(PALETTE)]}"></div></div>
        </div>""", unsafe_allow_html=True)

    # ── BUDGET VS ACTUAL
    if not budgets.empty:
        st.markdown('<div class="section-label">Budget vs Actual</div>', unsafe_allow_html=True)
        bmap = dict(zip(budgets["Category"], budgets["MonthlyBudget"].astype(float)))
        for cat, bud in bmap.items():
            if bud <= 0: continue
            actual = exp_df[exp_df["Category"]==cat]["Abs"].sum()
            pct    = min(actual / bud * 100, 100)
            bc     = C["expense"] if pct > 100 else C["warning"] if pct > 80 else C["income"]
            ico    = cat_icon(cat)
            st.markdown(f"""
            <div style="padding:6px 2px;border-bottom:1px solid {C['border']}">
                <div style="display:flex;justify-content:space-between;margin-bottom:3px">
                    <span style="font-weight:700;font-size:.8rem">{ico} {cat}</span>
                    <span style="font-family:'JetBrains Mono',monospace;font-size:.75rem;color:{bc}">
                        {sym}{actual:,.0f} / {sym}{bud:,.0f}</span>
                </div>
                <div class="bar-wrap" style="height:5px">
                    <div class="bar-fill" style="width:{pct:.0f}%;background:{bc}"></div></div>
            </div>""", unsafe_allow_html=True)

    # ── DAILY BARS
    st.markdown('<div class="section-label">Daily Spending</div>', unsafe_allow_html=True)
    daily = exp_df.copy()
    daily["Day"] = daily["Date"].dt.day
    daily = daily.groupby("Day")["Abs"].sum().reset_index()

    fig_b = go.Figure(go.Bar(x=daily["Day"], y=daily["Abs"],
                              marker_color=C["primary"], opacity=0.85))
    fig_b.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color=C["text"],
        xaxis=dict(title="Day of Month", gridcolor=C["border"], dtick=5,
                   tickfont=dict(color=C["muted"],size=9)),
        yaxis=dict(gridcolor=C["border"], tickfont=dict(color=C["muted"],size=9), tickprefix=sym),
        margin=dict(l=2,r=2,t=2,b=2), height=180, showlegend=False,
    )
    st.plotly_chart(fig_b, use_container_width=True, config={"displayModeBar":False})

    # ── 6-MONTH TREND (respects account filter)
    st.markdown('<div class="section-label">6-Month Trend</div>', unsafe_allow_html=True)
    all_exp = filter_by_account(df, st.session_state.ana_acct_filter)
    all_exp = all_exp[all_exp["Amount"]<0].copy()
    all_exp["Mon"] = all_exp["Date"].dt.to_period("M")
    trend   = all_exp.groupby("Mon")["Amount"].sum().abs().reset_index().tail(6)
    trend["MS"] = trend["Mon"].astype(str)
    fig_l = go.Figure(go.Scatter(
        x=trend["MS"], y=trend["Amount"], mode="lines+markers",
        line=dict(color=C["primary"],width=2.5),
        marker=dict(color=C["primary"],size=6),
        fill="tozeroy", fillcolor="rgba(124,109,248,0.1)",
    ))
    fig_l.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color=C["text"],
        xaxis=dict(gridcolor=C["border"], tickfont=dict(color=C["muted"],size=9)),
        yaxis=dict(gridcolor=C["border"], tickfont=dict(color=C["muted"],size=9), tickprefix=sym),
        margin=dict(l=2,r=2,t=2,b=2), height=180, showlegend=False,
    )
    st.plotly_chart(fig_l, use_container_width=True, config={"displayModeBar":False})

    # ── TOP MERCHANTS
    st.markdown('<div class="section-label">Top Merchants</div>', unsafe_allow_html=True)
    tm = exp_df.groupby("Merchant")["Abs"].sum().sort_values(ascending=False).head(8)
    for merchant, amt in tm.items():
        cnt = len(exp_df[exp_df["Merchant"]==merchant])
        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;
             padding:6px 2px;border-bottom:1px solid {C['border']}">
            <div>
                <div style="font-weight:700;font-size:.8rem">{merchant[:30]}</div>
                <div style="font-size:.65rem;color:{C['muted']}">{cnt} txn{"s" if cnt>1 else ""}</div>
            </div>
            <div style="font-family:'JetBrains Mono',monospace;color:{C['expense']};font-size:.85rem;font-weight:600">
                {sym}{amt:,.0f}</div>
        </div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN — SETTINGS
# ═══════════════════════════════════════════════════════════════════════════════

def screen_settings():
    cats_df    = load_categories()
    budgets    = load_budgets()
    settings   = load_settings()
    rules_df   = load_email_rules()
    errors_df  = load_parse_errors()

    st.markdown('<div class="page-title">Settings ⚙️</div>', unsafe_allow_html=True)

    # ── GENERAL
    st.markdown('<div class="section-label">General</div>', unsafe_allow_html=True)
    with st.expander("💱  Currency & Budget", expanded=False):
        sym      = st.text_input("Currency Symbol", value=settings.get("currency_symbol","₹"))
        code     = st.text_input("Currency Code",   value=settings.get("currency_code","INR"))
        m_budget = st.number_input("Monthly Budget",
                                    value=float(settings.get("monthly_budget","30000")),
                                    min_value=0.0, step=1000.0, format="%.0f")
        if st.button("Save", key="save_gen", type="primary"):
            ss  = get_ss(); ws = ss.worksheet("Settings")
            all_v = ws.get_all_values()
            upd   = {"currency_symbol":sym,"currency_code":code,
                     "monthly_budget":str(int(m_budget))}
            for k, v in upd.items():
                found = False
                for i, row in enumerate(all_v[1:], start=2):
                    if row[0] == k:
                        ws.update_cell(i, 2, v); found = True; break
                if not found:
                    ws.append_row([k, v])
            st.cache_data.clear(); st.success("✅  Saved!")

    # ── BUDGETS
    st.markdown('<div class="section-label">Category Budgets</div>', unsafe_allow_html=True)
    with st.expander("🎯  Set Monthly Budget per Category", expanded=False):
        cats = cats_df["Category"].unique().tolist()
        bmap = dict(zip(budgets["Category"], budgets["MonthlyBudget"].astype(float))) if not budgets.empty else {}
        new_bmap = {}
        for cat in cats:
            ico = cat_icon(cat)
            val = st.number_input(f"{ico}  {cat}", value=float(bmap.get(cat,0)),
                                   min_value=0.0, step=500.0, format="%.0f", key=f"b_{cat}")
            if val > 0: new_bmap[cat] = val
        if st.button("Save Budgets", key="save_bud", type="primary"):
            ss = get_ss(); ws = ss.worksheet("Budgets")
            ws.clear(); ws.append_row(["Category","MonthlyBudget"])
            if new_bmap:
                ws.append_rows([[c,a] for c,a in new_bmap.items()])
            st.cache_data.clear(); st.success("✅  Budgets saved!")

    # ── KEYWORD RULES
    st.markdown('<div class="section-label">Smart Categorisation</div>', unsafe_allow_html=True)
    with st.expander("🤖  Edit Keyword Rules", expanded=False):
        st.markdown(f"<div style='color:{C['muted']};font-size:.8rem;margin-bottom:10px'>Comma-separated keywords for auto-categorisation.</div>", unsafe_allow_html=True)
        kw_updates = {}
        for _, row in cats_df.iterrows():
            key    = f"kw_{row['Category']}_{row['Subcategory']}"
            new_kw = st.text_input(
                f"{row.get('Icon','📌')}  {row['Category']} › {row['Subcategory']}",
                value=row.get("Keywords",""), key=key)
            kw_updates[(row["Category"], row["Subcategory"])] = new_kw
        if st.button("Save Rules", key="save_kw", type="primary"):
            ss = get_ss(); ws = ss.worksheet("Categories")
            ws.clear(); ws.append_row(HEADERS["Categories"])
            rows_to_save = []
            for _, row in cats_df.iterrows():
                rows_to_save.append([
                    row["Category"], row["Subcategory"],
                    kw_updates.get((row["Category"],row["Subcategory"]), row.get("Keywords","")),
                    row.get("Icon","📌"),
                ])
            ws.append_rows(rows_to_save)
            st.cache_data.clear(); st.success("✅  Rules updated!")

    # ── ADD CATEGORY
    st.markdown('<div class="section-label">Categories</div>', unsafe_allow_html=True)
    with st.expander("➕  Add New Category / Subcategory", expanded=False):
        nc = st.text_input("Category Name",            key="nc_name")
        ns = st.text_input("Subcategory Name",         key="nc_sub")
        nk = st.text_input("Keywords (comma-sep)",     key="nc_kw")
        ni = st.text_input("Icon (emoji)", value="📌", key="nc_icon")
        if st.button("Add", key="add_cat", type="primary"):
            if nc and ns:
                get_ss().worksheet("Categories").append_row([nc,ns,nk,ni])
                st.cache_data.clear()
                st.success(f"✅  Added {ni} {nc} › {ns}")
            else:
                st.error("Enter both Category and Subcategory.")

    # ════════════════════════════════════════════════════════════════════════
    #  EMAIL IMPORT RULES  — v2.0
    #  Rules stored in EmailRules sheet.
    #  Code.gs reads them on each trigger run and imports matching emails.
    # ════════════════════════════════════════════════════════════════════════
    st.markdown('<div class="section-label">Email Import Rules</div>', unsafe_allow_html=True)

    # ── RUN NOW button
    cn1, cn2 = st.columns([3, 1])
    with cn1:
        st.markdown(f"<div style='font-size:.8rem;color:{C['muted']};padding:6px 0'>"
                    f"Rules are run automatically by Code.gs daily trigger. "
                    f"Tap <b style='color:{C['text']}'>Queue Run Now</b> to flag an immediate run "
                    f"— Code.gs will execute it on its next wake.</div>",
                    unsafe_allow_html=True)
    with cn2:
        if st.button("▶ Queue Run Now", key="btn_run_now", type="primary",
                     use_container_width=True):
            try:
                trigger_run_now()
                st.success("✅ Run queued! Code.gs will pick it up shortly.")
            except Exception as ex:
                st.error(f"Could not queue: {ex}")

    # ── IMPORT LOG
    with st.expander("📊  Recent Import Log", expanded=False):
        try:
            log_df = load_importlog()
            if log_df.empty:
                st.markdown(f"<div style='color:{C['muted']};font-size:.82rem'>No import runs yet.</div>",
                            unsafe_allow_html=True)
            else:
                show_log = log_df.tail(10).iloc[::-1].reset_index(drop=True)
                for _, lr in show_log.iterrows():
                    imp_raw = str(lr.get("Imported","0"))
                    imp_num = ''.join(filter(str.isdigit, imp_raw)) or "0"
                    imp_c   = C["income"] if int(imp_num) > 0 else C["muted"]
                    ts      = str(lr.get("Timestamp",""))[:16]
                    skipped = str(lr.get("Skipped",""))
                    files   = str(lr.get("Files",""))
                    rule_n  = str(lr.get("RuleName","")).strip()
                    st.markdown(f"""
                    <div style="display:flex;justify-content:space-between;align-items:center;
                         padding:7px 4px;border-bottom:1px solid {C['border']};font-size:.77rem">
                        <div>
                            <div style="font-weight:700;color:{C['text']}">{ts}</div>
                            <div style="color:{C['muted']};font-size:.68rem">
                                {files}{(' · ' + rule_n) if rule_n else ''}
                            </div>
                        </div>
                        <div style="text-align:right">
                            <span style="color:{imp_c};font-family:'JetBrains Mono',monospace;
                                   font-weight:700">+{imp_num}</span>
                            <span style="color:{C['muted']};font-size:.68rem;margin-left:6px">
                                {skipped} skipped</span>
                        </div>
                    </div>""", unsafe_allow_html=True)
        except Exception:
            st.info("Import log not available yet.")

    # ── PARSE ERRORS
    if not errors_df.empty:
        with st.expander(f"⚠️  Parse Errors ({len(errors_df)})", expanded=False):
            st.markdown(f"<div style='color:{C['muted']};font-size:.78rem;margin-bottom:8px'>"
                        f"These emails matched the sender/subject filter but the body template "
                        f"could not extract data. Refine the template below.</div>",
                        unsafe_allow_html=True)
            for _, er in errors_df.tail(10).iloc[::-1].iterrows():
                ts       = str(er.get("Timestamp",""))[:16]
                rule_n   = str(er.get("RuleName",""))
                reason   = str(er.get("ErrorReason",""))
                snippet  = str(er.get("BodySnippet",""))[:120]
                st.markdown(f"""
                <div style="background:rgba(255,79,109,.07);border:1px solid rgba(255,79,109,.25);
                     border-radius:10px;padding:10px 12px;margin:4px 0;font-size:.75rem">
                    <div style="display:flex;justify-content:space-between">
                        <span style="font-weight:800;color:{C['expense']}">{rule_n}</span>
                        <span style="color:{C['muted']}">{ts}</span>
                    </div>
                    <div style="color:{C['warning']};margin:3px 0">{reason}</div>
                    <div style="color:{C['muted']};font-family:'JetBrains Mono',monospace;
                         font-size:.65rem;word-break:break-all">{snippet}…</div>
                </div>""", unsafe_allow_html=True)
            if st.button("🗑️ Clear Parse Errors", key="clear_parse_err"):
                try:
                    ss = get_ss(); ws = ss.worksheet("ParseErrors")
                    ws.clear(); ws.append_row(HEADERS["ParseErrors"])
                    st.cache_data.clear(); st.success("✅ Cleared.")
                    st.rerun()
                except Exception as ex:
                    st.error(str(ex))

    # ── EXISTING RULES LIST
    with st.expander("📋  Active Email Rules", expanded=not rules_df.empty):
        if rules_df.empty:
            st.markdown(f"<div style='color:{C['muted']};font-size:.83rem;padding:8px 0'>"
                        f"No rules yet. Add one below.</div>", unsafe_allow_html=True)
        else:
            for _, rule in rules_df.iterrows():
                is_active  = str(rule.get("Active","TRUE")).upper() in ("TRUE","YES","1")
                is_dry     = str(rule.get("DryRun","FALSE")).upper()  in ("TRUE","YES","1")
                acct_lbl   = str(rule.get("AccountLabel","")).strip()
                lookback   = str(rule.get("LookbackDays","2")).strip()
                last_run   = str(rule.get("LastRun","—")).strip()[:16]
                last_imp   = str(rule.get("LastImported","—")).strip()
                active_c   = C["income"] if is_active else C["muted"]

                st.markdown(f"""
                <div style="background:{C['surface2']};border:1px solid {C['border']};
                     border-radius:12px;padding:12px 14px;margin:6px 0">
                    <div style="display:flex;justify-content:space-between;align-items:flex-start">
                        <div style="flex:1;min-width:0">
                            <div style="font-weight:800;font-size:.9rem">{rule['RuleName']}</div>
                            <div style="font-size:.7rem;color:{C['muted']};margin-top:2px">
                                From: <span style="color:{C['info']}">{rule['Sender']}</span>
                            </div>
                            <div style="font-size:.7rem;color:{C['muted']}">
                                Subject: <em>{rule.get('SubjectContains','')}</em>
                            </div>
                            <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:4px;align-items:center">
                                {(account_badge_html(acct_lbl, inline=True)) if acct_lbl else ''}
                                <span style="font-size:.62rem;color:{C['muted']}">📅 {lookback}d lookback</span>
                                {('<span style="font-size:.62rem;color:' + C['warning'] + '">🔬 DRY RUN</span>') if is_dry else ''}
                            </div>
                        </div>
                        <div style="text-align:right;flex-shrink:0;margin-left:10px">
                            <div style="font-size:.65rem;font-weight:800;color:{active_c}">
                                {'● ACTIVE' if is_active else '○ OFF'}
                            </div>
                            <div style="font-size:.6rem;color:{C['muted']};margin-top:4px">
                                Last run: {last_run}
                            </div>
                            <div style="font-size:.6rem;color:{C['income'] if last_imp.isdigit() and int(last_imp)>0 else C['muted']}">
                                +{last_imp} imported
                            </div>
                        </div>
                    </div>
                    <div style="margin-top:8px;font-size:.66rem;color:{C['muted']};
                         background:{C['bg']};border-radius:8px;padding:6px 10px;
                         font-family:'JetBrains Mono',monospace;line-height:1.6;
                         word-break:break-all">{rule.get('BodyTemplate','')}</div>
                </div>""", unsafe_allow_html=True)

                ca, cb, cc = st.columns(3)
                with ca:
                    tog_lbl = "⏸ Disable" if is_active else "▶ Enable"
                    if st.button(tog_lbl, key=f"tog_{rule['RuleName']}",
                                 use_container_width=True):
                        _update_email_rule(rule["RuleName"],
                                           {"Active": "FALSE" if is_active else "TRUE"})
                        st.rerun()
                with cb:
                    new_dry = "FALSE" if is_dry else "TRUE"
                    dry_lbl = "🔴 Live mode" if is_dry else "🔬 Dry run"
                    if st.button(dry_lbl, key=f"dry_{rule['RuleName']}",
                                 use_container_width=True):
                        _update_email_rule(rule["RuleName"], {"DryRun": new_dry})
                        st.rerun()
                with cc:
                    if st.button("🗑️ Delete", key=f"del_{rule['RuleName']}",
                                 use_container_width=True):
                        _delete_email_rule(rule["RuleName"])
                        st.rerun()

    # ── ADD NEW RULE
    with st.expander("➕  Add New Email Rule", expanded=False):
        st.markdown(f"""
        <div style="background:{C['surface2']};border-radius:10px;padding:10px 12px;
             font-size:.78rem;color:{C['muted']};line-height:1.8;margin-bottom:10px">
            Use placeholders in Body Template:<br>
            <span style="color:{C['primary']};font-family:'JetBrains Mono',monospace">{{amt}}</span> amount &nbsp;
            <span style="color:{C['warning']};font-family:'JetBrains Mono',monospace">{{act}}</span> account text &nbsp;
            <span style="color:{C['info']};font-family:'JetBrains Mono',monospace">{{tdetails}}</span> merchant &nbsp;
            <span style="color:{C['income']};font-family:'JetBrains Mono',monospace">{{date}}</span> date &nbsp;
            <span style="color:{C['muted']};font-family:'JetBrains Mono',monospace">{{skip}}</span> discard<br>
            <b>HDFC:</b> <span style="font-family:'JetBrains Mono',monospace;font-size:.7rem">
            Rs.{{amt}} is debited from your {{act}} towards {{tdetails}} on {{date}}</span><br>
            <b>SBI:</b> <span style="font-family:'JetBrains Mono',monospace;font-size:.7rem">
            Rs.{{amt}} spent on your {{skip}} {{act}} at {{tdetails}} on {{date}}.</span>
        </div>""", unsafe_allow_html=True)

        r_name    = st.text_input("Rule Name *",        placeholder="e.g. HDFC Credit Card", key="nr_name")
        r_sender  = st.text_input("Sender Email *",     placeholder="alerts@hdfcbank.bank.in", key="nr_sender")
        r_subject = st.text_input("Subject Contains",   placeholder="debited via Credit Card", key="nr_subject")
        r_template= st.text_area("Body Template *",
                      placeholder="Rs.{amt} is debited from your {act} towards {tdetails} on {date}",
                      key="nr_template", height=80, label_visibility="visible")

        c_a, c_b = st.columns(2)
        with c_a:
            r_lookback = st.number_input("Lookback Days", value=2, min_value=1,
                                          max_value=30, step=1, key="nr_lookback",
                                          help="How many days back Code.gs scans Gmail for this rule")
            r_deftype  = st.selectbox("Transaction Type",
                                       ["Debit (Expense)","Credit (Income)"], key="nr_deftype")
        with c_b:
            r_acct  = st.text_input("Account Label *",
                                     placeholder="HDFC CC 7500  /  SBI CC 4996", key="nr_acct",
                                     help="Stored in Tags column — used for account filtering")
            r_dry   = st.toggle("🔬 Dry Run (test without saving)", value=False, key="nr_dry")

        # ── LIVE TEST PARSER (runs in browser via Python)
        st.markdown(f"<div style='color:{C['muted']};font-size:.75rem;margin:10px 0 2px'>"
                    f"<b>Test your template</b> — paste a sample email body:</div>",
                    unsafe_allow_html=True)
        test_body = st.text_area("Sample email body", key="nr_test_body",
                                  height=80, label_visibility="collapsed",
                                  placeholder="Paste the full email notification text here…")
        if st.button("🔍 Test Parse", key="nr_test_btn"):
            if r_template.strip() and test_body.strip():
                result = parse_email_body(r_template.strip(), test_body.strip())
                st.session_state.email_parse_result = result
                st.rerun()
            else:
                st.warning("Enter both template and sample body.")

        epr = st.session_state.get("email_parse_result")
        if epr is not None:
            if epr:
                amt_v  = clean_amount(epr.get("amt",""))
                td_v   = epr.get("tdetails","—")
                act_v  = epr.get("act","—")
                dt_v   = epr.get("date","—")
                sym_s  = settings.get("currency_symbol","₹")
                acct_d = r_acct.strip() or act_v
                st.markdown(f"""
                <div style="background:rgba(0,200,150,.08);border:1px solid rgba(0,200,150,.3);
                     border-radius:12px;padding:12px 14px;margin:8px 0">
                    <div style="font-size:.63rem;font-weight:800;letter-spacing:1px;
                           color:{C['income']};text-transform:uppercase;margin-bottom:8px">
                        ✅ Parse successful</div>
                    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:.78rem">
                        <div><span style="color:{C['muted']}">Amount</span><br>
                             <span style="font-family:'JetBrains Mono',monospace;
                             color:{C['expense']};font-weight:700">
                             {sym_s}{amt_v:,.2f if amt_v else 0}</span></div>
                        <div><span style="color:{C['muted']}">Merchant</span><br>
                             <span style="font-weight:700">{td_v}</span></div>
                        <div><span style="color:{C['muted']}">Account Tag</span><br>
                             {account_badge_html(acct_d, inline=True)}</div>
                        <div><span style="color:{C['muted']}">Date (raw)</span><br>
                             <span style="font-weight:600">{dt_v}</span></div>
                    </div>
                </div>""", unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="background:rgba(255,79,109,.08);border:1px solid rgba(255,79,109,.3);
                     border-radius:12px;padding:12px;margin:8px 0;font-size:.82rem;
                     color:{C['expense']}">
                    ❌ No match. Check the template text matches this email exactly.</div>""",
                    unsafe_allow_html=True)

        if st.button("💾 Save Rule", key="nr_save", type="primary",
                     use_container_width=True):
            if r_name.strip() and r_sender.strip() and r_template.strip() and r_acct.strip():
                existing_names = rules_df["RuleName"].tolist() if not rules_df.empty else []
                if r_name.strip() in existing_names:
                    st.error("A rule with this name already exists.")
                else:
                    _write_email_rule({
                        "RuleName":        r_name.strip(),
                        "Sender":          r_sender.strip(),
                        "SubjectContains": r_subject.strip(),
                        "BodyTemplate":    r_template.strip(),
                        "DateFormat":      "",
                        "DefaultType":     "Expense" if "Debit" in r_deftype else "Income",
                        "AccountLabel":    r_acct.strip(),
                        "Active":          "TRUE",
                        "DryRun":          "TRUE" if r_dry else "FALSE",
                        "LookbackDays":    str(r_lookback),
                        "LastRun":         "",
                        "LastImported":    "",
                    })
                    st.session_state.email_parse_result = None
                    st.success(f"✅ Rule '{r_name.strip()}' saved!")
                    st.rerun()
            else:
                st.error("Rule Name, Sender, Body Template, and Account Label are required.")

    # ── EXPORT
    st.markdown('<div class="section-label">Data</div>', unsafe_allow_html=True)
    with st.expander("📤  Export Transactions", expanded=False):
        df_exp = load_transactions()
        if not df_exp.empty:
            csv = df_exp.to_csv(index=False).encode("utf-8")
            st.download_button("⬇️  Download all transactions as CSV", data=csv,
                file_name=f"clearspend_{date.today()}.csv",
                mime="text/csv", use_container_width=True)
        else:
            st.info("No transactions to export yet.")

    # ── ABOUT
    st.markdown("---")
    st.markdown(f"""
    <div style="text-align:center;padding:20px 0 8px;color:{C['muted']};font-size:.8rem">
        <div style="font-size:1.8rem">💳</div>
        <div style="font-weight:900;color:{C['text']};font-size:1rem;margin:6px 0">ClearSpend v2.0</div>
        <div>Multi-Account · Unified Email Import · Google Sheets</div>
    </div>""", unsafe_allow_html=True)




def run_setup():
    if not st.session_state.setup_ok:
        with st.spinner("⚡ Setting up ClearSpend..."):
            try:
                ensure_sheets()
                st.session_state.setup_ok = True
            except Exception as ex:
                st.error(f"**Setup failed:** {ex}")
                st.markdown("""
**What to check:**
1. `GOOGLE_CREDENTIALS` secret is set in Streamlit Cloud → App Settings → Secrets.
2. The service account JSON has Google Sheets + Drive API enabled.
3. The service account email has been given Editor access to the spreadsheet.
""")
                st.stop()


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    init_state()
    inject_css()
    run_setup()

    render_top_bar()
    nav = st.session_state.nav
    if   nav == "home":         screen_home()
    elif nav == "transactions": screen_transactions()
    elif nav == "add":          screen_add()
    elif nav == "analytics":    screen_analytics()
    elif nav == "settings":     screen_settings()


if __name__ == "__main__":
    main()