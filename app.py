# ═══════════════════════════════════════════════════════════════════════════════
#  CLEARSPEND — Personal Expense Tracker
#  Mobile-First · Google Sheets Backend · Smart Categorisation
#  Version 1.0
# ═══════════════════════════════════════════════════════════════════════════════

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
import json, uuid, re, calendar
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
}

PAYMENT_METHODS = ["UPI","Credit Card","Debit Card","Cash","Net Banking","Wallet","BNPL"]

DEFAULT_CATEGORIES = [
    ["Food & Dining",     "Restaurants",   "restaurant,cafe,dhaba,biryani,pizza,burger,shawarma,mcdonalds,kfc,subway,dominos,haldirams,barbeque", "🍽️"],
    ["Food & Dining",     "Delivery",      "swiggy,zomato,dunzo,magicpin,delivery,eatsure,licious", "🛵"],
    ["Food & Dining",     "Groceries",     "bigbasket,blinkit,zepto,grofers,dmart,reliance fresh,more,supermarket,grocery,vegetables,kiranas,jiomart", "🛒"],
    ["Food & Dining",     "Coffee & Tea",  "starbucks,cafe coffee,ccd,barista,third wave,chai,tea,coffee", "☕"],
    ["Transport",         "Fuel",          "bpcl,hp petrol,indian oil,iocl,shell,fuel,petrol,diesel,cng,pump,bharat petroleum", "⛽"],
    ["Transport",         "Cab & Auto",    "ola,uber,rapido,namma yatri,auto,taxi,cab,meru,indrive,bluemart", "🚕"],
    ["Transport",         "Public",        "metro,bmrc,bus,bmtc,ksrtc,msrtc,local train,suburban,railway,ksrtc", "🚌"],
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

def ensure_sheets():
    ss = get_ss()
    existing = [ws.title for ws in ss.worksheets()]
    for name, hdrs in HEADERS.items():
        if name not in existing:
            ws = ss.add_worksheet(title=name, rows=2000, cols=len(hdrs))
            ws.append_row(hdrs)
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

@st.cache_data(ttl=20)
def load_transactions():
    ss = get_ss()
    data = ss.worksheet("Transactions").get_all_records()
    if not data:
        return pd.DataFrame(columns=HEADERS["Transactions"])
    df = pd.DataFrame(data)
    df["Date"]   = pd.to_datetime(df["Date"], errors="coerce")
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
    return df

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


# ═══════════════════════════════════════════════════════════════════════════════
#  SMART CATEGORISATION
# ═══════════════════════════════════════════════════════════════════════════════

def auto_cat(merchant: str, cats_df: pd.DataFrame):
    """Keyword match → (category, subcategory, confidence)."""
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
#  HELPERS
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

/* center + cap width */
[data-testid="stAppViewContainer"] > .main {{
    max-width: 480px;
    margin: 0 auto;
    padding: 0 0 100px 0 !important;
}}

.block-container {{
    padding: 0 12px 8px !important;
    max-width: 480px !important;
}}

/* hide chrome */
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

/* ── NAV BUTTONS (real st.button) ── */
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

/* Active nav state via wrapper class */
.nav-on [data-testid="stButton"] > button {{
    color: {C["primary"]} !important;
    background: {C["primary_dim"]} !important;
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
        "nav":          "home",
        "edit_txn":     None,
        "filter_cat":   "All",
        "f_month":      0,   # 0 = auto-detect from data
        "f_year":       0,   # 0 = auto-detect from data
        "search":       "",
        "preview_rows": None,
        "setup_ok":     False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ═══════════════════════════════════════════════════════════════════════════════
#  BOTTOM NAVIGATION
# ═══════════════════════════════════════════════════════════════════════════════

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
                    st.session_state.nav = "add"
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                wrap = "nav-on" if active else ""
                st.markdown(f'<div class="{wrap}">', unsafe_allow_html=True)
                if st.button(f"{icon}\n{label}", key=f"nav_{key}"):
                    st.session_state.nav = key
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  DIALOGS
# ═══════════════════════════════════════════════════════════════════════════════

@st.dialog("✏️ Edit Transaction", width="small")
def dlg_edit(txn):
    cats_df = load_categories()
    cats = cats_df["Category"].unique().tolist()
    cat_idx = cats.index(txn["Category"]) if txn["Category"] in cats else 0

    ttype = st.radio("", ["💸 Expense","💰 Income"], horizontal=True,
                     index=0 if txn.get("Type","Expense") == "Expense" else 1,
                     key="dlg_type")
    amount  = st.number_input("Amount (₹)", value=abs(float(txn["Amount"])),
                               min_value=0.0, step=1.0, format="%.0f", key="dlg_amt")
    merch   = st.text_input("Merchant", value=txn["Merchant"], key="dlg_merch")
    sel_cat = st.selectbox("Category", cats, index=cat_idx, key="dlg_cat")
    subs    = cats_df[cats_df["Category"]==sel_cat]["Subcategory"].tolist()
    sub_idx = subs.index(txn.get("Subcategory","")) if txn.get("Subcategory","") in subs else 0
    sel_sub = st.selectbox("Subcategory", subs, index=sub_idx, key="dlg_sub") if subs else ""
    pm_idx  = PAYMENT_METHODS.index(txn.get("PaymentMethod","UPI")) if txn.get("PaymentMethod","UPI") in PAYMENT_METHODS else 0
    pm      = st.selectbox("Payment Method", PAYMENT_METHODS, index=pm_idx, key="dlg_pm")
    txn_dt  = st.date_input("Date",
                             value=txn["Date"].date() if hasattr(txn["Date"],"date") else date.today(),
                             key="dlg_date")
    notes   = st.text_input("Notes", value=txn.get("Notes",""), key="dlg_notes")

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("💾 Update", use_container_width=True, type="primary"):
            if amount > 0 and merch.strip():
                upd = {
                    "RowID": txn["RowID"], "Date": txn_dt.strftime("%Y-%m-%d"),
                    "Merchant": merch.strip(), "Type": "Expense" if "Expense" in ttype else "Income",
                    "Amount": -abs(amount) if "Expense" in ttype else abs(amount),
                    "Category": sel_cat, "Subcategory": sel_sub,
                    "PaymentMethod": pm, "Tags": txn.get("Tags",""),
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
    if not df.empty:
        mdf = df[(df["Date"].dt.date >= ms) & (df["Date"].dt.date <= me)]
    else:
        mdf = df.copy()

    spent  = abs(mdf[mdf["Amount"] < 0]["Amount"].sum())
    income = mdf[mdf["Amount"] > 0]["Amount"].sum()

    # last-month comparison
    lm = now.month - 1 or 12
    ly = now.year if now.month > 1 else now.year - 1
    lms, lme = month_range(ly, lm)
    lm_spent = abs(df[(df["Date"].dt.date >= lms) & (df["Date"].dt.date <= lme) & (df["Amount"] < 0)]["Amount"].sum()) if not df.empty else 0

    pct    = ((spent - lm_spent) / lm_spent * 100) if lm_spent > 0 else 0
    b_used = min(spent / budget * 100, 100) if budget > 0 else 0
    bar_c  = C["expense"] if b_used > 90 else C["warning"] if b_used > 70 else C["income"]
    d_c    = C["expense"] if pct > 0 else C["income"]
    d_arr  = "▲" if pct > 0 else "▼"

    # ── HEADER
    st.markdown(f"""
    <div style="padding:14px 4px 4px">
        <div style="color:{C['muted']};font-size:.8rem;font-weight:600">{now.strftime('%B %Y')}</div>
        <div style="font-size:1.5rem;font-weight:900;color:{C['text']}">Overview 👋</div>
    </div>""", unsafe_allow_html=True)

    # ── HERO CARD
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
        st.markdown('<div class="section-label">Top Categories</div>', unsafe_allow_html=True)
        exp_df = mdf[mdf["Amount"] < 0]
        if not exp_df.empty:
            top = exp_df.groupby("Category")["Amount"].sum().abs().sort_values(ascending=False).head(5)
            mx  = top.max()
            for cat, amt in top.items():
                ico = cat_icon(cat)
                w   = (amt / mx * 100) if mx > 0 else 0
                st.markdown(f"""
                <div class="card-sm">
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:5px">
                        <div style="display:flex;align-items:center;gap:8px">
                            <span style="font-size:1.1rem">{ico}</span>
                            <span style="font-weight:700;font-size:.88rem">{cat}</span>
                        </div>
                        <span class="mono" style="font-size:.88rem;color:{C['expense']}">{sym}{amt:,.0f}</span>
                    </div>
                    <div class="bar-wrap"><div class="bar-fill" style="width:{w:.0f}%;background:{C['primary']}"></div></div>
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
            amt = row["Amount"]
            ac  = C["income"] if amt > 0 else C["expense"]
            sg  = "+" if amt > 0 else "−"
            ico = cat_icon(row["Category"])
            ds  = row["Date"].strftime("%d %b") if pd.notna(row["Date"]) else ""
            st.markdown(f"""
            <div class="txn-row">
                <div class="txn-icon">{ico}</div>
                <div style="flex:1;min-width:0;overflow:hidden">
                    <div style="font-weight:700;font-size:.88rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{row['Merchant']}</div>
                    <div style="font-size:.72rem;color:{C['muted']}">{row['Category']} · {ds}</div>
                </div>
                <div class="mono" style="color:{ac};font-size:.9rem;flex-shrink:0">{sg}{sym}{abs(amt):,.0f}</div>
            </div>""", unsafe_allow_html=True)

        if st.button("View All Transactions →", use_container_width=True):
            st.session_state.nav = "transactions"
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN — TRANSACTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def screen_transactions():
    df       = load_transactions()
    settings = load_settings()
    sym      = settings.get("currency_symbol","₹")

    st.markdown('<div class="page-title">Transactions 📋</div>', unsafe_allow_html=True)

    # ── SEARCH
    q = st.text_input("", placeholder="🔍  Search merchant...", key="txn_q",
                      value=st.session_state.search, label_visibility="collapsed")
    st.session_state.search = q

    # ── MONTH / YEAR
    c1, c2 = st.columns(2)
    MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    # Auto-detect most recent month that has data
    if (st.session_state.f_month == 0 or st.session_state.f_year == 0) and not df.empty:
        latest = df.dropna(subset=["Date"]).sort_values("Date", ascending=False).iloc[0]["Date"]
        st.session_state.f_month = int(latest.month)
        st.session_state.f_year  = int(latest.year)
    elif st.session_state.f_month == 0:
        st.session_state.f_month = datetime.today().month
        st.session_state.f_year  = datetime.today().year
    # Build year list from actual data range
    if not df.empty:
        years = sorted(df["Date"].dropna().dt.year.unique().astype(int).tolist())
    else:
        years = list(range(datetime.today().year - 2, datetime.today().year + 1))
    if st.session_state.f_year not in years:
        st.session_state.f_year = years[-1]
    c1t, c2t = st.columns(2)
    with c1t:
        sel_m = st.selectbox("Month", MONTHS, index=st.session_state.f_month - 1, key="t_month",
                              label_visibility="collapsed")
        st.session_state.f_month = MONTHS.index(sel_m) + 1
    with c2t:
        sel_y = st.selectbox("Year", years,
                              index=years.index(st.session_state.f_year),
                              key="t_year", label_visibility="collapsed")
        st.session_state.f_year = int(sel_y)

    # ── FILTER
    filtered = df.copy()
    if not filtered.empty:
        ms, me = month_range(st.session_state.f_year, st.session_state.f_month)
        filtered = filtered[(filtered["Date"].dt.date >= ms) & (filtered["Date"].dt.date <= me)]
    if q:
        filtered = filtered[filtered["Merchant"].str.contains(q, case=False, na=False)]

    # ── SUMMARY BAR
    if not filtered.empty:
        tot_exp = abs(filtered[filtered["Amount"] < 0]["Amount"].sum())
        tot_inc = filtered[filtered["Amount"] > 0]["Amount"].sum()
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"""<div class="card-sm" style="text-align:center">
                <div style="font-size:.6rem;color:{C['muted']};font-weight:800;letter-spacing:.8px;text-transform:uppercase">Expenses</div>
                <div class="mono" style="color:{C['expense']}">{sym}{tot_exp:,.0f}</div>
            </div>""", unsafe_allow_html=True)
        with c2:
            st.markdown(f"""<div class="card-sm" style="text-align:center">
                <div style="font-size:.6rem;color:{C['muted']};font-weight:800;letter-spacing:.8px;text-transform:uppercase">Income</div>
                <div class="mono" style="color:{C['income']}">{sym}{tot_inc:,.0f}</div>
            </div>""", unsafe_allow_html=True)

    # ── CATEGORY PILLS
    cats = ["All"] + sorted(filtered["Category"].dropna().unique().tolist()) if not filtered.empty else ["All"]
    if st.session_state.filter_cat not in cats:
        st.session_state.filter_cat = "All"

    pill_cols = st.columns(min(len(cats), 5))
    for i, cat in enumerate(cats[:5]):
        with pill_cols[i]:
            on = cat == st.session_state.filter_cat
            st.markdown(f'<div class="{"pill-on" if on else "pill-off"}">', unsafe_allow_html=True)
            if st.button(cat[:8], key=f"pill_{cat}"):
                st.session_state.filter_cat = cat
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    if st.session_state.filter_cat != "All":
        filtered = filtered[filtered["Category"] == st.session_state.filter_cat]

    # ── LIST
    if filtered.empty:
        st.markdown(f"""<div class="card" style="text-align:center;padding:36px;margin-top:12px">
            <div style="font-size:2rem">🔍</div>
            <div style="font-weight:800;margin:8px 0">No transactions</div>
            <div style="color:{C['muted']};font-size:.85rem">Try a different month or search term</div>
        </div>""", unsafe_allow_html=True)
        return

    for day, grp in sorted(
        filtered.sort_values("Date", ascending=False).groupby(filtered["Date"].dt.date),
        reverse=True
    ):
        day_total = grp["Amount"].sum()
        dc = C["income"] if day_total >= 0 else C["expense"]
        day_str = pd.Timestamp(day).strftime("%a, %d %b")
        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;margin:12px 0 5px;padding:0 2px">
            <div style="font-size:.68rem;font-weight:800;color:{C['muted']};letter-spacing:.8px;text-transform:uppercase">{day_str}</div>
            <div class="mono" style="font-size:.78rem;color:{dc}">{"+" if day_total>=0 else "−"}{sym}{abs(day_total):,.0f}</div>
        </div>""", unsafe_allow_html=True)

        for _, row in grp.iterrows():
            amt = row["Amount"]
            ac  = C["income"] if amt > 0 else C["expense"]
            sg  = "+" if amt > 0 else "−"
            ico = cat_icon(row["Category"])
            ab  = '<span class="badge-auto">AUTO</span>' if str(row.get("AutoCat","")).lower() == "yes" else ""
            pm  = f" · {row['PaymentMethod']}" if row.get("PaymentMethod") else ""

            c1, c2 = st.columns([5,1])
            with c1:
                st.markdown(f"""
                <div class="txn-row">
                    <div class="txn-icon">{ico}</div>
                    <div style="flex:1;min-width:0;overflow:hidden">
                        <div style="font-weight:700;font-size:.88rem">{row['Merchant']}</div>
                        <div style="font-size:.7rem;color:{C['muted']};margin-top:2px">
                            <span class="badge-cat">{row['Category']}</span>{pm} {ab}
                        </div>
                    </div>
                    <div class="mono" style="color:{ac};font-size:.9rem;flex-shrink:0">{sg}{sym}{abs(amt):,.0f}</div>
                </div>""", unsafe_allow_html=True)
            with c2:
                if st.button("✏️", key=f"e_{row['RowID']}", help="Edit"):
                    st.session_state.edit_txn = row.to_dict()
                    st.rerun()

    if st.session_state.edit_txn:
        dlg_edit(st.session_state.edit_txn)


# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN — ADD
# ═══════════════════════════════════════════════════════════════════════════════

def screen_add():
    cats_df  = load_categories()
    settings = load_settings()
    sym      = settings.get("currency_symbol","₹")
    cats     = cats_df["Category"].unique().tolist()

    st.markdown('<div class="page-title">Add Transaction ➕</div>', unsafe_allow_html=True)

    ttype = st.radio("", ["💸 Expense","💰 Income"], horizontal=True, key="add_type")
    is_exp = "Expense" in ttype

    with st.form("add_form", clear_on_submit=True):
        amount  = st.number_input(f"Amount ({sym})", min_value=0.0, step=1.0, format="%.0f")
        merch   = st.text_input("Merchant / Description", placeholder="e.g. Swiggy, BESCOM, Salary...")

        c1, c2 = st.columns(2)
        with c1:
            sel_cat = st.selectbox("Category", cats)
        with c2:
            subs = cats_df[cats_df["Category"]==sel_cat]["Subcategory"].tolist()
            sel_sub = st.selectbox("Subcategory", subs) if subs else ""

        c3, c4 = st.columns(2)
        with c3:
            pm = st.selectbox("Payment Method", PAYMENT_METHODS)
        with c4:
            txn_date = st.date_input("Date", value=date.today())

        tags  = st.text_input("Tags (optional)", placeholder="vacation, work, gift...")
        notes = st.text_input("Notes (optional)", placeholder="Quick note...")

        if st.form_submit_button("💾  Save Transaction", use_container_width=True, type="primary"):
            if amount > 0 and merch.strip():
                _write_txn({
                    "RowID": str(uuid.uuid4())[:8],
                    "Date":  txn_date.strftime("%Y-%m-%d"),
                    "Merchant": merch.strip().title(),
                    "Amount": -abs(amount) if is_exp else abs(amount),
                    "Type":   "Expense" if is_exp else "Income",
                    "Category":    sel_cat,
                    "Subcategory": sel_sub,
                    "PaymentMethod": pm,
                    "Tags": tags, "Notes": notes,
                    "Source": "manual", "AutoCat": "no",
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
                date_col   = st.selectbox("📅 Date column",              all_cols, key="imp_date")
                merch_col  = st.selectbox("🏪 Merchant/Description",     all_cols, key="imp_merch")
            with c2:
                amt_col    = st.selectbox("💰 Amount column",            all_cols, key="imp_amt")
                type_col   = st.selectbox("↕️ Dr/Cr column (optional)",  all_cols, key="imp_type")

            if st.button("🔍  Preview Categorised Rows", use_container_width=True):
                if "— skip —" in [date_col, merch_col, amt_col]:
                    st.error("Map Date, Merchant, and Amount columns.")
                else:
                    cats_df2   = load_categories()
                    prev_rows  = []
                    for _, r in raw.iterrows():
                        raw_m = str(r.get(merch_col,"")).strip()
                        try:
                            raw_a = float(str(r.get(amt_col,0)).replace(",","").replace("₹","").replace("$",""))
                        except:
                            raw_a = 0
                        if type_col != "— skip —":
                            tv = str(r.get(type_col,"")).upper()
                            signed = abs(raw_a) if ("CR" in tv or "CREDIT" in tv) else -abs(raw_a)
                            tval   = "Income" if signed > 0 else "Expense"
                        else:
                            signed = raw_a
                            tval   = "Income" if raw_a > 0 else "Expense"
                        cat, sub, conf = auto_cat(raw_m, cats_df2)
                        prev_rows.append({
                            "Date": str(r.get(date_col,"")),
                            "Merchant": raw_m,
                            "Amount": signed,
                            "Category": cat,
                            "Subcategory": sub,
                            "Type": tval,
                            "Confidence": "✅ Auto" if conf=="high" else "⚠️ Review",
                        })
                    st.session_state.preview_rows = prev_rows
                    st.rerun()

        except Exception as ex:
            st.error(f"Could not read file: {ex}")

    # ── CONFIRM IMPORT
    if st.session_state.preview_rows:
        prev = st.session_state.preview_rows
        pv_df = pd.DataFrame(prev)
        st.markdown(f"<div style='font-weight:700;margin:10px 0 4px'>{len(prev)} transactions ready to import</div>", unsafe_allow_html=True)
        st.dataframe(pv_df[["Date","Merchant","Amount","Category","Confidence"]],
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
                            "Imported","","","import",
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
                st.session_state.preview_rows = None
                st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN — ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════════

def screen_analytics():
    df       = load_transactions()
    settings = load_settings()
    budgets  = load_budgets()
    sym      = settings.get("currency_symbol","₹")

    st.markdown('<div class="page-title">Insights 📊</div>', unsafe_allow_html=True)

    if df.empty:
        st.markdown(f"""<div class="card" style="text-align:center;padding:48px">
            <div style="font-size:3rem">📊</div>
            <div style="font-weight:800;font-size:1.1rem;margin:12px 0">No data yet</div>
            <div style="color:{C['muted']}">Add transactions to unlock insights</div>
        </div>""", unsafe_allow_html=True)
        return

    MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    # Default to most recent month with data
    if not df.empty:
        latest_a = df.dropna(subset=["Date"]).sort_values("Date", ascending=False).iloc[0]["Date"]
        def_am = latest_a.month - 1
        min_ay = int(df["Date"].dt.year.min())
        max_ay = int(df["Date"].dt.year.max())
        years_a = list(range(min_ay, max_ay + 1))
        def_ay_idx = years_a.index(latest_a.year) if latest_a.year in years_a else len(years_a)-1
    else:
        def_am = datetime.today().month - 1
        years_a = list(range(datetime.today().year-2, datetime.today().year+1))
        def_ay_idx = len(years_a)-1

    c1, c2 = st.columns(2)
    with c1:
        a_m  = st.selectbox("", MONTHS, index=def_am, key="a_m", label_visibility="collapsed")
        a_mn = MONTHS.index(a_m) + 1
    with c2:
        a_y   = st.selectbox("", years_a, index=def_ay_idx, key="a_y", label_visibility="collapsed")

    ms, me = month_range(int(a_y), int(a_mn))
    mdf    = df[(df["Date"].dt.date >= ms) & (df["Date"].dt.date <= me)]
    exp_df = mdf[mdf["Amount"] < 0].copy()

    if exp_df.empty:
        st.info(f"No expense data for {a_m} {a_y}.")
        return

    exp_df["Abs"] = exp_df["Amount"].abs()
    total_exp = exp_df["Abs"].sum()

    # ── DONUT
    st.markdown('<div class="section-label">Spending by Category</div>', unsafe_allow_html=True)
    cat_tot = exp_df.groupby("Category")["Abs"].sum().reset_index()
    cat_tot.columns = ["Category","Amount"]

    fig_d = px.pie(
        cat_tot, values="Amount", names="Category",
        hole=0.55,
        color_discrete_sequence=["#7c6df8","#00c896","#ff4f6d","#f0a500","#58a6ff",
                                  "#a78bfa","#34d399","#fb7185","#fbbf24","#60a5fa",
                                  "#c084fc","#2dd4bf"],
    )
    fig_d.update_traces(textposition="outside", textinfo="label+percent", textfont_size=10)
    fig_d.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color=C["text"], showlegend=False,
        margin=dict(l=4,r=4,t=8,b=4), height=270,
    )
    st.plotly_chart(fig_d, use_container_width=True, config={"displayModeBar":False})

    for _, row in cat_tot.sort_values("Amount", ascending=False).iterrows():
        ico = cat_icon(row["Category"])
        pct = row["Amount"] / total_exp * 100
        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 2px;border-bottom:1px solid {C['border']}">
            <div style="display:flex;align-items:center;gap:8px">
                <span>{ico}</span>
                <span style="font-weight:700;font-size:.88rem">{row['Category']}</span>
            </div>
            <div style="text-align:right">
                <div class="mono" style="color:{C['expense']};font-size:.88rem">{sym}{row['Amount']:,.0f}</div>
                <div style="font-size:.68rem;color:{C['muted']}">{pct:.1f}%</div>
            </div>
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
            <div class="card-sm">
                <div style="display:flex;justify-content:space-between;margin-bottom:5px">
                    <span style="font-weight:700;font-size:.88rem">{ico} {cat}</span>
                    <span class="mono" style="font-size:.78rem;color:{bc}">{sym}{actual:,.0f} / {sym}{bud:,.0f}</span>
                </div>
                <div class="bar-wrap"><div class="bar-fill" style="width:{pct:.0f}%;background:{bc}"></div></div>
            </div>""", unsafe_allow_html=True)

    # ── DAILY BARS
    st.markdown('<div class="section-label">Daily Spending</div>', unsafe_allow_html=True)
    daily = exp_df.groupby(exp_df["Date"].dt.day)["Abs"].sum().reset_index()
    daily.columns = ["Day","Amount"]

    fig_b = go.Figure(go.Bar(
        x=daily["Day"], y=daily["Amount"],
        marker_color=C["primary"], opacity=0.85,
    ))
    fig_b.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color=C["text"],
        xaxis=dict(title="Day", gridcolor=C["border"], tickfont=dict(color=C["muted"],size=10)),
        yaxis=dict(gridcolor=C["border"], tickfont=dict(color=C["muted"],size=10)),
        margin=dict(l=4,r=4,t=4,b=4), height=200, showlegend=False,
    )
    st.plotly_chart(fig_b, use_container_width=True, config={"displayModeBar":False})

    # ── 6-MONTH TREND
    st.markdown('<div class="section-label">6-Month Trend</div>', unsafe_allow_html=True)
    all_exp = df[df["Amount"] < 0].copy()
    all_exp["Month"] = all_exp["Date"].dt.to_period("M")
    trend   = all_exp.groupby("Month")["Amount"].sum().abs().reset_index().tail(6)
    trend["MS"] = trend["Month"].astype(str)

    fig_l = go.Figure(go.Scatter(
        x=trend["MS"], y=trend["Amount"],
        mode="lines+markers",
        line=dict(color=C["primary"], width=2.5),
        marker=dict(color=C["primary"], size=7),
        fill="tozeroy", fillcolor="rgba(124,109,248,0.1)",
    ))
    fig_l.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color=C["text"],
        xaxis=dict(gridcolor=C["border"], tickfont=dict(color=C["muted"],size=10)),
        yaxis=dict(gridcolor=C["border"], tickfont=dict(color=C["muted"],size=10)),
        margin=dict(l=4,r=4,t=4,b=4), height=200, showlegend=False,
    )
    st.plotly_chart(fig_l, use_container_width=True, config={"displayModeBar":False})

    # ── TOP MERCHANTS
    st.markdown('<div class="section-label">Top Merchants</div>', unsafe_allow_html=True)
    tm = exp_df.groupby("Merchant")["Abs"].sum().sort_values(ascending=False).head(8)
    for merchant, amt in tm.items():
        cnt = len(exp_df[exp_df["Merchant"]==merchant])
        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 2px;border-bottom:1px solid {C['border']}">
            <div>
                <div style="font-weight:700;font-size:.88rem">{merchant}</div>
                <div style="font-size:.7rem;color:{C['muted']}">{cnt} transaction{'s' if cnt>1 else ''}</div>
            </div>
            <div class="mono" style="color:{C['expense']};font-size:.9rem">{sym}{amt:,.0f}</div>
        </div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN — SETTINGS
# ═══════════════════════════════════════════════════════════════════════════════

def screen_settings():
    cats_df  = load_categories()
    budgets  = load_budgets()
    settings = load_settings()

    st.markdown('<div class="page-title">Settings ⚙️</div>', unsafe_allow_html=True)

    # ── GENERAL
    st.markdown('<div class="section-label">General</div>', unsafe_allow_html=True)
    with st.expander("💱  Currency & Budget", expanded=False):
        sym      = st.text_input("Currency Symbol", value=settings.get("currency_symbol","₹"))
        code     = st.text_input("Currency Code", value=settings.get("currency_code","INR"))
        m_budget = st.number_input("Monthly Budget", value=float(settings.get("monthly_budget","30000")),
                                   min_value=0.0, step=1000.0, format="%.0f")
        if st.button("Save", key="save_gen", type="primary"):
            ss  = get_ss()
            ws  = ss.worksheet("Settings")
            all_v = ws.get_all_values()
            upd   = {"currency_symbol":sym,"currency_code":code,"monthly_budget":str(int(m_budget))}
            for k, v in upd.items():
                found = False
                for i, row in enumerate(all_v[1:], start=2):
                    if row[0] == k:
                        ws.update_cell(i, 2, v)
                        found = True; break
                if not found:
                    ws.append_row([k, v])
            st.cache_data.clear()
            st.success("✅  Saved!")

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
            if val > 0:
                new_bmap[cat] = val
        if st.button("Save Budgets", key="save_bud", type="primary"):
            ss = get_ss(); ws = ss.worksheet("Budgets")
            ws.clear(); ws.append_row(["Category","MonthlyBudget"])
            if new_bmap:
                ws.append_rows([[c,a] for c,a in new_bmap.items()])
            st.cache_data.clear()
            st.success("✅  Budgets saved!")

    # ── KEYWORD RULES
    st.markdown('<div class="section-label">Smart Categorisation</div>', unsafe_allow_html=True)
    with st.expander("🤖  Edit Keyword Rules", expanded=False):
        st.markdown(f"<div style='color:{C['muted']};font-size:.8rem;margin-bottom:10px'>Comma-separated. When a merchant name contains these words it auto-assigns to that category.</div>", unsafe_allow_html=True)
        kw_updates = {}
        for _, row in cats_df.iterrows():
            key = f"kw_{row['Category']}_{row['Subcategory']}"
            new_kw = st.text_input(
                f"{row.get('Icon','📌')}  {row['Category']} › {row['Subcategory']}",
                value=row.get("Keywords",""), key=key,
            )
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
            st.cache_data.clear()
            st.success("✅  Rules updated!")

    # ── ADD CATEGORY
    st.markdown('<div class="section-label">Categories</div>', unsafe_allow_html=True)
    with st.expander("➕  Add New Category / Subcategory", expanded=False):
        nc = st.text_input("Category Name", key="nc_name")
        ns = st.text_input("Subcategory Name", key="nc_sub")
        nk = st.text_input("Keywords (comma-separated)", key="nc_kw")
        ni = st.text_input("Icon (emoji)", value="📌", key="nc_icon")
        if st.button("Add", key="add_cat", type="primary"):
            if nc and ns:
                ss = get_ss(); ws = ss.worksheet("Categories")
                ws.append_row([nc,ns,nk,ni])
                st.cache_data.clear()
                st.success(f"✅  Added {ni} {nc} › {ns}")
            else:
                st.error("Enter both Category and Subcategory.")

    # ── EXPORT
    st.markdown('<div class="section-label">Data</div>', unsafe_allow_html=True)
    with st.expander("📤  Export Transactions", expanded=False):
        df = load_transactions()
        if not df.empty:
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️  Download all transactions as CSV", data=csv,
                file_name=f"clearspend_{date.today()}.csv",
                mime="text/csv", use_container_width=True,
            )
        else:
            st.info("No transactions to export yet.")

    # ── GMAIL IMPORT HELP
    st.markdown('<div class="section-label">Gmail Auto-Import</div>', unsafe_allow_html=True)
    with st.expander("📧  How to set up Gmail auto-import", expanded=False):
        st.markdown(f"""
<div style="color:{C['muted']};font-size:.82rem;line-height:1.7">

**Step 1 — Open Google Apps Script**
Go to [script.google.com](https://script.google.com) and create a new project.

**Step 2 — Paste the Code.gs script**
Copy the full contents of Code.gs (provided alongside this app) into the editor.

**Step 3 — Set your configuration**
At the top of Code.gs, set:
- `SENDER_EMAIL` → your bank's email address
- `SPREADSHEET_ID` → your ClearSpend Google Sheet ID (from the URL)

**Step 4 — Authorise & deploy**
Run the script once manually (▶ Run), grant Gmail + Sheets permissions.

**Step 5 — Set a daily trigger**
Triggers → Add trigger → `importBankEmails` → Time-driven → Day timer.

That's it. New bank emails with XLSX/CSV attachments will auto-import daily.

</div>""", unsafe_allow_html=True)

    # ── ABOUT
    st.markdown("---")
    st.markdown(f"""
    <div style="text-align:center;padding:20px 0 8px;color:{C['muted']};font-size:.8rem">
        <div style="font-size:1.8rem">💳</div>
        <div style="font-weight:900;color:{C['text']};font-size:1rem;margin:6px 0">ClearSpend v1.0</div>
        <div>Personal Expense Tracker · Google Sheets</div>
    </div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  SETUP
# ═══════════════════════════════════════════════════════════════════════════════

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

    nav = st.session_state.nav
    if   nav == "home":         screen_home()
    elif nav == "transactions": screen_transactions()
    elif nav == "add":          screen_add()
    elif nav == "analytics":    screen_analytics()
    elif nav == "settings":     screen_settings()

    render_nav()


if __name__ == "__main__":
    main()
