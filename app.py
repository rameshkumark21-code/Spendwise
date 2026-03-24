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
    "ParseErrors":      ["Timestamp","RuleName","Sender","Subject","BodySnippet","ErrorReason"],
    "MerchantAliases":  ["RawName","CanonicalName","LastUpdated"],
    "TelegramSettings": ["Key","Value"],
}

PAYMENT_METHODS = ["UPI","Credit Card","Debit Card","Cash","Net Banking","Wallet","BNPL"]

DEFAULT_CATEGORIES = [
    ["Bills & Utilities", "Credit Card Payment",
     "sbi card,sbi cards,icici bank credit card,hdfc credit,axis credit,kotak credit,credit card payment",
     "💳"],
    ["Bills & Utilities", "Electricity",
     "bescom,tangedco,bangalore electricity supply,bangalore electricity,electricity,eb bill,tneb,wbsedcl,msedcl,adani electricity,tata power",
     "⚡"],
    ["Bills & Utilities", "LPG & Gas",
     "trupti enterprises,sahaya pragash kumar,lpg,cylinder,indane,bharat gas,mahanagar gas,gas",
     "🔥"],
    ["Bills & Utilities", "Mobile & Recharge",
     "recharge of airtel,recharge of bsnl,airtel mobile,bsnl mobile,vodafone idea,airtel money,airtel,bsnl,vodafone,vi vodafone,jio,act fibernet,hathway,recharge,broadband,postpaid,mobile",
     "📡"],
    ["Bills & Utilities", "OTT & Subscriptions",
     "automatic payment for jiohotstar,automatic payment for netflix,automatic payment of,jiohotstar,netflix,zee5,airtelxstream,hotstar,sonyliv,prime video,spotify,youtube premium,apple music,adobe,notion,disney",
     "📺"],
    ["Entertainment", "Movies & Events",
     "theatre sri guru,nexus shantiniketan,garuda mall,pvr,inox,bookmyshow,cinepolis,movie,theatre",
     "🎬"],
    ["Entertainment", "Outings & Activities",
     "namma bengaluru aquarium,the royal park,kiosk 2 mayura,balbhavan,bal bhavan,aquarium,amusement,theme park",
     "🎪"],
    ["Entertainment", "Spiritual & Temples",
     "tirupathi tirumala devasthanams,tmsm krpm,tirumala,tirupathi,devasthanam,devasthanams,temple,church,mosque",
     "🛕"],
    ["Food & Dining", "Groceries",
     "zeptonow,zepto,blinkit,bigbasket,grofers,dmart,jiomart,eco hypermarket,vasantham,vasantham super market,m s vasantham,kpn farm fresh,hap daily,sri bhuvaneswary rice traders,marudhar mart,family choice,amul,supermarket,hypermarket,farm fresh,grocery,rice traders",
     "🛒"],
    ["Food & Dining", "Restaurants & Mess",
     "hungerbox,udupi kitchen,udupi gokula,shrayanka foods,sendhoor coffee,salted chilli restaurant,sai akshiya bhavan,sri acharya bhavan,shree lakshmi bhavan,daalchini,chai biskut,box bites cafe,basha bhai biryani,avenue food plaza,arv donne biryani,andhra aatithyam,adyar ananda bhavan,aasai aasai,a1 tandoori,alankar cafe,alagar mess,amman coffee,bhavana s,b2b biriyani,gopizza,guntur vari amma,guntur andhra mess,hotel nellore ruchul,hotel shri lakshmi,hyderabadi biryani adda,ippopay merchant,kps restaurant,lulus bakery,madurai bun parotta,mr subburaj,restaurant,mess,bhavan,biryani,biriyani,parotta,tiffin,cafe,dhaba,hotel nellore,hotel shri",
     "🍽️"],
    ["Food & Dining", "Snacks & Sweets",
     "zam zam sweets,triveni vada pav,teaman,t t tea stall,suketha shetty,sri durga bakery,southern foods,shivani chats and sweets,sattur snacks,rathina madhapan,reddemma k,paban kundu,nuts n chocos,nrk sweets bakery,moideen kunhi,madhappan marappan,kuchen helado,kanti sweets,instant retail india,hasanamba iyengar bakery,gopal krishna shetty,fresh juice house,devishree juice,adavan bakery,a sweets and snacks,a m tasty bakery,bakery,sweets,snacks,juice,tea stall,vada pav",
     "🍬"],
    ["Food & Dining", "Vegetables & Meat",
     "sri vinayaga vegetables,ms sri vinayaka vegitables,navyashree vegetable suppliers,my chicken and more,sagar fish and chicken,bismila chicken center,vegetables,vegetable,chicken,fish and chicken,meat,fish",
     "🥩"],
    ["Health", "Hospital & Clinic",
     "aristo speciality hospital,sri manjunatha hospital,tirumala orthopaedic,m s sanjivani child care,ms sri meenakshi diagnostic,ms santhi s s sankarnarayanan,sankaranarayanan karuppaiya,narayanaswamy k,s narayanaswamy,sakumalla satyanarayana,chebrolu lakshminarayana,gulab babu,hospital,clinic,doctor,diagnostics,blood test,lab,fortis,manipal,narayana,apollo hospital",
     "🏥"],
    ["Health", "Pharmacy",
     "apollo pharmacy,16428 apollo pharmacy,16012 apollo pharmacy,8 meds pharmacy,m s sanjivani pharma,sanjivani pharma,sulochana medicals,vijaya medicals,pavan medicals,ramdev medical,orsun pharmacy,pradhan mantri bhartiya janaushadhi kendra,wellnessmedicals,janaushadhi,medplus,1mg,pharmeasy,netmeds,pharmacy,medicals,medicine",
     "💊"],
    ["Personal Care", "Photography",
     "sen studio,studio,photography",
     "📸"],
    ["Personal Care", "Salon & Grooming",
     "dugdha parlour,salon,spa,haircut,beauty,grooming,nails,parlour,jawed habib",
     "💇"],
    ["Shopping", "Clothing",
     "the chennai silks,rainbow kids,pinkz,lifestyle,westside,pantaloons,max fashion,clothing,apparel",
     "👗"],
    ["Shopping", "Online",
     "amazon india,amazon,flipkart,meesho,myntra,ajio,nykaa,snapdeal,tata cliq",
     "📦"],
    ["Shopping", "Retail & Stores",
     "vishal mega mart,sri vijayalakshmi stores,rps electricals,it digital store,surya fancey gift senter,thaim zone,croma,vijay sales",
     "🏪"],
    ["Transport", "Cab & Ride",
     "rapido,roppen transportation,dikson k,ola,uber,namma yatri,indrive,cab,taxi",
     "🚕"],
    ["Transport", "Fuel",
     "jefema fuel mart,padmashree fuels,vetri fuels,oshan energy,le konn energy stations,kavya petro,j k enterprises old madras,s v m fuel station,muthu filling station,sri parvathy filling station,filling station,fuel mart,fuel station,petrol pump,petrol,diesel,cng,iocl,bpcl,hp petrol,indian oil,shell,bharat petroleum,vriddhi fuels",
     "⛽"],
    ["Transport", "Metro & Bus",
     "bengaluru metro qr,bmtc bus,tamilnadu state transport,tamil nadu state transport,bengaluru metro,metro qr,bmtc,ksrtc,msrtc,bus,metro",
     "🚌"],
    ["Transport", "Tours & Travel Agent",
     "madhulika tours and travels,giripugal travels,tours and travels",
     "🗺️"],
    ["Transport", "Train",
     "irctc_app_upi,irctc connect app,irctc mpp,irctc cf,irctc app upi,irctc,indian railways uts,indian railways",
     "🚂"],
    ["Travel", "Hotels & Stays",
     "tvl jay priya residency,syed tourist home,sri sai hotels,sri ganesh residency,hotel gowri,hotel aadhithya,ganesh residency,jay priya residency,residency,tourist home,lodge,oyo,treebo,airbnb,hotel",
     "🏨"],
    ["Education", "Courses",
     "udemy,coursera,unacademy,vedantu,byjus,course,class,workshop,training,skillshare",
     "📚"],
    ["Investments", "Mutual Funds & SIP",
     "zerodha,groww,kuvera,sip,mutual fund,etf,paytm money,angel,coin by zerodha",
     "📈"],
    ["Investments", "Deposits",
     "fd,ppf,nsc,recurring deposit,fixed deposit,post office savings",
     "🏦"],
    ["Gifts & Social", "Gifts",
     "gift,present,birthday,anniversary,wedding",
     "🎁"],
    ["Gifts & Social", "Donations",
     "donation,charity,ngo,pm relief",
     "🤲"],
    ["Rent & Housing", "Rent",
     "rent,pg,hostel,society maintenance,house rent,landlord",
     "🏠"],
    ["Others", "Miscellaneous",
     "",
     "📌"],
]

DEFAULT_SETTINGS = [
    ["currency_symbol", "₹"],
    ["currency_code",   "INR"],
    ["monthly_budget",  "30000"],
    ["app_name",        "ClearSpend"],
]


# ── DEFAULT EMAIL RULES (auto-seeded on first run) ─────────────────────────
DEFAULT_EMAIL_RULES = [
    # HDFC Credit Card alert
    # Sample: Rs.926.31 is debited from your HDFC Bank Credit Card ending 7500 towards PAY*Hindustan Petroleu on 17 Mar, 2026 at 00:21:22.
    ["HDFC Credit Card",
     "alerts@hdfcbank.bank.in",
     "debited via Credit Card",
     "Rs.{amt} is debited from your {act} towards {tdetails} on {date}",
     "use_email_date",
     "Expense",
     "HDFC CC",
     "TRUE",
     "TRUE",   # DryRun = TRUE — user must turn off after verifying
     "2",
     "",
     ""],

    # SBI Credit Card alert
    # Sample: Rs.205.84 spent on your SBI Credit Card ending 4996 at SVMFUELSTATION on 26/09/25.
    ["SBI Credit Card",
     "onlinesbicard@sbicard.com",
     "Transaction Alert from SBI Card",
     "Rs.{amt} spent on your {skip} {act} at {tdetails} on {date}.",
     "use_email_date",
     "Expense",
     "SBI CC",
     "TRUE",
     "TRUE",   # DryRun = TRUE — user must turn off after verifying
     "2",
     "",
     ""],
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
            rows = 500 if name in ("EmailRules","ParseErrors") else 2000
            ws = ss.add_worksheet(title=name, rows=rows, cols=len(hdrs))
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
    # V4: create MerchantAliases and TelegramSettings if missing
    if "MerchantAliases" not in existing:
        ws = ss.add_worksheet(title="MerchantAliases", rows=500, cols=3)
        ws.append_row(HEADERS["MerchantAliases"])
    if "TelegramSettings" not in existing:
        ws = ss.add_worksheet(title="TelegramSettings", rows=50, cols=2)
        ws.append_row(HEADERS["TelegramSettings"])

    # Seed default email rules on first run if sheet is empty
    email_ws = ss.worksheet("EmailRules")
    if len(email_ws.get_all_values()) <= 1 and DEFAULT_EMAIL_RULES:
        email_ws.append_rows(DEFAULT_EMAIL_RULES)

    for title in ["Sheet1"]:
        try:
            ss.del_worksheet(ss.worksheet(title))
        except Exception:
            pass


# ── CRUD ───────────────────────────────────────────────────────────────────────

def _parse_dates(series):
    """
    Parse date strings from Sheets into Timestamps.
    Code.gs writes DD/MM/YYYY. Manual adds/edits write YYYY-MM-DD.
    Both handled correctly — in DD/MM/YYYY, group(1)=day, group(2)=month.
    """
    import re as _re
    DMY  = _re.compile(r'^(\d{1,2})/(\d{1,2})/(\d{4})$')
    ISO  = _re.compile(r'^(\d{4})-(\d{2})-(\d{2})$')
    DMY2 = _re.compile(r'^(\d{1,2})-(\d{1,2})-(\d{4})$')

    def parse_one(v):
        s = str(v).strip()
        if not s or s in ("nan","None","NaT",""):
            return pd.NaT
        # DD/MM/YYYY or D/M/YYYY — Code.gs format, day is group(1), month is group(2)
        m = DMY.match(s)
        if m:
            try:
                # FIXED: group(1)=day, group(2)=month, group(3)=year
                return pd.Timestamp(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except: pass
        # YYYY-MM-DD — manual add/edit format (ISO is already correct)
        m = ISO.match(s)
        if m:
            try: 
                return pd.Timestamp(s)
            except: pass
        # DD-MM-YYYY
        m = DMY2.match(s)
        if m:
            try:
                # FIXED: group(1)=day, group(2)=month, group(3)=year
                return pd.Timestamp(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except: pass
        try: 
            return pd.Timestamp(pd.to_datetime(s, dayfirst=True, errors="coerce"))
        except: 
            return pd.NaT

    return series.apply(parse_one)


def _normalise_date_str(s):
    """Return a clean DD/MM/YYYY string from any common format."""
    if pd.isna(s):
        return ""
    s2 = str(s).strip()
    if not s2:
        return ""
    # Already clean
    if _re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', s2):
        return s2
    if _re.match(r'^\d{4}-\d{2}-\d{2}$', s2):
        return s2
    # DMY with dashes 25-11-2023
    m = _re.match(r'^(\d{1,2})-(\d{1,2})-(\d{4})$', s2)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{a:02d}/{b:02d}/{y}"
    # MDY with slashes 11/25/2023 (US)
    m = _re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', s2)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if a > 12:
            return f"{a:02d}/{b:02d}/{y}"   # a=day (>12), b=month — DD/MM/YYYY
        elif b > 12:
            return f"{b:02d}/{a:02d}/{y}"   # b=day (>12), a=month — swap to DD/MM/YYYY
        else:
            # FIXED: Both ≤12 means ambiguous, but Code.gs convention is DD/MM/YYYY
            # So a=day, b=month → DD/MM/YYYY
            return f"{a:02d}/{b:02d}/{y}"   # DD/MM/YYYY
    # Fallback: let pandas try dayfirst
    try:
        dt = pd.to_datetime(s2, dayfirst=True, errors="raise")
        return dt.strftime("%d/%m/%Y")
    except:
        return ""

def _detect_date_issues(df: pd.DataFrame) -> dict:
    """
    Scan all Date values and classify them.
    Returns dict with counts and lists of row IDs per issue type.
    """
    import re
    results = {
        "total":        len(df),
        "iso":          [],   # YYYY-MM-DD → needs converting
        "short_year":   [],   # MM/DD/YY → needs 4-digit year
        "nat":          [],   # unparseable
        "ok":           [],   # already MM/DD/YYYY
        "suspicious":   [],   # day<=12 AND month<=12 → could be swapped
    }
    for _, row in df.iterrows():
        rid  = str(row.get("RowID",""))
        dval = str(row.get("Date","")).strip()
        if not dval or dval in ("nan","None","NaT",""):
            results["nat"].append(rid); continue

        if re.match(r'^\d{4}-\d{2}-\d{2}', dval):
            results["iso"].append(rid); continue

        m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{2})$', dval)
        if m:
            results["short_year"].append(rid); continue

        m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', dval)
        if m:
            d, mo = int(m.group(1)), int(m.group(2))
            if mo > 12:
                results["suspicious"].append(rid)  # definitely swapped
            else:
                results["ok"].append(rid)
            continue

        results["nat"].append(rid)

    return results


@st.cache_data(ttl=20)
def _load_transactions():
    df = _raw_sheets_data()
    
    # DEBUG: Show what _parse_dates returns
    print("\n=== BEFORE _parse_dates ===")
    print("Raw values:", df['Date'].head(3).tolist())
    
    df['Date'] = _parse_dates(df['Date'])
    
    print("\n=== AFTER _parse_dates ===")
    for i, val in df['Date'].head(3).items():
        print(f"Row {i}: {val} (type: {type(val).__name__})")
    
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

@st.cache_data(ttl=60)
def load_cat_freq():
    """
    Return category and subcategory lists derived from actual transaction history,
    sorted by frequency (most-used first). Falls back to Categories sheet if no
    transactions exist yet. Cached for 60 s — fast enough for smooth UX.

    Returns:
        cats_sorted  : list of category strings (most-used first)
        sub_map      : dict { category: [subcategory, ...] } (most-used first per cat)
    """
    df = load_transactions()
    cats_df = load_categories()

    if df.empty:
        # No transactions — use Categories sheet alphabetically
        cats_sorted = sorted(cats_df["Category"].dropna().unique().tolist())
        sub_map = {}
        for cat in cats_sorted:
            sub_map[cat] = cats_df[cats_df["Category"]==cat]["Subcategory"].dropna().tolist()
        return cats_sorted, sub_map

    # Count category frequency from transactions
    cat_counts = (
        df[df["Category"].notna() & (df["Category"] != "")]
        ["Category"].value_counts()
    )
    # Merge with any cats in Categories sheet not yet in transactions
    all_cats = set(cats_df["Category"].dropna().unique().tolist())
    known_cats = set(cat_counts.index.tolist())
    extra_cats = sorted(all_cats - known_cats)
    cats_sorted = cat_counts.index.tolist() + extra_cats

    # Count subcategory frequency per category
    sub_map = {}
    for cat in cats_sorted:
        # Subs seen in transactions for this category
        cat_df = df[df["Category"] == cat]
        sub_counts = (
            cat_df[cat_df["Subcategory"].notna() & (cat_df["Subcategory"] != "")]
            ["Subcategory"].value_counts()
        )
        subs_from_txns = sub_counts.index.tolist()

        # Subs defined in Categories sheet for this category (for completeness)
        subs_from_cats = (
            cats_df[cats_df["Category"]==cat]["Subcategory"]
            .dropna().unique().tolist()
        )
        # Merge: transaction-ranked first, then any from sheet not yet seen
        extra_subs = [s for s in subs_from_cats if s not in subs_from_txns]
        sub_map[cat] = subs_from_txns + extra_subs

    return cats_sorted, sub_map


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

def _bulk_update_merchant_cat(row_ids: list, new_cat: str, new_sub: str):
    """
    Update Category and Subcategory for a list of RowIDs in one pass.
    Reads the sheet once, patches the relevant rows, writes back only changed cells.
    Also sets AutoCat = 'no' on each updated row.
    """
    if not row_ids:
        return 0
    ss  = get_ss()
    ws  = ss.worksheet("Transactions")
    all_vals = ws.get_all_values()
    hdrs     = all_vals[0]

    try:
        cat_col = hdrs.index("Category")    + 1  # 1-based
        sub_col = hdrs.index("Subcategory") + 1
        id_col  = hdrs.index("RowID")       + 1
        ac_col  = hdrs.index("AutoCat")     + 1
    except ValueError:
        return 0

    id_set    = set(row_ids)
    updated   = 0
    # Batch all updates — one update_cells call per column to minimise API calls
    from gspread.utils import rowcol_to_a1
    cat_updates = []
    sub_updates = []
    ac_updates  = []

    for i, row in enumerate(all_vals[1:], start=2):
        if row[id_col - 1] in id_set:
            cat_updates.append({"range": rowcol_to_a1(i, cat_col), "values": [[new_cat]]})
            sub_updates.append({"range": rowcol_to_a1(i, sub_col), "values": [[new_sub]]})
            ac_updates.append( {"range": rowcol_to_a1(i, ac_col),  "values": [["no"]]})
            updated += 1

    if cat_updates:
        ws.batch_update(cat_updates + sub_updates + ac_updates)

    st.cache_data.clear()
    return updated


# ── MERCHANT ALIAS ─────────────────────────────────────────────────────────

@st.cache_data(ttl=120)
def load_merchant_aliases() -> dict:
    """Return {raw_lower: canonical} lookup dict."""
    ss = get_ss()
    try:
        data = ss.worksheet("MerchantAliases").get_all_records()
        return {str(r["RawName"]).lower().strip(): str(r["CanonicalName"]).strip()
                for r in data if r.get("RawName") and r.get("CanonicalName")}
    except Exception:
        return {}

def resolve_merchant(name: str, aliases: dict) -> str:
    """Apply alias lookup, return canonical name or original."""
    return aliases.get(str(name).lower().strip(), name)

def save_merchant_alias(raw: str, canonical: str):
    ss = get_ss()
    ws = ss.worksheet("MerchantAliases")
    all_v = ws.get_all_values()
    # Update if exists
    for i, row in enumerate(all_v[1:], start=2):
        if row and str(row[0]).lower().strip() == raw.lower().strip():
            ws.update_cell(i, 2, canonical)
            ws.update_cell(i, 3, date.today().isoformat())
            st.cache_data.clear()
            return
    ws.append_row([raw.strip(), canonical.strip(), date.today().isoformat()])
    st.cache_data.clear()

def delete_merchant_alias(raw: str):
    ss = get_ss()
    ws = ss.worksheet("MerchantAliases")
    all_v = ws.get_all_values()
    for i, row in enumerate(all_v[1:], start=2):
        if row and str(row[0]).lower().strip() == raw.lower().strip():
            ws.delete_rows(i); st.cache_data.clear(); return


# ── TELEGRAM ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=120)
def load_telegram_settings() -> dict:
    ss = get_ss()
    try:
        data = ss.worksheet("TelegramSettings").get_all_records()
        return {r["Key"]: r["Value"] for r in data if r.get("Key")}
    except Exception:
        return {}

def save_telegram_setting(key: str, value: str):
    ss = get_ss()
    ws = ss.worksheet("TelegramSettings")
    all_v = ws.get_all_values()
    for i, row in enumerate(all_v[1:], start=2):
        if row and row[0] == key:
            ws.update_cell(i, 2, value)
            st.cache_data.clear(); return
    ws.append_row([key, value])
    st.cache_data.clear()

def send_telegram(bot_token: str, chat_id: str, message: str) -> tuple[bool, str]:
    """Send a Telegram message. Returns (success, error_msg)."""
    import urllib.request as _ur
    import urllib.parse  as _up
    try:
        url  = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = _up.urlencode({
            "chat_id":    chat_id,
            "text":       message,
            "parse_mode": "HTML",
        }).encode()
        req  = _ur.Request(url, data=data)
        resp = _ur.urlopen(req, timeout=8)
        return True, ""
    except Exception as e:
        return False, str(e)

def check_and_send_budget_alerts(df: pd.DataFrame, budgets: pd.DataFrame,
                                  settings: dict, tg_cfg: dict):
    """
    Called on Insights load. Sends Telegram alert when a category exceeds
    the budget_alert_pct threshold (default 80%). Only sends once per month
    per category by recording last-alerted in TelegramSettings.
    """
    if not tg_cfg.get("bot_token") or not tg_cfg.get("chat_id"):
        return
    if budgets.empty or df.empty:
        return

    bot_token = tg_cfg["bot_token"]
    chat_id   = tg_cfg["chat_id"]
    threshold = float(tg_cfg.get("alert_pct", 80))
    sym       = settings.get("currency_symbol", "₹")

    now = datetime.today()
    ms, me = month_range(now.year, now.month)
    mdf    = df[(df["Date"].dt.date >= ms) & (df["Date"].dt.date <= me)]
    exp_df = mdf[mdf["Amount"] < 0].copy()
    if exp_df.empty:
        return

    exp_df["Abs"] = exp_df["Amount"].abs()
    cat_totals    = exp_df.groupby("Category")["Abs"].sum()

    alert_key_prefix = f"tg_alerted_{now.year}_{now.month:02d}_"

    for _, brow in budgets.iterrows():
        cat = str(brow["Category"])
        bud = float(brow["MonthlyBudget"] or 0)
        if bud <= 0 or cat not in cat_totals:
            continue
        actual = cat_totals[cat]
        pct    = actual / bud * 100
        if pct < threshold:
            continue
        # Check if already alerted this month
        alert_key = alert_key_prefix + cat.replace(" ","_")[:20]
        if tg_cfg.get(alert_key):
            continue
        ico = cat_icon(cat)
        msg = (f"\U0001F6A8 <b>Budget Alert \u2014 {ico} {cat}</b>\n\n"
               f"Spent <b>{sym}{actual:,.0f}</b> of {sym}{bud:,.0f} "
               f"({pct:.0f}%) this month.\n"
               f"Remaining: {sym}{max(bud-actual,0):,.0f}")
        ok, _ = send_telegram(bot_token, chat_id, msg)
        if ok:
            save_telegram_setting(alert_key, "sent")


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
        "edit_rule_name":     None,   # RuleName currently being edited
        "filter_sub_cat":     "All",
        "home_cat_view":      "Category",
        "pending_bulk":       None,
        "review_misc_page":   False,  # show bulk-recategorise panel
        "tg_test_result":     None,   # telegram test send result
        "alias_search":       "",     # merchant alias search string
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
    # Frequency-sorted lists — shared cache with Add screen, no extra load
    cats_sorted, sub_map = load_cat_freq()
    df_all = load_transactions()

    cur_cat = str(txn.get("Category",""))
    # Keep current category at index 0 if it exists so dialog opens on it
    cats_ordered = ([cur_cat] + [c for c in cats_sorted if c != cur_cat]
                    if cur_cat and cur_cat in cats_sorted else cats_sorted)
    cat_idx = 0  # always starts on the transaction's own category

    ttype = st.radio("", ["💸 Expense","💰 Income"], horizontal=True,
                     index=0 if txn.get("Type","Expense") == "Expense" else 1,
                     key="dlg_type")
    amount = st.number_input("Amount (₹)", value=abs(float(txn["Amount"])),
                              min_value=0.0, step=1.0, format="%.0f", key="dlg_amt")
    merch  = st.text_input("Merchant", value=txn["Merchant"], key="dlg_merch")

    dlg_cat_opts = cats_ordered + ["➕ New category…"]
    sel_cat_r = st.selectbox("Category", dlg_cat_opts, index=cat_idx, key="dlg_cat")
    if sel_cat_r == "➕ New category…":
        nc = st.text_input("New category", key="dlg_nc")
        ns = st.text_input("First subcategory", key="dlg_ns")
        if st.button("✅ Create", key="dlg_create_cat"):
            if nc.strip() and ns.strip():
                get_ss().worksheet("Categories").append_row([nc.strip(), ns.strip(),"","📌"])
                st.cache_data.clear(); st.rerun()
        sel_cat = cats_ordered[0] if cats_ordered else "Others"
    else:
        sel_cat = sel_cat_r

    # Subcategory — frequency-sorted, current txn sub preselected
    subs = sub_map.get(sel_cat, [])
    cur_sub = str(txn.get("Subcategory",""))
    subs_ordered = ([cur_sub] + [s for s in subs if s != cur_sub]
                    if cur_sub and cur_sub in subs and sel_cat == cur_cat
                    else subs)
    sub_idx = 0
    dlg_sub_opts = subs_ordered + ["➕ New subcategory…"] if subs_ordered else ["➕ New subcategory…"]
    sel_sub_r = st.selectbox("Subcategory", dlg_sub_opts, index=sub_idx, key="dlg_sub")
    if sel_sub_r == "➕ New subcategory…":
        ns2 = st.text_input("New subcategory name", key="dlg_ns2")
        if st.button("✅ Add Sub", key="dlg_create_sub"):
            if ns2.strip():
                get_ss().worksheet("Categories").append_row([sel_cat, ns2.strip(),"","📌"])
                st.cache_data.clear(); st.rerun()
        sel_sub = subs_ordered[0] if subs_ordered else ""
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
                orig_cat = str(txn.get("Category",""))
                orig_sub = str(txn.get("Subcategory",""))
                cat_changed = (sel_cat != orig_cat or sel_sub != orig_sub)

                upd = {
                    "RowID": txn["RowID"], "Date": txn_dt.strftime("%d/%m/%Y"),  # FIXED: DD/MM/YYYY
                    "Merchant": merch.strip(),

                    "Type": "Expense" if "Expense" in ttype else "Income",
                    "Amount": -abs(amount) if "Expense" in ttype else abs(amount),
                    "Category": sel_cat, "Subcategory": sel_sub,
                    "PaymentMethod": pm, "Tags": dlg_acct,
                    "Notes": notes, "Source": txn.get("Source","manual"), "AutoCat": "no",
                }
                _update_txn(txn["RowID"], upd)
                st.session_state.edit_txn = None

                # If category/subcategory changed, queue bulk-suggest for same merchant
                if cat_changed:
                    st.session_state.pending_bulk = {
                        "merchant": merch.strip(),
                        "cat":      sel_cat,
                        "sub":      sel_sub,
                        "skip_id":  txn["RowID"],
                    }

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
#  BULK CATEGORY SUGGEST DIALOG
# ═══════════════════════════════════════════════════════════════════════════════

@st.dialog("🔄 Apply to Similar Transactions", width="small")
def dlg_bulk_suggest():
    """
    Shown after dlg_edit when category/subcategory changed.
    Lists all other transactions for the same merchant and lets the
    user approve or reject applying the same category change to all of them.
    """
    pb       = st.session_state.pending_bulk
    merchant = pb["merchant"]
    new_cat  = pb["cat"]
    new_sub  = pb["sub"]
    skip_id  = pb["skip_id"]

    df_all = load_transactions()
    cats_sorted, sub_map = load_cat_freq()
    settings = load_settings()
    sym      = settings.get("currency_symbol", "₹")

    # All other transactions for this merchant (exclude the one just edited)
    others = df_all[
        (df_all["Merchant"].str.strip().str.lower() == merchant.strip().lower()) &
        (df_all["RowID"].astype(str) != str(skip_id))
    ].copy()

    st.markdown(f"""
    <div style="background:{C['surface2']};border-left:3px solid {C['primary']};
         border-radius:0 10px 10px 0;padding:10px 14px;margin-bottom:12px;font-size:.8rem">
        <div style="font-weight:800;color:{C['text']};margin-bottom:3px">
            {merchant}
        </div>
        <div style="color:{C['muted']}">
            You just set this merchant to
            <span style="color:{C['primary']};font-weight:700">{new_cat} › {new_sub}</span>.
        </div>
    </div>""", unsafe_allow_html=True)

    if others.empty:
        st.markdown(f"<div style='color:{C['muted']};font-size:.82rem;padding:8px 0'>"
                    f"No other past transactions found for this merchant.</div>",
                    unsafe_allow_html=True)
        if st.button("✕ Close", use_container_width=True):
            st.session_state.pending_bulk = None; st.rerun()
        return

    # Show how many are already in this category vs different
    already = len(others[
        (others["Category"] == new_cat) & (others["Subcategory"] == new_sub)
    ])
    different = len(others) - already

    if different == 0:
        st.markdown(f"""
        <div style="background:rgba(0,200,150,.08);border:1px solid rgba(0,200,150,.3);
             border-radius:10px;padding:10px 12px;margin-bottom:10px;font-size:.8rem;
             color:{C['income']}">
            ✅ All {len(others)} past transactions for this merchant are already
            in <b>{new_cat} › {new_sub}</b>. Nothing to update.
        </div>""", unsafe_allow_html=True)
        if st.button("✕ Close", use_container_width=True):
            st.session_state.pending_bulk = None; st.rerun()
        return

    st.markdown(f"""
    <div style="font-size:.78rem;color:{C['muted']};margin-bottom:8px">
        Found <b style="color:{C['text']}">{different}</b> past transaction(s) with a
        different category. Approve to update them all.
        {f"<span style='color:{C['income']}'>({already} already correct)</span>" if already > 0 else ""}
    </div>""", unsafe_allow_html=True)

    # Table of transactions that would be updated
    to_update = others[
        ~((others["Category"] == new_cat) & (others["Subcategory"] == new_sub))
    ].sort_values("Date", ascending=False)

    # Render compact table
    st.markdown(f"""
    <div style="background:{C['bg']};border:1px solid {C['border']};border-radius:10px;
         overflow:hidden;margin-bottom:12px">
        <div style="display:grid;grid-template-columns:2fr 2fr 1.5fr 1fr;
             padding:6px 10px;background:{C['surface2']};
             font-size:.6rem;font-weight:800;letter-spacing:.8px;
             text-transform:uppercase;color:{C['muted']}">
            <span>Category → New</span><span>Subcategory → New</span>
            <span>Amount</span><span>Date</span>
        </div>""", unsafe_allow_html=True)

    for _, row in to_update.head(15).iterrows():
        old_c   = str(row.get("Category",""))
        old_s   = str(row.get("Subcategory",""))
        amt     = row["Amount"]
        ac      = C["income"] if amt > 0 else C["expense"]
        sg      = "+" if amt > 0 else "−"
        ds      = row["Date"].strftime("%d %b %y") if pd.notna(row["Date"]) else "—"
        changed_c = old_c != new_cat
        changed_s = old_s != new_sub

        cat_html = (f'<span style="color:{C["muted"]};text-decoration:line-through;font-size:.62rem">'
                    f'{old_c}</span><br><span style="color:{C["primary"]};font-weight:700">{new_cat}</span>'
                    if changed_c else
                    f'<span style="color:{C["muted"]}">{new_cat}</span>')
        sub_html = (f'<span style="color:{C["muted"]};text-decoration:line-through;font-size:.62rem">'
                    f'{old_s}</span><br><span style="color:{C["primary"]};font-weight:700">{new_sub}</span>'
                    if changed_s else
                    f'<span style="color:{C["muted"]}">{new_sub}</span>')

        st.markdown(f"""
        <div style="display:grid;grid-template-columns:2fr 2fr 1.5fr 1fr;
             padding:7px 10px;border-top:1px solid {C['surface2']};
             font-size:.75rem;align-items:center">
            <span>{cat_html}</span>
            <span>{sub_html}</span>
            <span style="font-family:'JetBrains Mono',monospace;color:{ac};font-weight:600">
                {sg}{sym}{abs(amt):,.0f}</span>
            <span style="color:{C['muted']};font-size:.68rem">{ds}</span>
        </div>""", unsafe_allow_html=True)

    if len(to_update) > 15:
        st.markdown(f"<div style='padding:6px 10px;font-size:.68rem;color:{C['muted']};border-top:1px solid {C['surface2']}'>"
                    f"…and {len(to_update)-15} more</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # ── Category / Subcategory confirmation dropdowns ────────────────────
    st.markdown(f"<div style='font-size:.72rem;color:{C['muted']};margin-bottom:4px'>"
                f"Confirm the category to apply to all {different} transaction(s):</div>",
                unsafe_allow_html=True)

    # Category dropdown pre-filled with new_cat
    all_cats = cats_sorted + ["Other"]
    cat_idx  = all_cats.index(new_cat) if new_cat in all_cats else 0
    confirm_cat = st.selectbox("Category", all_cats, index=cat_idx, key="bulk_cat")

    # Subcategory dropdown pre-filled with new_sub for confirmed category
    all_subs = sub_map.get(confirm_cat, [])
    sub_idx  = all_subs.index(new_sub) if new_sub in all_subs else 0
    confirm_sub = st.selectbox("Subcategory", all_subs if all_subs else [new_sub],
                               index=sub_idx, key="bulk_sub")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Approve / Reject ─────────────────────────────────────────────────
    b1, b2 = st.columns(2)
    with b1:
        if st.button("✅ Approve — Update All", use_container_width=True, type="primary"):
            row_ids = to_update["RowID"].astype(str).tolist()
            n = _bulk_update_merchant_cat(row_ids, confirm_cat, confirm_sub)
            st.session_state.pending_bulk = None
            st.success(f"✅ Updated {n} transaction(s) → {confirm_cat} › {confirm_sub}")
            st.rerun()
    with b2:
        if st.button("✕ Reject — Keep as is", use_container_width=True):
            st.session_state.pending_bulk = None; st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  V4: REVIEW UNCATEGORISED DIALOG
# ═══════════════════════════════════════════════════════════════════════════════

@st.dialog("🗂️ Review Uncategorised Transactions", width="small")
def dlg_review_misc():
    """Bulk-recategorise all transactions in Others › Miscellaneous."""
    df_all = load_transactions()
    cats_sorted, sub_map = load_cat_freq()
    settings = load_settings()
    sym = settings.get("currency_symbol","₹")

    misc = df_all[
        (df_all["Category"].astype(str).str.strip() == "Others") |
        (df_all["Subcategory"].astype(str).str.strip() == "Miscellaneous")
    ].copy()

    if misc.empty:
        st.markdown(f"""
        <div style="background:rgba(0,200,150,.08);border:1px solid rgba(0,200,150,.3);
             border-radius:12px;padding:16px;text-align:center;color:{C['income']}">
            🎉 No uncategorised transactions found!
        </div>""", unsafe_allow_html=True)
        if st.button("✕ Close", use_container_width=True):
            st.session_state.review_misc_page = False; st.rerun()
        return

    st.markdown(f"""
    <div style="font-size:.8rem;color:{C['muted']};margin-bottom:10px">
        <b style="color:{C['text']}">{len(misc)}</b> transactions are in
        Others › Miscellaneous. Group by merchant and assign categories below.
    </div>""", unsafe_allow_html=True)

    # Group by merchant for efficient bulk editing
    merch_groups = misc.groupby("Merchant").agg(
        count=("RowID","count"),
        total=("Amount", lambda x: x.abs().sum()),
        ids=("RowID", list)
    ).reset_index().sort_values("total", ascending=False)

    # Category selector (single for all, or per-merchant)
    st.markdown(f"<div style='font-size:.72rem;color:{C['muted']};margin-bottom:4px'>Apply to all at once:</div>", unsafe_allow_html=True)
    bulk_cat_idx = cats_sorted.index("Others") if "Others" in cats_sorted else 0
    bulk_cat = st.selectbox("Category (bulk)", cats_sorted, index=bulk_cat_idx, key="rm_bulk_cat")
    bulk_subs = sub_map.get(bulk_cat, [])
    bulk_sub = st.selectbox("Subcategory (bulk)", bulk_subs if bulk_subs else ["Miscellaneous"], key="rm_bulk_sub")

    b1, b2 = st.columns(2)
    with b1:
        if st.button(f"✅ Apply to All {len(misc)}", use_container_width=True, type="primary"):
            all_ids = misc["RowID"].astype(str).tolist()
            n = _bulk_update_merchant_cat(all_ids, bulk_cat, bulk_sub)
            st.session_state.review_misc_page = False
            st.success(f"✅ Updated {n} transactions → {bulk_cat} › {bulk_sub}")
            st.rerun()
    with b2:
        if st.button("✕ Close", use_container_width=True):
            st.session_state.review_misc_page = False; st.rerun()

    st.markdown(f'<div class="section-label">By Merchant ({len(merch_groups)} groups)</div>', unsafe_allow_html=True)

    for _, mg in merch_groups.head(20).iterrows():
        mname = str(mg["Merchant"])
        mcount = int(mg["count"])
        mtotal = float(mg["total"])
        mids   = [str(x) for x in mg["ids"]]

        st.markdown(f"""
        <div style="background:{C['surface2']};border:1px solid {C['border']};
             border-radius:10px;padding:9px 12px;margin:5px 0">
            <div style="display:flex;justify-content:space-between;align-items:center">
                <div style="font-weight:700;font-size:.82rem;flex:1;min-width:0;
                     white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{mname[:35]}</div>
                <div style="flex-shrink:0;margin-left:8px;font-size:.72rem;color:{C['muted']}">
                    {mcount} txn · <span style="font-family:'JetBrains Mono',monospace;
                    color:{C['expense']}">{sym}{mtotal:,.0f}</span>
                </div>
            </div>
        </div>""", unsafe_allow_html=True)
        mc1, mc2, mc3 = st.columns([3, 3, 1])
        with mc1:
            cat_k = f"rm_cat_{mname[:20]}"
            mc_idx = cats_sorted.index("Others") if "Others" in cats_sorted else 0
            sel_c = st.selectbox("", cats_sorted, index=mc_idx, key=cat_k,
                                 label_visibility="collapsed")
        with mc2:
            sub_k  = f"rm_sub_{mname[:20]}"
            msubs  = sub_map.get(sel_c, [])
            sel_s  = st.selectbox("", msubs if msubs else ["Miscellaneous"],
                                  key=sub_k, label_visibility="collapsed")
        with mc3:
            if st.button("✓", key=f"rm_apply_{mname[:20]}", help="Apply to this merchant"):
                _bulk_update_merchant_cat(mids, sel_c, sel_s)
                st.toast(f"✓ {mname[:20]} → {sel_c}", icon="✅")
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

    # ── SPENDING BREAKDOWN — compact horizontal bars, tappable rows
    if not mdf.empty:
        exp_df = mdf[mdf["Amount"] < 0].copy()
        if not exp_df.empty:
            exp_df["Abs"] = exp_df["Amount"].abs()
            total_exp_home = exp_df["Abs"].sum()

            PALETTE = ["#7c6df8","#00c896","#ff4f6d","#f0a500","#58a6ff",
                       "#a78bfa","#34d399","#fb7185","#fbbf24","#60a5fa","#c084fc","#2dd4bf"]

            # Toggle
            st.markdown(f'<div class="section-label">Spending Breakdown <span style="font-size:.6rem;color:{C["muted"]}">tap any row to explore</span></div>', unsafe_allow_html=True)
            tog_c1, tog_c2 = st.columns(2)
            with tog_c1:
                is_cat = (st.session_state.home_cat_view == "Category")
                st.markdown(f'<div class="{"pill-on" if is_cat else "pill-off"}">', unsafe_allow_html=True)
                if st.button("Category", key="home_tog_cat"):
                    st.session_state.home_cat_view = "Category"; st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)
            with tog_c2:
                is_sub = (st.session_state.home_cat_view == "Subcategory")
                st.markdown(f'<div class="{"pill-on" if is_sub else "pill-off"}">', unsafe_allow_html=True)
                if st.button("Subcategory", key="home_tog_sub"):
                    st.session_state.home_cat_view = "Subcategory"; st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

            group_col = st.session_state.home_cat_view
            grp = (exp_df.groupby(group_col)["Abs"].sum()
                   .reset_index().rename(columns={group_col:"Label","Abs":"Amount"})
                   .sort_values("Amount", ascending=False))
            max_amt = grp["Amount"].max() if not grp.empty else 1

            for i, (_, row) in enumerate(grp.iterrows()):
                lbl    = str(row["Label"])
                amt    = row["Amount"]
                pct    = amt / total_exp_home * 100
                bar_w  = (amt / max_amt * 100)
                colour = PALETTE[i % len(PALETTE)]
                if group_col == "Category":
                    ico = cat_icon(lbl)
                else:
                    pr = exp_df[exp_df["Subcategory"] == lbl]["Category"]
                    ico = cat_icon(pr.iloc[0]) if not pr.empty else "📂"

                # Compact row: icon | label + bar | amount + pct
                col_info, col_amt = st.columns([6, 2])
                with col_info:
                    st.markdown(f"""
                    <div style="display:flex;align-items:center;gap:7px;padding:3px 0 0">
                        <span style="font-size:.82rem;flex-shrink:0">{ico}</span>
                        <div style="flex:1;min-width:0">
                            <div style="font-weight:700;font-size:.78rem;white-space:nowrap;
                                 overflow:hidden;text-overflow:ellipsis;color:{C['text']}">{lbl}</div>
                            <div class="bar-wrap" style="height:4px;margin:3px 0 2px">
                                <div class="bar-fill"
                                     style="width:{bar_w:.0f}%;background:{colour}"></div>
                            </div>
                        </div>
                    </div>""", unsafe_allow_html=True)
                with col_amt:
                    st.markdown(f"""
                    <div style="text-align:right;padding:3px 0 0">
                        <div style="font-family:'JetBrains Mono',monospace;color:{C['expense']};
                             font-size:.78rem;font-weight:700">{sym}{amt:,.0f}</div>
                        <div style="color:{C['muted']};font-size:.6rem">{pct:.1f}%</div>
                    </div>""", unsafe_allow_html=True)

                # Invisible full-width tap target overlaid using zero-height button
                st.markdown(f'<div style="margin-top:-36px;height:36px;overflow:hidden;">', unsafe_allow_html=True)
                st.markdown(f'<div class="home-cat-btn" style="opacity:0;margin:0">', unsafe_allow_html=True)
                if st.button(lbl, key=f"hb_{i}_{group_col[:3]}", use_container_width=True):
                    st.session_state.nav         = "transactions"
                    st.session_state.f_month     = now.month
                    st.session_state.f_year      = now.year
                    st.session_state.search      = ""
                    st.session_state.acct_filter = "All"
                    if group_col == "Category":
                        st.session_state.filter_cat     = lbl
                        st.session_state.filter_sub_cat = "All"
                    else:
                        pr2 = exp_df[exp_df["Subcategory"] == lbl]["Category"]
                        st.session_state.filter_cat     = pr2.mode()[0] if not pr2.empty else "All"
                        st.session_state.filter_sub_cat = lbl
                    st.rerun()
                st.markdown('</div></div>', unsafe_allow_html=True)

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

        st.markdown(f'<div style="margin-top:10px">', unsafe_allow_html=True)
        if st.button("View All Transactions →", use_container_width=True, type="primary"):
            st.session_state.nav = "transactions"
            st.session_state.filter_cat     = "All"
            st.session_state.filter_sub_cat = "All"
            st.session_state.f_month = 0
            st.session_state.f_year  = 0
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN — TRANSACTIONS (SPENDS)
# ═══════════════════════════════════════════════════════════════════════════════

def screen_transactions():
    df       = load_transactions()
    settings = load_settings()
    sym      = settings.get("currency_symbol","₹")

    c_t, c_r, c_misc = st.columns([5,1,1])
    with c_t:
        st.markdown('<div class="page-title">Spends 📋</div>', unsafe_allow_html=True)
    with c_r:
        if st.button("🔄", key="txn_reload", help="Reload data"):
            st.cache_data.clear(); st.rerun()
    with c_misc:
        if st.button("🗂️", key="btn_review_misc", help="Review uncategorised"):
            st.session_state.review_misc_page = True; st.rerun()

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

    # ── FILTER — always apply month + account, then search within result
    filtered = df.copy()
    if not filtered.empty:
        # Step 1: always apply month/year scope
        ms, me = month_range(st.session_state.f_year, st.session_state.f_month)
        filtered = filtered[(filtered["Date"].dt.date >= ms) & (filtered["Date"].dt.date <= me)]

        # Step 2: apply account filter
        filtered = filter_by_account(filtered, st.session_state.acct_filter)

        # Step 3: if search query, match across all text fields (case-insensitive)
        if q:
            q_lower = q.lower().strip()
            def row_matches(r):
                return any(q_lower in str(r.get(col,"")).lower()
                           for col in ["Merchant","Category","Subcategory",
                                       "Notes","Tags","PaymentMethod"])
            mask = filtered.apply(row_matches, axis=1)
            filtered = filtered[mask]

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
                    st.session_state.filter_cat     = cat
                    st.session_state.filter_sub_cat = "All"  # reset sub on cat change
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)
        if st.session_state.filter_cat != "All":
            filtered = filtered[filtered["Category"]==st.session_state.filter_cat]

    # ── SUBCATEGORY PILLS (only when a category is selected)
    if not filtered.empty and st.session_state.filter_cat != "All":
        subs = filtered["Subcategory"].dropna().unique().tolist()
        if len(subs) > 1:
            sub_opts = ["All"] + sorted(subs)
            if st.session_state.filter_sub_cat not in sub_opts:
                st.session_state.filter_sub_cat = "All"
            sub_cols = st.columns(min(len(sub_opts), 5))
            for i, sub in enumerate(sub_opts[:5]):
                with sub_cols[i]:
                    sub_lbl = sub[:7] + "…" if sub != "All" and len(sub) > 8 else sub
                    on_s = sub == st.session_state.filter_sub_cat
                    st.markdown(f'<div class="{"pill-on" if on_s else "pill-off"}">', unsafe_allow_html=True)
                    if st.button(sub_lbl, key=f"sub_pill_{sub}"):
                        st.session_state.filter_sub_cat = sub; st.rerun()
                    st.markdown('</div>', unsafe_allow_html=True)
            if st.session_state.filter_sub_cat != "All":
                filtered = filtered[filtered["Subcategory"]==st.session_state.filter_sub_cat]

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
            sub   = str(row.get("Subcategory",""))
            pm    = str(row.get("PaymentMethod",""))
            tag   = str(row.get("Tags","")).strip()
            notes = str(row.get("Notes","")).strip()
            auto_badge = ' <span class="badge-auto">A</span>' if str(row.get("AutoCat","")).lower()=="yes" else ""
            acct_badge = f" {account_badge_html(tag, inline=True)}" if tag else ""
            merch = str(row["Merchant"])[:32]
            notes_html = (f'<div style="font-size:.62rem;color:{C["muted"]};margin-top:1px;'
                          f'font-style:italic;white-space:nowrap;overflow:hidden;'
                          f'text-overflow:ellipsis">{notes}</div>') if notes else ""

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
                        </div>{notes_html}
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

    if st.session_state.pending_bulk:
        dlg_bulk_suggest()

    if st.session_state.review_misc_page:
        dlg_review_misc()


# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN — ADD TRANSACTION
# ═══════════════════════════════════════════════════════════════════════════════

def screen_add():
    df_all   = load_transactions()
    settings = load_settings()
    sym      = settings.get("currency_symbol","₹")
    # Frequency-sorted categories from actual transaction history
    cats_sorted, sub_map = load_cat_freq()

    st.markdown('<div class="page-title">Add Transaction ➕</div>', unsafe_allow_html=True)

    ttype  = st.radio("", ["💸 Expense","💰 Income"], horizontal=True, key="add_type")
    is_exp = "Expense" in ttype

    # ── CATEGORY — sorted by frequency
    CAT_OPTIONS = cats_sorted + ["➕ New category…"]
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
        sel_cat = cats_sorted[0] if cats_sorted else "Others"
    else:
        sel_cat = sel_cat_raw

    # ── SUBCATEGORY — frequency-sorted for the selected category
    subs_list = sub_map.get(sel_cat, [])
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
                    "Date":          txn_date.strftime("%d/%m/%Y"),  # FIXED: DD/MM/YYYY
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
                        # Normalise date to MM/DD/YYYY
                        raw_date_str = str(r.get(date_col,"")).strip()
                        norm_date = _normalise_date_str(raw_date_str)
                        prev_rows.append({
                            "Date": norm_date,
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
    mdf    = filter_by_account(mdf, st.session_state.ana_acct_filter)
    exp_df = mdf[mdf["Amount"] < 0].copy()

    # ── MONTH-OVER-MONTH COMPARISON CARD
    prev_m  = int(a_mn) - 1 or 12
    prev_y  = int(a_y) if int(a_mn) > 1 else int(a_y) - 1
    pms, pme = month_range(prev_y, prev_m)
    prev_df = filter_by_account(
        df[(df["Date"].dt.date >= pms) & (df["Date"].dt.date <= pme) & (df["Amount"] < 0)],
        st.session_state.ana_acct_filter
    )
    cur_tot  = exp_df["Amount"].abs().sum()
    prev_tot = prev_df["Amount"].abs().sum()
    delta    = cur_tot - prev_tot
    delta_pct = (delta / prev_tot * 100) if prev_tot > 0 else 0
    d_c   = C["expense"] if delta > 0 else C["income"]
    d_arr = "▲" if delta > 0 else "▼"
    MONTHS_SHORT = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    mc1, mc2, mc3 = st.columns(3)
    with mc1:
        st.markdown(f"""<div class="card-sm" style="text-align:center;padding:10px">
            <div style="font-size:.58rem;color:{C['muted']};font-weight:800;text-transform:uppercase;letter-spacing:.8px">{a_m} {a_y}</div>
            <div class="mono" style="font-size:1.1rem;color:{C['expense']}">{sym}{cur_tot:,.0f}</div>
        </div>""", unsafe_allow_html=True)
    with mc2:
        st.markdown(f"""<div class="card-sm" style="text-align:center;padding:10px">
            <div style="font-size:.58rem;color:{C['muted']};font-weight:800;text-transform:uppercase;letter-spacing:.8px">{MONTHS_SHORT[prev_m-1]} {prev_y}</div>
            <div class="mono" style="font-size:1.1rem;color:{C['muted']}">{sym}{prev_tot:,.0f}</div>
        </div>""", unsafe_allow_html=True)
    with mc3:
        st.markdown(f"""<div class="card-sm" style="text-align:center;padding:10px">
            <div style="font-size:.58rem;color:{C['muted']};font-weight:800;text-transform:uppercase;letter-spacing:.8px">vs Last Month</div>
            <div class="mono" style="font-size:1.1rem;color:{d_c}">{d_arr} {abs(delta_pct):.0f}%</div>
        </div>""", unsafe_allow_html=True)

    # ── TRIGGER BUDGET ALERTS (silent, non-blocking)
    try:
        tg_cfg = load_telegram_settings()
        if tg_cfg.get("bot_token") and tg_cfg.get("chat_id"):
            check_and_send_budget_alerts(df, budgets, settings, tg_cfg)
    except Exception:
        pass

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
    # ════════════════════════════════════════════════════════════════════════
    st.markdown('<div class="section-label">Email Import Rules</div>', unsafe_allow_html=True)

    # ── HOW IT WORKS banner ──────────────────────────────────────────────────
    st.markdown(f"""
    <div style="background:{C['surface2']};border-left:3px solid {C['primary']};
         border-radius:0 10px 10px 0;padding:10px 14px;margin-bottom:12px;font-size:.78rem;
         color:{C['muted']};line-height:1.8">
        <b style="color:{C['text']}">How this works:</b> Add one rule per bank/card.
        Each rule tells the app how to read that bank's email alerts.
        Code.gs runs daily and imports matching emails automatically.
        Use the <b style="color:{C['text']}">Test Parse</b> button to verify before saving.<br>
        <b style="color:{C['warning']}">Template placeholders:</b>
        <span style="font-family:'JetBrains Mono',monospace;color:{C['primary']}">&nbsp;{{amt}}</span> amount
        <span style="font-family:'JetBrains Mono',monospace;color:{C['warning']}">&nbsp;{{act}}</span> account text
        <span style="font-family:'JetBrains Mono',monospace;color:{C['info']}">&nbsp;{{tdetails}}</span> merchant
        <span style="font-family:'JetBrains Mono',monospace;color:{C['income']}">&nbsp;{{date}}</span> date
        <span style="font-family:'JetBrains Mono',monospace;color:{C['muted']}">&nbsp;{{skip}}</span> ignore
    </div>""", unsafe_allow_html=True)

    # ── RUN NOW button ───────────────────────────────────────────────────────
    rn1, rn2 = st.columns([4, 1])
    with rn1:
        st.markdown(f"<div style='font-size:.75rem;color:{C['muted']};padding:4px 0'>"
                    f"Code.gs runs daily automatically. Tap to queue a run now.</div>",
                    unsafe_allow_html=True)
    with rn2:
        if st.button("▶ Run Now", key="btn_run_now", type="primary",
                     use_container_width=True):
            try:
                trigger_run_now()
                st.success("✅ Queued! Code.gs will pick it up shortly.")
            except Exception as ex:
                st.error(f"Could not queue: {ex}")

    # ── IMPORT LOG ───────────────────────────────────────────────────────────
    with st.expander("📊  Recent Import Log", expanded=False):
        try:
            log_df = load_importlog()
            if log_df.empty:
                st.markdown(f"<div style='color:{C['muted']};font-size:.82rem;padding:8px 0'>"
                            f"No import runs yet.</div>", unsafe_allow_html=True)
            else:
                for _, lr in log_df.tail(10).iloc[::-1].iterrows():
                    imp_raw = str(lr.get("Imported","0"))
                    imp_num = ''.join(filter(str.isdigit, imp_raw)) or "0"
                    imp_c   = C["income"] if int(imp_num) > 0 else C["muted"]
                    ts      = str(lr.get("Timestamp",""))[:16]
                    skipped = str(lr.get("Skipped",""))
                    files   = str(lr.get("Files",""))
                    st.markdown(f"""
                    <div style="display:flex;justify-content:space-between;align-items:center;
                         padding:6px 2px;border-bottom:1px solid {C['border']};font-size:.75rem">
                        <div>
                            <div style="font-weight:700;color:{C['text']}">{ts}</div>
                            <div style="color:{C['muted']};font-size:.68rem">{files}</div>
                        </div>
                        <div style="text-align:right">
                            <span style="color:{imp_c};font-family:'JetBrains Mono',monospace;font-weight:700">+{imp_num}</span>
                            <span style="color:{C['muted']};font-size:.68rem;margin-left:6px">{skipped} skipped</span>
                        </div>
                    </div>""", unsafe_allow_html=True)
        except Exception:
            st.info("Import log not available yet.")

    # ── PARSE ERRORS ─────────────────────────────────────────────────────────
    if not errors_df.empty:
        with st.expander(f"⚠️  Parse Errors ({len(errors_df)})", expanded=False):
            st.markdown(f"<div style='color:{C['muted']};font-size:.78rem;margin-bottom:8px'>"
                        f"These emails matched sender/subject but body parse failed. "
                        f"Refine the template.</div>", unsafe_allow_html=True)
            for _, er in errors_df.tail(8).iloc[::-1].iterrows():
                ts      = str(er.get("Timestamp",""))[:16]
                rule_n  = str(er.get("RuleName",""))
                reason  = str(er.get("ErrorReason",""))
                snippet = str(er.get("BodySnippet",""))[:100]
                st.markdown(f"""
                <div style="background:rgba(255,79,109,.07);border:1px solid rgba(255,79,109,.25);
                     border-radius:10px;padding:10px 12px;margin:4px 0;font-size:.74rem">
                    <div style="display:flex;justify-content:space-between;margin-bottom:3px">
                        <span style="font-weight:800;color:{C['expense']}">{rule_n}</span>
                        <span style="color:{C['muted']}">{ts}</span>
                    </div>
                    <div style="color:{C['warning']};margin-bottom:3px">{reason}</div>
                    <div style="color:{C['muted']};font-family:'JetBrains Mono',monospace;
                         font-size:.63rem;word-break:break-all">{snippet}…</div>
                </div>""", unsafe_allow_html=True)
            if st.button("🗑️ Clear Parse Errors", key="clear_parse_err"):
                try:
                    ss = get_ss(); ws = ss.worksheet("ParseErrors")
                    ws.clear(); ws.append_row(HEADERS["ParseErrors"])
                    st.cache_data.clear(); st.success("✅ Cleared."); st.rerun()
                except Exception as ex:
                    st.error(str(ex))

    # ── EXISTING RULES — with Edit inline ───────────────────────────────────
    st.markdown(f'<div class="section-label">Your Rules ({len(rules_df)})</div>',
                unsafe_allow_html=True)

    if rules_df.empty:
        st.markdown(f"""
        <div class="card-sm" style="text-align:center;padding:20px;color:{C['muted']};font-size:.82rem">
            No rules yet — add your first rule below ↓
        </div>""", unsafe_allow_html=True)
    else:
        for _, rule in rules_df.iterrows():
            r_nm       = str(rule.get("RuleName",""))
            is_active  = str(rule.get("Active","TRUE")).upper() in ("TRUE","YES","1")
            is_dry     = str(rule.get("DryRun","FALSE")).upper() in ("TRUE","YES","1")
            acct_lbl   = str(rule.get("AccountLabel","")).strip()
            lookback   = str(rule.get("LookbackDays","2")).strip()
            last_run   = str(rule.get("LastRun","—")).strip()[:16]
            last_imp   = str(rule.get("LastImported","—")).strip()
            active_c   = C["income"] if is_active else C["muted"]
            is_editing = (st.session_state.edit_rule_name == r_nm)

            # ── Rule card ──────────────────────────────────────────────────
            st.markdown(f"""
            <div style="background:{C['surface2']};border:1px solid {C['border']};
                 border-radius:12px;padding:12px 14px;margin:6px 0">
                <div style="display:flex;justify-content:space-between;align-items:flex-start">
                    <div style="flex:1;min-width:0">
                        <div style="font-weight:800;font-size:.9rem">{r_nm}</div>
                        <div style="font-size:.7rem;color:{C['muted']};margin-top:2px">
                            📧 <span style="color:{C['info']}">{rule.get('Sender','')}</span>
                        </div>
                        <div style="font-size:.7rem;color:{C['muted']}">
                            Subject: <em>{rule.get('SubjectContains','')}</em>
                        </div>
                        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:5px;align-items:center">
                            {account_badge_html(acct_lbl, inline=True) if acct_lbl else ''}
                            <span style="font-size:.6rem;color:{C['muted']}">📅 {lookback}d lookback</span>
                            {f'<span style="font-size:.6rem;color:{C["warning"]}">🔬 DRY RUN</span>' if is_dry else ''}
                        </div>
                    </div>
                    <div style="text-align:right;flex-shrink:0;margin-left:10px">
                        <div style="font-size:.65rem;font-weight:800;color:{active_c}">
                            {'● ACTIVE' if is_active else '○ OFF'}
                        </div>
                        <div style="font-size:.6rem;color:{C['muted']};margin-top:3px">
                            Last: {last_run or '—'}
                        </div>
                        <div style="font-size:.6rem;color:{C['income'] if last_imp.isdigit() and int(last_imp)>0 else C['muted']}">
                            +{last_imp} imported
                        </div>
                    </div>
                </div>
                <div style="margin-top:8px;font-size:.64rem;color:{C['muted']};
                     background:{C['bg']};border-radius:7px;padding:5px 9px;
                     font-family:'JetBrains Mono',monospace;line-height:1.6;
                     word-break:break-all">{rule.get('BodyTemplate','')}</div>
            </div>""", unsafe_allow_html=True)

            # Action buttons row
            ba, bb, bc, bd = st.columns(4)
            with ba:
                tog_lbl = "⏸ Disable" if is_active else "▶ Enable"
                if st.button(tog_lbl, key=f"tog_{r_nm}", use_container_width=True):
                    _update_email_rule(r_nm, {"Active": "FALSE" if is_active else "TRUE"})
                    st.rerun()
            with bb:
                if is_dry:
                    if st.button("🚀 Go Live", key=f"dry_{r_nm}",
                                 use_container_width=True, type="primary",
                                 help="Disable Dry Run — next trigger writes real transactions"):
                        _update_email_rule(r_nm, {"DryRun": "FALSE"})
                        st.success(f"✅ {r_nm} is now LIVE!")
                        st.rerun()
                else:
                    if st.button("🔬 Dry Run", key=f"dry_{r_nm}",
                                 use_container_width=True,
                                 help="Switch back to test mode"):
                        _update_email_rule(r_nm, {"DryRun": "TRUE"})
                        st.rerun()
            with bc:
                edit_lbl = "✕ Cancel" if is_editing else "✏️ Edit"
                if st.button(edit_lbl, key=f"edit_{r_nm}", use_container_width=True):
                    st.session_state.edit_rule_name = None if is_editing else r_nm
                    st.rerun()
            with bd:
                if st.button("🗑️ Delete", key=f"del_{r_nm}", use_container_width=True):
                    _delete_email_rule(r_nm)
                    if st.session_state.edit_rule_name == r_nm:
                        st.session_state.edit_rule_name = None
                    st.rerun()

            # ── Inline edit form (expands under the rule) ──────────────────
            if is_editing:
                st.markdown(f"""
                <div style="background:{C['bg']};border:1px solid {C['primary']}44;
                     border-radius:10px;padding:12px;margin:4px 0 8px">
                    <div style="font-size:.65rem;font-weight:800;letter-spacing:1px;
                           color:{C['primary']};text-transform:uppercase;margin-bottom:10px">
                        ✏️ Editing: {r_nm}
                    </div>
                </div>""", unsafe_allow_html=True)

                with st.form(f"edit_form_{r_nm}", clear_on_submit=False):
                    e_sender  = st.text_input("Sender Email",
                                              value=str(rule.get("Sender","")),
                                              key=f"e_sender_{r_nm}")
                    e_subject = st.text_input("Subject Contains",
                                              value=str(rule.get("SubjectContains","")),
                                              key=f"e_subject_{r_nm}")
                    e_tmpl    = st.text_area("Body Template",
                                             value=str(rule.get("BodyTemplate","")),
                                             height=80, key=f"e_tmpl_{r_nm}")
                    ec1, ec2 = st.columns(2)
                    with ec1:
                        e_acct = st.text_input("Account Label",
                                               value=str(rule.get("AccountLabel","")),
                                               key=f"e_acct_{r_nm}")
                        e_look = st.number_input("Lookback Days",
                                                 value=int(rule.get("LookbackDays",2) or 2),
                                                 min_value=1, max_value=30, step=1,
                                                 key=f"e_look_{r_nm}")
                    with ec2:
                        cur_type  = str(rule.get("DefaultType","Expense"))
                        type_opts = ["Debit (Expense)","Credit (Income)"]
                        type_idx  = 1 if cur_type == "Income" else 0
                        e_type    = st.selectbox("Transaction Type", type_opts,
                                                  index=type_idx, key=f"e_type_{r_nm}")
                        e_dry     = st.toggle("🔬 Dry Run",
                                              value=is_dry, key=f"e_dry_{r_nm}")

                    if st.form_submit_button("💾 Save Changes",
                                             use_container_width=True, type="primary"):
                        _update_email_rule(r_nm, {
                            "Sender":          e_sender.strip(),
                            "SubjectContains": e_subject.strip(),
                            "BodyTemplate":    e_tmpl.strip(),
                            "AccountLabel":    e_acct.strip(),
                            "LookbackDays":    str(e_look),
                            "DefaultType":     "Expense" if "Debit" in e_type else "Income",
                            "DryRun":          "TRUE" if e_dry else "FALSE",
                        })
                        st.session_state.edit_rule_name = None
                        st.success(f"✅ Rule '{r_nm}' updated!")
                        st.rerun()

    # ── ADD NEW RULE — always visible, form clears on save ───────────────────
    st.markdown(f'<div class="section-label">Add New Rule</div>', unsafe_allow_html=True)

    # Quick-fill templates
    st.markdown(f"<div style='font-size:.72rem;color:{C['muted']};margin-bottom:6px'>"
                f"Quick-fill a template or fill manually:</div>",
                unsafe_allow_html=True)

    TEMPLATES = {
        "— blank —": ("","","","","HDFC CC / SBI CC / Paytm UPI"),
        "HDFC Credit Card": (
            "alerts@hdfcbank.bank.in",
            "debited via Credit Card",
            "Rs.{amt} is debited from your {act} towards {tdetails} on {date}",
            "Expense",
            "HDFC CC 7500",
        ),
        "SBI Credit Card": (
            "onlinesbicard@sbicard.com",
            "Transaction Alert from SBI Card",
            "Rs.{amt} spent on your {skip} {act} at {tdetails} on {date}.",
            "Expense",
            "SBI CC 4996",
        ),
        "ICICI Credit Card": (
            "alerts@icicibank.com",
            "ICICI Bank Credit Card",
            "Rs.{amt} has been debited from your {act} at {tdetails} on {date}",
            "Expense",
            "ICICI CC",
        ),
        "Axis Credit Card": (
            "efulfillment@axisbank.com",
            "Axis Bank Credit Card",
            "Rs.{amt} has been charged to your {act} at {tdetails} on {date}",
            "Expense",
            "Axis CC",
        ),
    }

    tpl_cols = st.columns(len(TEMPLATES))
    for i, (tpl_name, _) in enumerate(TEMPLATES.items()):
        with tpl_cols[i]:
            lbl = "Blank" if "blank" in tpl_name else tpl_name.split()[0]
            is_sel = st.session_state.get("nr_template_pick","— blank —") == tpl_name
            st.markdown(f'<div class="{"pill-on" if is_sel else "pill-off"}">', unsafe_allow_html=True)
            if st.button(lbl, key=f"tpl_{i}"):
                st.session_state.nr_template_pick = tpl_name
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    # Fill defaults from selected template
    tpl_key  = st.session_state.get("nr_template_pick","— blank —")
    tpl_vals = TEMPLATES.get(tpl_key, TEMPLATES["— blank —"])

    with st.form("add_rule_form", clear_on_submit=True):
        nr_name    = st.text_input("Rule Name *",
                                    placeholder="e.g. HDFC Credit Card  ·  SBI CC  ·  Kotak Debit",
                                    key="nr_name_f")
        nr_sender  = st.text_input("Sender Email *",
                                    value=tpl_vals[0],
                                    placeholder="alerts@hdfcbank.bank.in",
                                    key="nr_sender_f")
        nr_subject = st.text_input("Subject Contains",
                                    value=tpl_vals[1],
                                    placeholder="debited via Credit Card",
                                    key="nr_subject_f")
        nr_tmpl    = st.text_area("Body Template *",
                                   value=tpl_vals[2],
                                   placeholder="Rs.{amt} is debited from your {act} towards {tdetails} on {date}",
                                   height=75, key="nr_tmpl_f")

        fa, fb = st.columns(2)
        with fa:
            nr_acct = st.text_input("Account Label *",
                                     value=tpl_vals[4],
                                     placeholder="HDFC CC 7500",
                                     key="nr_acct_f",
                                     help="Stored in Tags column — used in all account filters")
            nr_look = st.number_input("Lookback Days", value=2,
                                       min_value=1, max_value=30, step=1,
                                       key="nr_look_f")
        with fb:
            nr_type = st.selectbox("Transaction Type",
                                    ["Debit (Expense)","Credit (Income)"],
                                    index=0 if tpl_vals[3]=="Expense" else 1,
                                    key="nr_type_f")
            nr_dry  = st.toggle("🔬 Start in Dry Run mode",
                                 value=True, key="nr_dry_f",
                                 help="Recommended — Code.gs parses but doesn't write until you turn this off")

        # ── Test Parse inside form ─────────────────────────────────────────
        st.markdown(f"<div style='color:{C['muted']};font-size:.73rem;margin:8px 0 3px'>"
                    f"Paste a sample email body to test your template before saving:</div>",
                    unsafe_allow_html=True)
        nr_test_body = st.text_area("Sample email body", key="nr_test_f",
                                     height=70, label_visibility="collapsed",
                                     placeholder="Paste the full email notification text here…")

        fb1, fb2 = st.columns(2)
        with fb1:
            test_clicked = st.form_submit_button("🔍 Test Parse", use_container_width=True)
        with fb2:
            save_clicked = st.form_submit_button("💾 Save Rule", use_container_width=True,
                                                  type="primary")

        if test_clicked:
            if nr_tmpl.strip() and nr_test_body.strip():
                result = parse_email_body(nr_tmpl.strip(), nr_test_body.strip())
                st.session_state.email_parse_result = result
                st.session_state._test_tmpl_snap = nr_tmpl.strip()
            else:
                st.warning("Enter both Body Template and sample email body to test.")

        if save_clicked:
            if nr_name.strip() and nr_sender.strip() and nr_tmpl.strip() and nr_acct.strip():
                existing_names = rules_df["RuleName"].tolist() if not rules_df.empty else []
                if nr_name.strip() in existing_names:
                    st.error(f"A rule named '{nr_name.strip()}' already exists. Use a different name.")
                else:
                    _write_email_rule({
                        "RuleName":        nr_name.strip(),
                        "Sender":          nr_sender.strip(),
                        "SubjectContains": nr_subject.strip(),
                        "BodyTemplate":    nr_tmpl.strip(),
                        "DateFormat":      "",
                        "DefaultType":     "Expense" if "Debit" in nr_type else "Income",
                        "AccountLabel":    nr_acct.strip(),
                        "Active":          "TRUE",
                        "DryRun":          "TRUE" if nr_dry else "FALSE",
                        "LookbackDays":    str(nr_look),
                        "LastRun":         "",
                        "LastImported":    "",
                    })
                    st.session_state.email_parse_result = None
                    st.session_state.nr_template_pick   = "— blank —"
                    st.success(f"✅ Rule '{nr_name.strip()}' saved! You can now add another rule.")
                    st.rerun()
            else:
                st.error("Rule Name, Sender Email, Body Template, and Account Label are all required.")

    # ── Test parse result (shown outside form so it persists) ─────────────
    epr = st.session_state.get("email_parse_result")
    if epr is not None:
        if epr:
            amt_v  = clean_amount(epr.get("amt",""))
            td_v   = epr.get("tdetails","—")
            act_v  = epr.get("act","—")
            dt_v   = epr.get("date","—")
            sym_s  = settings.get("currency_symbol","₹")
            st.markdown(f"""
            <div style="background:rgba(0,200,150,.08);border:1px solid rgba(0,200,150,.3);
                 border-radius:12px;padding:12px 14px;margin:8px 0">
                <div style="font-size:.63rem;font-weight:800;letter-spacing:1px;
                       color:{C['income']};text-transform:uppercase;margin-bottom:8px">✅ Parse successful</div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:.78rem">
                    <div><span style="color:{C['muted']}">Amount</span><br>
                         <span style="font-family:'JetBrains Mono',monospace;color:{C['expense']};font-weight:700">
                         {sym_s}{f"{amt_v:,.2f}" if amt_v else "—"}</span></div>
                    <div><span style="color:{C['muted']}">Merchant</span><br>
                         <span style="font-weight:700">{td_v}</span></div>
                    <div><span style="color:{C['muted']}">Account (raw)</span><br>
                         <span style="font-weight:600">{act_v}</span></div>
                    <div><span style="color:{C['muted']}">Date (raw)</span><br>
                         <span style="font-weight:600">{dt_v}</span></div>
                </div>
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="background:rgba(255,79,109,.08);border:1px solid rgba(255,79,109,.3);
                 border-radius:12px;padding:12px;margin:8px 0;font-size:.82rem;color:{C['expense']}">
                ❌ No match. Check the template text matches the email exactly.
            </div>""", unsafe_allow_html=True)
        if st.button("✕ Clear result", key="clr_parse"):
            st.session_state.email_parse_result = None
            st.rerun()

    # ── DATE AUDIT & FIX ────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Date Audit & Fix</div>', unsafe_allow_html=True)
    with st.expander("📅  Scan & Fix Existing Dates", expanded=False):
        st.markdown(
            f"<div style='color:{C['muted']};font-size:.8rem;margin-bottom:10px'>"
            f"Scans all transactions for incorrectly stored dates. "
            f"Preferred format is <b style='color:{C['text']}'>MM/DD/YYYY</b>. "
            f"Fixes ISO dates (YYYY-MM-DD), 2-digit years, and obviously swapped "
            f"month/day values (where month &gt; 12).</div>",
            unsafe_allow_html=True)

        if st.button("🔍 Run Date Scan", key="run_date_scan", use_container_width=True):
            df_scan = load_transactions()
            if df_scan.empty:
                st.info("No transactions to scan.")
            else:
                report = _detect_date_issues(df_scan)
                st.session_state["date_scan_report"] = report
                st.rerun()

        rpt = st.session_state.get("date_scan_report")
        if rpt:
            total = rpt["total"]
            iso_c = len(rpt["iso"])
            sy_c  = len(rpt["short_year"])
            sus_c = len(rpt["suspicious"])
            nat_c = len(rpt["nat"])
            ok_c  = len(rpt["ok"])
            needs_fix = iso_c + sy_c + sus_c

            # Summary grid
            st.markdown(f"""
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin:10px 0">
                <div class="card-sm" style="text-align:center;padding:10px">
                    <div style="font-size:.6rem;color:{C['muted']};font-weight:800;text-transform:uppercase;letter-spacing:.8px">✅ Correct</div>
                    <div class="mono" style="font-size:1.3rem;color:{C['income']}">{ok_c}</div>
                    <div style="font-size:.62rem;color:{C['muted']}">MM/DD/YYYY</div>
                </div>
                <div class="card-sm" style="text-align:center;padding:10px">
                    <div style="font-size:.6rem;color:{C['muted']};font-weight:800;text-transform:uppercase;letter-spacing:.8px">⚠️ Need Fix</div>
                    <div class="mono" style="font-size:1.3rem;color:{C['warning'] if needs_fix>0 else C['muted']}">{needs_fix}</div>
                    <div style="font-size:.62rem;color:{C['muted']}">rows affected</div>
                </div>
                <div class="card-sm" style="text-align:center;padding:10px">
                    <div style="font-size:.6rem;color:{C['muted']};font-weight:800;text-transform:uppercase;letter-spacing:.8px">❌ Unparseable</div>
                    <div class="mono" style="font-size:1.3rem;color:{C['expense'] if nat_c>0 else C['muted']}">{nat_c}</div>
                    <div style="font-size:.62rem;color:{C['muted']}">blank/invalid</div>
                </div>
            </div>""", unsafe_allow_html=True)

            # Breakdown
            breakdown = []
            if iso_c  > 0: breakdown.append(f"**{iso_c}** stored as YYYY-MM-DD (ISO) → will convert to MM/DD/YYYY")
            if sy_c   > 0: breakdown.append(f"**{sy_c}** stored with 2-digit year (MM/DD/YY) → will expand to 4-digit")
            if sus_c  > 0: breakdown.append(f"**{sus_c}** have month > 12 — clearly day/month swapped → will correct")
            if nat_c  > 0: breakdown.append(f"**{nat_c}** are blank or completely unparseable — will be left unchanged")
            if ok_c   > 0: breakdown.append(f"**{ok_c}** are already correct MM/DD/YYYY — untouched")

            for line in breakdown:
                st.markdown(f"<div style='font-size:.8rem;padding:2px 4px'>• {line}</div>",
                            unsafe_allow_html=True)

            if needs_fix == 0 and nat_c == 0:
                st.success("✅ All dates are already in MM/DD/YYYY format. Nothing to fix!")
            else:
                st.markdown(f"""
                <div style="background:rgba(240,165,0,.08);border:1px solid rgba(240,165,0,.3);
                     border-radius:10px;padding:10px 12px;margin:10px 0;font-size:.78rem;
                     color:{C['warning']}">
                    ⚠️  This will update <b>{needs_fix} rows</b> in your Google Sheet directly.
                    It cannot be undone. Make sure you have exported a backup CSV first.
                </div>""", unsafe_allow_html=True)

                cf1, cf2 = st.columns(2)
                with cf1:
                    if st.button(f"🔧 Fix {needs_fix} Dates", key="fix_dates_btn",
                                 type="primary", use_container_width=True):
                        df_fix  = load_transactions()
                        ss      = get_ss()
                        ws      = ss.worksheet("Transactions")
                        all_vals = ws.get_all_values()
                        hdrs     = all_vals[0]
                        date_col_idx = hdrs.index("Date") + 1  # 1-based
                        id_col_idx   = hdrs.index("RowID") + 1

                        # Build RowID → sheet row map
                        row_map = {}
                        for i, row in enumerate(all_vals[1:], start=2):
                            if row:
                                row_map[row[id_col_idx - 1]] = i

                        # IDs that need fixing
                        to_fix_ids = set(rpt["iso"] + rpt["short_year"] + rpt["suspicious"])
                        fix_count  = 0
                        errors     = 0

                        progress = st.progress(0, text="Fixing dates…")
                        total_to_fix = len(to_fix_ids)

                        for fi, (_, row) in enumerate(df_fix.iterrows()):
                            rid = str(row.get("RowID",""))
                            if rid not in to_fix_ids:
                                continue
                            raw_date = str(row.get("Date",""))
                            new_date = _normalise_date_str(raw_date)
                            if new_date != raw_date and rid in row_map:
                                try:
                                    ws.update_cell(row_map[rid], date_col_idx, new_date)
                                    fix_count += 1
                                except Exception:
                                    errors += 1
                            progress.progress(
                                min((fi+1)/max(total_to_fix,1), 1.0),
                                text=f"Fixed {fix_count}…"
                            )

                        st.cache_data.clear()
                        st.session_state["date_scan_report"] = None
                        if errors == 0:
                            st.success(f"✅ Fixed {fix_count} dates. All now in MM/DD/YYYY format.")
                        else:
                            st.warning(f"Fixed {fix_count} dates. {errors} could not be updated (Sheets API limit — re-run to retry).")
                        st.rerun()
                with cf2:
                    if st.button("✕ Clear Scan", key="clear_scan",
                                 use_container_width=True):
                        st.session_state["date_scan_report"] = None
                        st.rerun()

    
    # ════════════════════════════════════════════════════════════════════════
    #  V4: TELEGRAM ALERTS
    # ════════════════════════════════════════════════════════════════════════
    st.markdown('<div class="section-label">Telegram Alerts</div>', unsafe_allow_html=True)
    with st.expander("📱  Budget Alerts via Telegram", expanded=False):
        tg_cfg = load_telegram_settings()
        st.markdown(f"""
        <div style="background:{C['surface2']};border-left:3px solid {C['info']};
             border-radius:0 10px 10px 0;padding:10px 14px;margin-bottom:12px;
             font-size:.78rem;color:{C['muted']};line-height:1.7">
            <b style="color:{C['text']}">Setup:</b> Create a bot via
            <span style="color:{C['info']}">@BotFather</span> on Telegram,
            get the Bot Token. Then message the bot and get your Chat ID from
            <span style="color:{C['info']}">@userinfobot</span>.<br>
            Alerts fire when any category exceeds the threshold % of its monthly budget.
        </div>""", unsafe_allow_html=True)

        tg_token   = st.text_input("Bot Token",   value=tg_cfg.get("bot_token",""),
                                    placeholder="123456:ABC-DEF...", key="tg_token",
                                    type="password")
        tg_chat_id = st.text_input("Chat ID",      value=tg_cfg.get("chat_id",""),
                                    placeholder="Your numeric chat ID", key="tg_chat")
        tg_pct     = st.slider("Alert when budget hits (%)",
                                min_value=50, max_value=100, step=5,
                                value=int(tg_cfg.get("alert_pct","80")),
                                key="tg_pct")

        tc1, tc2 = st.columns(2)
        with tc1:
            if st.button("💾 Save", key="tg_save", type="primary",
                         use_container_width=True):
                if tg_token.strip() and tg_chat_id.strip():
                    save_telegram_setting("bot_token",  tg_token.strip())
                    save_telegram_setting("chat_id",    tg_chat_id.strip())
                    save_telegram_setting("alert_pct",  str(tg_pct))
                    st.success("✅ Telegram settings saved!")
                else:
                    st.error("Enter both Bot Token and Chat ID.")
        with tc2:
            if st.button("🔔 Test", key="tg_test", use_container_width=True):
                if tg_token.strip() and tg_chat_id.strip():
                    ok, err = send_telegram(tg_token.strip(), tg_chat_id.strip(),
                                            "✅ <b>ClearSpend test</b> — Telegram alerts are working!")
                    st.session_state.tg_test_result = (ok, err)
                    st.rerun()
                else:
                    st.error("Save settings first.")

        if st.session_state.tg_test_result is not None:
            ok, err = st.session_state.tg_test_result
            if ok:
                st.success("✅ Test message delivered!")
            else:
                st.error(f"❌ Failed: {err}")
            if st.button("✕ Dismiss", key="tg_dismiss"):
                st.session_state.tg_test_result = None; st.rerun()

    # ════════════════════════════════════════════════════════════════════════
    #  V4: MERCHANT ALIASES
    # ════════════════════════════════════════════════════════════════════════
    st.markdown('<div class="section-label">Merchant Aliases</div>', unsafe_allow_html=True)
    with st.expander("🏷️  Normalise Merchant Names", expanded=False):
        st.markdown(f"""
        <div style="font-size:.78rem;color:{C['muted']};margin-bottom:10px;line-height:1.6">
            Map messy imported names to a clean canonical name.
            e.g. "PAY*Hindustan Petroleu" → "Hindustan Petroleum".<br>
            Code.gs applies these during import. Existing transactions are
            not automatically renamed — use the Edit dialog for those.
        </div>""", unsafe_allow_html=True)

        aliases = load_merchant_aliases()
        if aliases:
            q_alias = st.text_input("🔍 Search", key="alias_search_box",
                                     placeholder="Filter by raw name…",
                                     label_visibility="collapsed")
            filtered_aliases = {k: v for k, v in aliases.items()
                                 if not q_alias or q_alias.lower() in k}
            for raw, canonical in list(filtered_aliases.items())[:30]:
                ac1, ac2, ac3 = st.columns([4, 4, 1])
                with ac1:
                    st.markdown(f"<div style='font-size:.78rem;color:{C['muted']};padding:6px 4px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap'>{raw[:30]}</div>", unsafe_allow_html=True)
                with ac2:
                    st.markdown(f"<div style='font-size:.78rem;font-weight:700;color:{C['text']};padding:6px 4px'>{canonical}</div>", unsafe_allow_html=True)
                with ac3:
                    if st.button("✕", key=f"del_alias_{raw[:20]}"):
                        delete_merchant_alias(raw); st.rerun()
        else:
            st.markdown(f"<div style='color:{C['muted']};font-size:.82rem;padding:8px 0'>No aliases yet.</div>", unsafe_allow_html=True)

        st.markdown(f"<div style='font-size:.7rem;font-weight:800;color:{C['muted']};margin:12px 0 4px;text-transform:uppercase;letter-spacing:1px'>Add Alias</div>", unsafe_allow_html=True)
        with st.form("add_alias_form", clear_on_submit=True):
            al1, al2 = st.columns(2)
            with al1:
                raw_in = st.text_input("Raw name (from import)",
                                        placeholder="PAY*Hindustan Petroleu")
            with al2:
                can_in = st.text_input("Canonical name",
                                        placeholder="Hindustan Petroleum")
            if st.form_submit_button("➕ Add Alias", use_container_width=True,
                                      type="primary"):
                if raw_in.strip() and can_in.strip():
                    save_merchant_alias(raw_in.strip(), can_in.strip())
                    st.success(f"✅ Alias saved: {raw_in} → {can_in}")
                    st.rerun()
                else:
                    st.error("Enter both names.")

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
    
    if nav == "home":
        screen_home()
    elif nav == "transactions":
        screen_transactions()
    elif nav == "add":
        screen_add()
    elif nav == "analytics":
        screen_analytics()
    elif nav == "settings":
        screen_settings()

if __name__ == "__main__":
    main()
