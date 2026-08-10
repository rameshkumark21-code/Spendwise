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
    page_title="ClearSpend Engine",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── MATERIAL 3 DESIGN TOKENS ──────────────────────────────────────────────────
C = {
    "bg":            "#121316",
    "surface":       "#1e1f23",
    "surface2":      "#282a2f",
    "border":        "#33373e",
    "primary":       "#7c6df8",
    "primary_dim":   "rgba(124,109,248,0.2)",
    "income":        "#2e7d32",
    "income_light":  "#a5d6a7",
    "expense":       "#e53935",
    "expense_light": "#ef9a9a",
    "warning":       "#f57c00",
    "info":          "#0288d1",
    "text":          "#f4f5f7",
    "muted":         "#9e9e9e",
    "pill_bg":       "#2d2f36",
}

# ── GOOGLE SHEETS CONSTANTS ────────────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SPREADSHEET_ID = "11dt8IdIjpiS3UCuAEDmLi9ETm5u4rONZpOlnLBK065w"
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
    ["Bills & Utilities", "Credit Card Payment", "sbi card,sbi cards,icici bank credit card,hdfc credit,axis credit,kotak credit,credit card payment", "💳"],
    ["Bills & Utilities", "Electricity", "bescom,tangedco,bangalore electricity supply,bangalore electricity,electricity,eb bill,tneb,wbsedcl,msedcl,adani electricity,tata power", "⚡"],
    ["Bills & Utilities", "LPG & Gas", "trupti enterprises,sahaya pragash kumar,lpg,cylinder,indane,bharat gas,mahanagar gas,gas", "🔥"],
    ["Bills & Utilities", "Mobile & Recharge", "recharge of airtel,recharge of bsnl,airtel mobile,bsnl mobile,vodafone idea,airtel money,airtel,bsnl,vodafone,vi vodafone,jio,act fibernet,hathway,recharge,broadband,postpaid,mobile", "📡"],
    ["Bills & Utilities", "OTT & Subscriptions", "automatic payment for jiohotstar,automatic payment for netflix,automatic payment of,jiohotstar,netflix,zee5,airtelxstream,hotstar,sonyliv,prime video,spotify,youtube premium,apple music,adobe,notion,disney", "📺"],
    ["Entertainment", "Movies & Events", "theatre sri guru,nexus shantiniketan,garuda mall,pvr,inox,bookmyshow,cinepolis,movie,theatre", "🎬"],
    ["Entertainment", "Outings & Activities", "namma bengaluru aquarium,the royal park,kiosk 2 mayura,balbhavan,bal bhavan,aquarium,amusement,theme park", "🎪"],
    ["Entertainment", "Spiritual & Temples", "tirupathi tirumala devasthanams,tmsm krpm,tirumala,tirupathi,devasthanam,devasthanams,temple,church,mosque", "🛕"],
    ["Food & Dining", "Groceries", "zeptonow,zepto,blinkit,bigbasket,grofers,dmart,jiomart,eco hypermarket,vasantham,vasantham super market,m s vasantham,kpn farm fresh,hap daily,sri bhuvaneswary rice traders,marudhar mart,family choice,amul,supermarket,hypermarket,farm fresh,grocery,rice traders", "🛒"],
    ["Food & Dining", "Restaurants & Mess", "hungerbox,udupi kitchen,udupi gokula,shrayanka foods,sendhoor coffee,salted chilli restaurant,sai akshiya bhavan,sri acharya bhavan,shree lakshmi bhavan,daalchini,chai biskut,box bites cafe,basha bhai biryani,avenue food plaza,arv donne biryani,andhra aatithyam,adyar ananda bhavan,aasai aasai,a1 tandoori,alankar cafe,alagar mess,amman coffee,bhavana s,b2b biriyani,gopizza,guntur vari amma,guntur andhra mess,hotel nellore ruchul,hotel shri lakshmi,hyderabadi biryani adda,ippopay merchant,kps restaurant,lulus bakery,madurai bun parotta,mr subburaj,restaurant,mess,bhavan,biryani,biriyani,parotta,tiffin,cafe,dhaba,hotel nellore,hotel shri", "🍽️"],
    ["Food & Dining", "Snacks & Sweets", "zam zam sweets,triveni vada pav,teaman,t t tea stall,suketha shetty,sri durga bakery,southern foods,shivani chats and sweets,sattur snacks,rathina madhapan,reddemma k,paban kundu,nuts n chocos,nrk sweets bakery,moideen kunhi,madhappan marappan,kuchen helado,kanti sweets,instant retail india,hasanamba iyengar bakery,gopal krishna shetty,fresh juice house,devishree juice,adavan bakery,a sweets and snacks,a m tasty bakery,bakery,sweets,snacks,juice,tea stall,vada pav", "🍬"],
    ["Food & Dining", "Vegetables & Meat", "sri vinayaga vegetables,ms sri vinayaka vegitables,navyashree vegetable suppliers,my chicken and more,sagar fish and chicken,bismila chicken center,vegetables,vegetable,chicken,fish and chicken,meat,fish", "🥩"],
    ["Health", "Hospital & Clinic", "aristo speciality hospital,sri manjunatha hospital,tirumala orthopaedic,m s sanjivani child care,ms sri meenakshi diagnostic,ms santhi s s sankarnarayanan,sankaranarayanan karuppaiya,narayanaswamy k,s narayanaswamy,sakumalla satyanarayana,chebrolu lakshminarayana,gulab babu,hospital,clinic,doctor,diagnostics,blood test,lab,fortis,manipal,narayana,apollo hospital", "🏥"],
    ["Health", "Pharmacy", "apollo pharmacy,16428 apollo pharmacy,16012 apollo pharmacy,8 meds pharmacy,m s sanjivani pharma,sanjivani pharma,sulochana medicals,vijaya medicals,pavan medicals,ramdev medical,orsun pharmacy,pradhan mantri bhartiya janaushadhi kendra,wellnessmedicals,janaushadhi,medplus,1mg,pharmeasy,netmeds,pharmacy,medicals,medicine", "💊"],
    ["Personal Care", "Photography", "sen studio,studio,photography", "📸"],
    ["Personal Care", "Salon & Grooming", "dugdha parlour,salon,spa,haircut,beauty,grooming,nails,parlour,jawed habib", "💇"],
    ["Shopping", "Clothing", "the chennai silks,rainbow kids,pinkz,lifestyle,westside,pantaloons,max fashion,clothing,apparel", "👗"],
    ["Shopping", "Online", "amazon india,amazon,flipkart,meesho,myntra,ajio,nykaa,snapdeal,tata cliq", "📦"],
    ["Shopping", "Retail & Stores", "vishal mega mart,sri vijayalakshmi stores,rps electricals,it digital store,surya fancey gift senter,thaim zone,croma,vijay sales", "🏪"],
    ["Transport", "Cab & Ride", "rapido,roppen transportation,dikson k,ola,uber,namma yatri,indrive,cab,taxi", "🚕"],
    ["Transport", "Fuel", "jefema fuel mart,padmashree fuels,vetri fuels,oshan energy,le konn energy stations,kavya petro,j k enterprises old madras,s v m fuel station,muthu filling station,sri parvathy filling station,filling station,fuel mart,fuel station,petrol pump,petrol,diesel,cng,iocl,bpcl,hp petrol,indian oil,shell,bharat petroleum,vriddhi fuels", "⛽"],
    ["Transport", "Metro & Bus", "bengaluru metro qr,bmtc bus,tamilnadu state transport,tamil nadu state transport,bengaluru metro,metro qr,bmtc,ksrtc,msrtc,bus,metro", "🚌"],
    ["Transport", "Tours & Travel Agent", "madhulika tours and travels,giripugal travels,tours and travels", "🗺️"],
    ["Transport", "Train", "irctc_app_upi,irctc connect app,irctc mpp,irctc cf,irctc app upi,irctc,indian railways uts,indian railways", "🚂"],
    ["Travel", "Hotels & Stays", "tvl jay priya residency,syed tourist home,sri sai hotels,sri ganesh residency,hotel gowri,hotel aadhithya,ganesh residency,jay priya residency,residency,tourist home,lodge,oyo,treebo,airbnb,hotel", "🏨"],
    ["Education", "Courses", "udemy,coursera,unacademy,vedantu,byjus,course,class,workshop,training,skillshare", "📚"],
    ["Investments", "Mutual Funds & SIP", "zerodha,groww,kuvera,sip,mutual fund,etf,paytm money,angel,coin by zerodha", "📈"],
    ["Investments", "Deposits", "fd,ppf,nsc,recurring deposit,fixed deposit,post office savings", "🏦"],
    ["Gifts & Social", "Gifts", "gift,present,birthday,anniversary,wedding", "🎁"],
    ["Gifts & Social", "Donations", "donation,charity,ngo,pm relief", "🤲"],
    ["Rent & Housing", "Rent", "rent,pg,hostel,society maintenance,house rent,landlord", "🏠"],
    ["Others", "Miscellaneous", "", "📌"],
]

DEFAULT_SETTINGS = [
    ["currency_symbol", "₹"],
    ["currency_code",   "INR"],
    ["monthly_budget",  "30000"],
    ["app_name",        "ClearSpend"],
]

DEFAULT_EMAIL_RULES = [
    ["HDFC Credit Card", "alerts@hdfcbank.bank.in", "debited via Credit Card", "Rs.{amt} is debited from your {act} towards {tdetails} on {date}", "use_email_date", "Expense", "HDFC CC 7500", "TRUE", "TRUE", "2", "", ""],
    ["SBI Credit Card", "onlinesbicard@sbicard.com", "Transaction Alert from SBI Card", "Rs.{amt} spent on your {skip} {act} at {tdetails} on {date}.", "use_email_date", "Expense", "SBI CC 4996", "TRUE", "TRUE", "2", "", ""],
]

# ═══════════════════════════════════════════════════════════════════════════════
#  GOOGLE SHEETS LAYER & RETRY SAFETY WRAPPER
# ═══════════════════════════════════════════════════════════════════════════════

def retry_gspread(func, retries=3, delay=1.5):
    for attempt in range(retries):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            if attempt == retries - 1:
                raise e
            time.sleep(delay * (2 ** attempt))
        except Exception as e:
            if attempt == retries - 1:
                raise e
            time.sleep(delay)

@st.cache_resource
def get_client():
    creds_dict = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

@st.cache_resource
def get_ss():
    client = get_client()
    try:
        return client.open_by_key(SPREADSHEET_ID)
    except Exception:
        try:
            return client.open(SPREADSHEET_NAME)
        except gspread.SpreadsheetNotFound:
            return client.create(SPREADSHEET_NAME)

def _ensure_columns(ws, required_headers: list):
    try:
        existing = ws.row_values(1)
        for h in required_headers:
            if h not in existing:
                col = len(existing) + 1
                ws.update_cell(1, col, h)
                existing.append(h)
    except Exception: pass

def ensure_sheets():
    ss = get_ss()
    try:
        existing = [ws.title for ws in ss.worksheets()]
    except Exception:
        existing = []
    
    for name, hdrs in HEADERS.items():
        if name not in existing:
            try:
                rows = 500 if name in ("EmailRules","ParseErrors") else 2000
                ws = ss.add_worksheet(title=name, rows=rows, cols=len(hdrs))
                ws.append_row(hdrs)
            except Exception: pass
        else:
            try:
                _ensure_columns(ss.worksheet(name), hdrs)
            except Exception: pass
            
    try:
        cats = ss.worksheet("Categories")
        if len(cats.get_all_values()) <= 1:
            cats.append_rows(DEFAULT_CATEGORIES)
    except Exception: pass

    try:
        setts = ss.worksheet("Settings")
        if len(setts.get_all_values()) <= 1:
            setts.append_rows(DEFAULT_SETTINGS)
    except Exception: pass

    try:
        email_ws = ss.worksheet("EmailRules")
        if len(email_ws.get_all_values()) <= 1 and DEFAULT_EMAIL_RULES:
            email_ws.append_rows(DEFAULT_EMAIL_RULES)
    except Exception: pass

# ── CRUD ───────────────────────────────────────────────────────────────────────

def _parse_dates(series):
    DMY  = re.compile(r'^(\d{1,2})/(\d{1,2})/(\d{4})$')
    ISO  = re.compile(r'^(\d{4})-(\d{2})-(\d{2})$')
    DMY2 = re.compile(r'^(\d{1,2})-(\d{1,2})-(\d{4})$')

    def parse_one(v):
        s = str(v).strip()
        if not s or s in ("nan","None","NaT",""):
            return pd.NaT
        m = DMY.match(s)
        if m:
            try: return pd.Timestamp(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except: pass
        m = ISO.match(s)
        if m:
            try: return pd.Timestamp(s)
            except: pass
        m = DMY2.match(s)
        if m:
            try: return pd.Timestamp(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except: pass
        try: return pd.Timestamp(pd.to_datetime(s, dayfirst=True, errors="coerce"))
        except: return pd.NaT

    return series.apply(parse_one)

def _normalise_date_str(s):
    if pd.isna(s): return ""
    s2 = str(s).strip()
    if not s2: return ""
    if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', s2): return s2
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s2):
        try: return pd.to_datetime(s2).strftime("%d/%m/%Y")
        except: return s2
    try: return pd.to_datetime(s2, dayfirst=True, errors="raise").strftime("%d/%m/%Y")
    except: return ""

def _detect_date_issues(df: pd.DataFrame) -> dict:
    results = {"total": len(df), "iso": [], "short_year": [], "nat": [], "ok": [], "suspicious": []}
    for _, row in df.iterrows():
        rid  = str(row.get("RowID",""))
        dval = str(row.get("Date","")).strip()
        if not dval or dval in ("nan","None","NaT",""):
            results["nat"].append(rid); continue
        if re.match(r'^\d{4}-\d{2}-\d{2}', dval):
            results["iso"].append(rid); continue
        m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{2})$', dval)
        if m: results["short_year"].append(rid); continue
        m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', dval)
        if m:
            d, mo = int(m.group(1)), int(m.group(2))
            if mo > 12: results["suspicious"].append(rid)
            else: results["ok"].append(rid)
            continue
        results["nat"].append(rid)
    return results

@st.cache_data(ttl=120)
def _load_transactions():
    try:
        data = retry_gspread(lambda: get_ss().worksheet("Transactions").get_all_records())
        if not data:
            return pd.DataFrame(columns=HEADERS["Transactions"])
        df = pd.DataFrame(data)
        df["Date"]   = _parse_dates(df["Date"])
        df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
        return df
    except Exception:
        return pd.DataFrame(columns=HEADERS["Transactions"])

@st.cache_data(ttl=60)
def load_importlog():
    try:
        data = retry_gspread(lambda: get_ss().worksheet("ImportLog").get_all_records())
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception: return pd.DataFrame()

@st.cache_data(ttl=300)
def load_categories():
    try:
        data = retry_gspread(lambda: get_ss().worksheet("Categories").get_all_records())
        return pd.DataFrame(data) if data else pd.DataFrame(DEFAULT_CATEGORIES, columns=HEADERS["Categories"])
    except Exception:
        return pd.DataFrame(DEFAULT_CATEGORIES, columns=HEADERS["Categories"])

@st.cache_data(ttl=180)
def load_cat_freq():
    try:
        df = _load_transactions()
        cats_df = load_categories()
        if df.empty:
            cats_sorted = sorted(cats_df["Category"].dropna().unique().tolist())
            sub_map = {cat: cats_df[cats_df["Category"]==cat]["Subcategory"].dropna().tolist() for cat in cats_sorted}
            return cats_sorted, sub_map
        cat_counts = df[df["Category"].notna() & (df["Category"] != "")]["Category"].value_counts()
        all_cats = set(cats_df["Category"].dropna().unique().tolist())
        extra_cats = sorted(all_cats - set(cat_counts.index.tolist()))
        cats_sorted = cat_counts.index.tolist() + extra_cats
        sub_map = {}
        for cat in cats_sorted:
            sub_counts = df[df["Category"] == cat]["Subcategory"].value_counts().index.tolist()
            subs_from_cats = cats_df[cats_df["Category"]==cat]["Subcategory"].dropna().unique().tolist()
            sub_map[cat] = sub_counts + [s for s in subs_from_cats if s not in sub_counts]
        return cats_sorted, sub_map
    except Exception:
        cats_df = pd.DataFrame(DEFAULT_CATEGORIES, columns=HEADERS["Categories"])
        cats_sorted = sorted(cats_df["Category"].unique().tolist())
        sub_map = {cat: cats_df[cats_df["Category"]==cat]["Subcategory"].tolist() for cat in cats_sorted}
        return cats_sorted, sub_map

@st.cache_data(ttl=300)
def load_budgets():
    try:
        data = retry_gspread(lambda: get_ss().worksheet("Budgets").get_all_records())
        return pd.DataFrame(data) if data else pd.DataFrame(columns=HEADERS["Budgets"])
    except Exception: return pd.DataFrame(columns=HEADERS["Budgets"])

@st.cache_data(ttl=300)
def load_settings():
    try:
        data = retry_gspread(lambda: get_ss().worksheet("Settings").get_all_records())
        return {r["Key"]: r["Value"] for r in data} if data else {k: v for k, v in DEFAULT_SETTINGS}
    except Exception: return {k: v for k, v in DEFAULT_SETTINGS}

@st.cache_data(ttl=300)
def load_email_rules():
    try:
        data = retry_gspread(lambda: get_ss().worksheet("EmailRules").get_all_records())
        return pd.DataFrame(data) if data else pd.DataFrame(columns=HEADERS["EmailRules"])
    except Exception: return pd.DataFrame(columns=HEADERS["EmailRules"])

@st.cache_data(ttl=60)
def load_parse_errors():
    try:
        data = retry_gspread(lambda: get_ss().worksheet("ParseErrors").get_all_records())
        return pd.DataFrame(data) if data else pd.DataFrame(columns=HEADERS["ParseErrors"])
    except Exception: return pd.DataFrame(columns=HEADERS["ParseErrors"])

@st.cache_data(ttl=300)
def load_merchant_aliases() -> dict:
    try:
        data = retry_gspread(lambda: get_ss().worksheet("MerchantAliases").get_all_records())
        return {str(r["RawName"]).lower().strip(): str(r["CanonicalName"]).strip() for r in data if r.get("RawName")}
    except Exception: return {}

@st.cache_data(ttl=300)
def load_telegram_settings() -> dict:
    try:
        data = retry_gspread(lambda: get_ss().worksheet("TelegramSettings").get_all_records())
        return {r["Key"]: r["Value"] for r in data} if data else {}
    except Exception: return {}

def trigger_run_now():
    try:
        ws = get_ss().worksheet("Settings")
        all_v = ws.get_all_values()
        for i, row in enumerate(all_v[1:], start=2):
            if row and row[0] == "trigger_queue":
                ws.update_cell(i, 2, "RUN"); st.cache_data.clear(); return
        ws.append_row(["trigger_queue", "RUN"]); st.cache_data.clear()
    except Exception as e: st.error(f"Trigger failed: {e}")

def _write_txn(row_dict):
    try:
        ws = get_ss().worksheet("Transactions")
        ws.append_row([row_dict.get(h, "") for h in HEADERS["Transactions"]], value_input_option="USER_ENTERED")
        st.cache_data.clear()
    except Exception as e: st.error(f"Failed to write transaction: {e}")

def _bulk_write_txns(rows):
    try:
        get_ss().worksheet("Transactions").append_rows(rows, value_input_option="USER_ENTERED")
        st.cache_data.clear()
    except Exception as e: st.error(f"Bulk write failed: {e}")

def _update_txn(row_id, upd):
    try:
        ws = get_ss().worksheet("Transactions")
        all_vals = ws.get_all_values()
        hdrs = all_vals[0]
        for i, row in enumerate(all_vals[1:], start=2):
            if row[0] == row_id:
                new_row = [upd.get(h, row[j]) for j, h in enumerate(hdrs)]
                ws.update(f"A{i}:{chr(64+len(hdrs))}{i}", [new_row])
                break
        time.sleep(0.5); st.cache_data.clear()
    except Exception as e: st.error(f"Update failed: {e}")

def _delete_txn(row_id):
    try:
        ws = get_ss().worksheet("Transactions")
        all_vals = ws.get_all_values()
        for i, row in enumerate(all_vals[1:], start=2):
            if row[0] == row_id:
                ws.delete_rows(i); break
        st.cache_data.clear()
    except Exception as e: st.error(f"Delete failed: {e}")

def _bulk_update_merchant_cat(row_ids: list, new_cat: str, new_sub: str):
    if not row_ids: return 0
    try:
        ws = get_ss().worksheet("Transactions")
        all_vals = ws.get_all_values()
        hdrs = all_vals[0]
        cat_col, sub_col = hdrs.index("Category") + 1, hdrs.index("Subcategory") + 1
        id_col, ac_col = hdrs.index("RowID") + 1, hdrs.index("AutoCat") + 1
        id_set, updated = set(row_ids), 0
        from gspread.utils import rowcol_to_a1
        updates = []
        for i, row in enumerate(all_vals[1:], start=2):
            if row[id_col - 1] in id_set:
                updates.append({"range": rowcol_to_a1(i, cat_col), "values": [[new_cat]]})
                updates.append({"range": rowcol_to_a1(i, sub_col), "values": [[new_sub]]})
                updates.append({"range": rowcol_to_a1(i, ac_col), "values": [["no"]]})
                updated += 1
        if updates: ws.batch_update(updates)
        st.cache_data.clear(); return updated
    except Exception as e: st.error(f"Bulk update failed: {e}"); return 0

def _move_subcategory(subcat: str, target_cat: str, retrospective: bool) -> tuple[str, int]:
    ss = get_ss()
    old_cat = None
    try:
        ws_cat = ss.worksheet("Categories")
        all_cats = ws_cat.get_all_values()
        hdrs_c = all_cats[0]
        c_idx = hdrs_c.index("Category")
        s_idx = hdrs_c.index("Subcategory")
        
        for i, row in enumerate(all_cats[1:], start=2):
            if len(row) > s_idx and row[s_idx].strip().lower() == subcat.strip().lower():
                old_cat = row[c_idx]
                ws_cat.update_cell(i, c_idx + 1, target_cat)
                break
    except Exception as e:
        st.error(f"Failed to update Category mapping: {e}")
        return "", 0

    txns_updated = 0
    if retrospective:
        try:
            ws_txn = ss.worksheet("Transactions")
            all_txns = ws_txn.get_all_values()
            hdrs_t = all_txns[0]
            cat_col = hdrs_t.index("Category") + 1
            sub_col = hdrs_t.index("Subcategory") + 1
            ac_col  = hdrs_t.index("AutoCat") + 1
            
            from gspread.utils import rowcol_to_a1
            updates = []
            for i, row in enumerate(all_txns[1:], start=2):
                if len(row) >= sub_col and row[sub_col - 1].strip().lower() == subcat.strip().lower():
                    updates.append({"range": rowcol_to_a1(i, cat_col), "values": [[target_cat]]})
                    updates.append({"range": rowcol_to_a1(i, ac_col), "values": [["no"]]})
                    txns_updated += 1
            if updates: ws_txn.batch_update(updates)
        except Exception as e: st.error(f"Retrospective update failed: {e}")

    st.cache_data.clear()
    return old_cat or "", txns_updated

def save_merchant_alias(raw: str, canonical: str):
    try:
        ws = get_ss().worksheet("MerchantAliases")
        all_v = ws.get_all_values()
        for i, row in enumerate(all_v[1:], start=2):
            if row and str(row[0]).lower().strip() == raw.lower().strip():
                ws.update_cell(i, 2, canonical)
                ws.update_cell(i, 3, date.today().isoformat())
                st.cache_data.clear(); return
        ws.append_row([raw.strip(), canonical.strip(), date.today().isoformat()])
        st.cache_data.clear()
    except Exception as e: st.error(f"Alias save failed: {e}")

def delete_merchant_alias(raw: str):
    try:
        ws = get_ss().worksheet("MerchantAliases")
        all_v = ws.get_all_values()
        for i, row in enumerate(all_v[1:], start=2):
            if row and str(row[0]).lower().strip() == raw.lower().strip():
                ws.delete_rows(i); st.cache_data.clear(); return
    except Exception as e: st.error(f"Alias delete failed: {e}")

def save_telegram_setting(key: str, value: str):
    try:
        ws = get_ss().worksheet("TelegramSettings")
        all_v = ws.get_all_values()
        for i, row in enumerate(all_v[1:], start=2):
            if row and row[0] == key:
                ws.update_cell(i, 2, value)
                st.cache_data.clear(); return
        ws.append_row([key, value])
        st.cache_data.clear()
    except Exception as e: st.error(f"Telegram setting failed: {e}")

def send_telegram(bot_token: str, chat_id: str, message: str) -> tuple[bool, str]:
    import urllib.request as _ur, urllib.parse as _up
    try:
        url  = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = _up.urlencode({"chat_id": chat_id, "text": message, "parse_mode": "HTML"}).encode()
        req  = _ur.Request(url, data=data)
        _ur.urlopen(req, timeout=8)
        return True, ""
    except Exception as e: return False, str(e)

def check_and_send_budget_alerts(df: pd.DataFrame, budgets: pd.DataFrame, settings: dict, tg_cfg: dict):
    if not tg_cfg.get("bot_token") or not tg_cfg.get("chat_id") or budgets.empty or df.empty: return
    bot_token, chat_id = tg_cfg["bot_token"], tg_cfg["chat_id"]
    threshold = float(tg_cfg.get("alert_pct", 80))
    sym = settings.get("currency_symbol", "₹")
    now = datetime.today()
    ms, me = month_range(now.year, now.month)
    mdf = df[(df["Date"].dt.date >= ms) & (df["Date"].dt.date <= me)]
    exp_df = mdf[mdf["Amount"] < 0].copy()
    if exp_df.empty: return
    exp_df["Abs"] = exp_df["Amount"].abs()
    cat_totals = exp_df.groupby("Category")["Abs"].sum()
    alert_key_prefix = f"tg_alerted_{now.year}_{now.month:02d}_"

    for _, brow in budgets.iterrows():
        cat = str(brow["Category"])
        bud = float(brow["MonthlyBudget"] or 0)
        if bud <= 0 or cat not in cat_totals: continue
        actual = cat_totals[cat]
        pct = actual / bud * 100
        if pct < threshold: continue
        alert_key = alert_key_prefix + cat.replace(" ","_")[:20]
        if tg_cfg.get(alert_key): continue
        msg = f"🚨 <b>Budget Alert — {cat_icon(cat)} {cat}</b>\n\nSpent <b>{sym}{actual:,.0f}</b> of {sym}{bud:,.0f} ({pct:.0f}%).\nRemaining: {sym}{max(bud-actual,0):,.0f}"
        ok, _ = send_telegram(bot_token, chat_id, msg)
        if ok: save_telegram_setting(alert_key, "sent")

def _write_email_rule(rule_dict):
    try:
        get_ss().worksheet("EmailRules").append_row([rule_dict.get(h, "") for h in HEADERS["EmailRules"]])
        st.cache_data.clear()
    except Exception as e: st.error(f"Rule write failed: {e}")

def _delete_email_rule(rule_name):
    try:
        ws = get_ss().worksheet("EmailRules")
        all_vals = ws.get_all_values()
        for i, row in enumerate(all_vals[1:], start=2):
            if row and row[0] == rule_name: ws.delete_rows(i); break
        st.cache_data.clear()
    except Exception as e: st.error(f"Rule delete failed: {e}")

def _update_email_rule(rule_name, upd):
    try:
        ws = get_ss().worksheet("EmailRules")
        all_vals = ws.get_all_values()
        hdrs = all_vals[0]
        for i, row in enumerate(all_vals[1:], start=2):
            if row and row[0] == rule_name:
                new_row = [upd.get(h, row[j]) for j, h in enumerate(hdrs)]
                ws.update(f"A{i}:{chr(64+len(hdrs))}{i}", [new_row]); break
        st.cache_data.clear()
    except Exception as e: st.error(f"Rule update failed: {e}")

# ── DOMAIN HELPERS ────────────────────────────────────────────────────────────

def auto_cat(merchant: str, cats_df: pd.DataFrame, remarks: str = ""):
    m = (merchant + " " + remarks).lower().strip()
    for _, row in cats_df.iterrows():
        kws = str(row.get("Keywords", "")).lower()
        if not kws: continue
        for kw in kws.split(","):
            if kw.strip() and kw.strip() in m:
                return row["Category"], row["Subcategory"], "high"
    return "Others", "Miscellaneous", "low"

def extract_accounts(df: pd.DataFrame) -> list:
    if df.empty or "Tags" not in df.columns: return []
    tags = df["Tags"].dropna().astype(str)
    tags = tags[tags.str.strip().str.len() > 0]
    return sorted(tags.unique().tolist())

def account_badge_html(account: str) -> str:
    acc = str(account).upper()
    color, bg = (C["warning"], "rgba(240,165,0,0.18)") if "CC" in acc or "CREDIT" in acc else \
                (C["info"], "rgba(88,166,255,0.18)") if "UPI" in acc or "PAYTM" in acc else \
                (C["income"], "rgba(0,200,150,0.18)")
    style = f"background:{bg};color:{color};font-size:.6rem;font-weight:800;letter-spacing:.4px;padding:2px 7px;border-radius:20px;text-transform:uppercase;white-space:nowrap;display:inline-block"
    return f'<span style="{style}">{account}</span>'

def filter_by_account(df: pd.DataFrame, acct: str) -> pd.DataFrame:
    if acct == "All" or df.empty: return df
    return df[df["Tags"].astype(str) == acct]

def parse_email_body(template: str, body: str) -> dict | None:
    TAGS = ["amt", "act", "tdetails", "date", "skip"]
    MARKER = "\x00"
    safe_tmpl = template
    for tag in TAGS: safe_tmpl = safe_tmpl.replace(f"{{{tag}}}", f"{MARKER}{tag}{MARKER}")
    parts = safe_tmpl.split(MARKER)
    regex_parts, named_groups = [], []
    for i, part in enumerate(parts):
        if i % 2 == 0: regex_parts.append(re.escape(part))
        else:
            if part in ("skip",) or part in named_groups: regex_parts.append(r"(?:.*?)")
            else: named_groups.append(part); regex_parts.append(f"(?P<{part}>.*?)")
    if not named_groups: return None
    final_regex = "".join(regex_parts)
    last_lazy = final_regex.rfind(".*?")
    if last_lazy >= 0: final_regex = final_regex[:last_lazy] + ".*" + final_regex[last_lazy + 3:]
    try:
        m = re.search(final_regex, body, re.DOTALL | re.IGNORECASE)
        if not m: return None
        return {tag: m.group(tag).strip() for tag in named_groups if m.group(tag) is not None}
    except: return None

def clean_amount(raw: str) -> float | None:
    try: return float(re.sub(r"[₹$,\s]", "", str(raw)))
    except: return None

def month_range(year, month):
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])

def cat_icon(cat):
    mapping = {row[0]: row[3] for row in DEFAULT_CATEGORIES}
    return mapping.get(cat, "📌")

def get_descending_months(df: pd.DataFrame) -> list:
    if df.empty or "Date" not in df.columns:
        now = datetime.today()
        return [now.strftime("%B %Y")]
    dates = df["Date"].dropna()
    if dates.empty:
        now = datetime.today()
        return [now.strftime("%B %Y")]
    periods = dates.dt.to_period("M").unique()
    sorted_periods = sorted(periods, reverse=True)
    return [p.strftime("%B %Y") for p in sorted_periods]

# ═══════════════════════════════════════════════════════════════════════════════
#  CSS & THEME INJECTION (MATERIAL 3 MOBILE NATIVE)
# ═══════════════════════════════════════════════════════════════════════════════

def inject_css():
    is_mobile = (st.session_state.get("view_mode") == "mobile")
    max_w = "480px" if is_mobile else "1380px"
    st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&family=JetBrains+Mono:wght@500;700&display=swap');

html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {{
    background: {C["bg"]} !important;
    color: {C["text"]};
    font-family: 'Nunito', sans-serif;
}}

.block-container {{
    max-width: {max_w} !important;
    padding: {"10px 12px 90px" if is_mobile else "12px 16px 24px"} !important;
    margin: 0 auto !important;
    transition: max-width 0.3s ease;
}}

[data-testid="stHeader"], [data-testid="stToolbar"], footer, #MainMenu {{ display:none !important; }}

/* Force Streamlit columns to NEVER collapse into vertical stacks */
div[data-testid="stHorizontalBlock"] {{
    display: flex !important;
    flex-direction: row !important;
    flex-wrap: nowrap !important;
    gap: 6px !important;
}}
div[data-testid="column"] {{
    width: auto !important;
    flex: 1 1 0px !important;
    min-width: 0 !important;
}}

/* Panels */
.card-panel {{
    background: {C["surface"]};
    border: 1px solid {C["border"]};
    border-radius: 18px;
    padding: 16px 18px;
    margin-bottom: 16px;
}}
.card-title {{
    font-size: 0.82rem; font-weight: 800; letter-spacing: 1px;
    text-transform: uppercase; color: {C["muted"]}; margin-bottom: 12px;
}}

/* KPI Cards */
.kpi-card {{
    background: {C["surface"]};
    border: 1px solid {C["border"]};
    border-radius: 16px; padding: 14px 16px;
}}
.kpi-label {{
    font-size: 0.62rem; font-weight: 800; text-transform: uppercase;
    letter-spacing: 0.8px; color: {C["muted"]}; margin-bottom: 4px;
}}
.kpi-value {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.45rem; font-weight: 700;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}}
.kpi-sub {{ font-size: 0.7rem; margin-top: 4px; color: {C["muted"]}; }}

/* Summary Strip */
.summary-strip {{
    background: {C["surface2"]};
    border-left: 4px solid {C["primary"]};
    border-radius: 0 12px 12px 0;
    padding: 10px 16px; margin-bottom: 16px;
    display: flex; justify-content: space-between; align-items: center;
    font-size: 0.82rem; flex-wrap: wrap; gap: 8px;
}}

/* Mobile Native Item Card Components */
.mobile-card-row {{
    display: flex;
    align-items: center;
    gap: 12px;
    background: {C["surface"]};
    border: 1px solid {C["border"]};
    border-radius: 16px;
    padding: 12px;
    margin-bottom: 8px;
}}
.txn-avatar-circle {{
    width: 40px; height: 40px;
    border-radius: 14px;
    background: {C["surface2"]};
    display: flex; align-items: center; justify-content: center;
    font-size: 1.15rem; flex-shrink: 0;
}}
.txn-notes-tag {{
    font-size: 0.7rem;
    color: {C["muted"]};
    font-style: italic;
    margin-top: 2px;
}}

/* Floating Action Button (FAB) */
.fab-button-fixed {{
    position: fixed;
    bottom: 24px;
    right: 24px;
    z-index: 999999;
}}

/* Buttons & Inputs */
[data-testid="stFormSubmitButton"] > button,
[data-testid="stButton"] > button[kind="primary"] {{
    background: {C["primary"]} !important; color: white !important;
    border-radius: 12px !important; font-weight: 800 !important;
    padding: 8px 16px !important; box-shadow: 0 3px 12px rgba(124,109,248,.35) !important;
}}
[data-testid="stButton"] > button {{
    border-radius: 10px !important; font-weight: 700 !important;
}}

.amt-exp {{ font-family: 'JetBrains Mono', monospace; color: {C["expense"]}; font-weight: 700; }}
.amt-inc {{ font-family: 'JetBrains Mono', monospace; color: {C["income"]}; font-weight: 700; }}
.cat-pill {{ background: {C["primary_dim"]}; color: {C["primary"]}; padding: 2px 8px; border-radius: 10px; font-size: 0.72rem; font-weight: 700; }}
.subcat-text {{ color: {C["muted"]}; font-size: 0.75rem; }}

::-webkit-scrollbar {{ width: 3px; height: 3px; }}
::-webkit-scrollbar-thumb {{ background: {C["border"]}; border-radius: 2px; }}
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  STATE & DIALOGS
# ═══════════════════════════════════════════════════════════════════════════════

def init_state():
    defaults = {
        "nav":              "home",
        "view_mode":        "desktop",
        "edit_txn":         None,
        "filter_cat":       "All",
        "filter_subcat":    "All",
        "acct_filter":      "All",
        "search":           "",
        "preview_rows":     None,
        "setup_ok":         False,
        "pending_bulk":       None,
        "review_misc_page":   False,
        "tg_test_result":     None,
        "edit_rule_name":     None,
        "home_rows_n":        15,
        "home_page":          1,
        "spends_rows_n":      15,
        "spends_page":        1,
        "show_acct_breakdown": False,
        "email_parse_result": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

@st.dialog("➕ Quick Add Expense", width="small")
def dlg_quick_add():
    df_all = _load_transactions()
    settings = load_settings()
    sym = settings.get("currency_symbol","₹")
    cats_sorted, sub_map = load_cat_freq()

    st.caption("One-Tap Presets:")
    p1, p2, p3, p4 = st.columns(4)
    with p1:
        if st.button("☕ Coffee ₹150", key="qa_p1", use_container_width=True):
            _write_txn({"RowID": str(uuid.uuid4())[:8], "Date": date.today().strftime("%d/%m/%Y"), "Merchant": "Coffee Shop", "Amount": -150, "Type": "Expense", "Category": "Food & Dining", "Subcategory": "Snacks & Sweets", "Tags": "Paytm UPI", "Source": "preset"})
            st.toast("✅ Added Coffee ₹150", icon="✅"); st.rerun()
    with p2:
        if st.button("⛽ Fuel ₹1k", key="qa_p2", use_container_width=True):
            _write_txn({"RowID": str(uuid.uuid4())[:8], "Date": date.today().strftime("%d/%m/%Y"), "Merchant": "Fuel Station", "Amount": -1000, "Type": "Expense", "Category": "Transport", "Subcategory": "Fuel", "Tags": "HDFC CC 7500", "Source": "preset"})
            st.toast("✅ Added Fuel ₹1,000", icon="✅"); st.rerun()
    with p3:
        if st.button("🍔 Swiggy ₹400", key="qa_p3", use_container_width=True):
            _write_txn({"RowID": str(uuid.uuid4())[:8], "Date": date.today().strftime("%d/%m/%Y"), "Merchant": "Swiggy", "Amount": -400, "Type": "Expense", "Category": "Food & Dining", "Subcategory": "Restaurants & Mess", "Tags": "Paytm UPI", "Source": "preset"})
            st.toast("✅ Added Swiggy ₹400", icon="✅"); st.rerun()
    with p4:
        if st.button("🛒 Blinkit ₹600", key="qa_p4", use_container_width=True):
            _write_txn({"RowID": str(uuid.uuid4())[:8], "Date": date.today().strftime("%d/%m/%Y"), "Merchant": "Blinkit", "Amount": -600, "Type": "Expense", "Category": "Food & Dining", "Subcategory": "Groceries", "Tags": "HDFC CC 7500", "Source": "preset"})
            st.toast("✅ Added Blinkit ₹600", icon="✅"); st.rerun()

    st.write("---")
    with st.form("quick_add_f", clear_on_submit=True):
        amount = st.number_input(f"Amount ({sym})", min_value=0.0, step=1.0, format="%.0f")
        merch  = st.text_input("Merchant", placeholder="e.g. Marudhar Mart, Swiggy...")
        sel_cat = st.selectbox("Category", cats_sorted)
        subs = sub_map.get(sel_cat, [])
        sel_sub = st.selectbox("Subcategory", subs if subs else ["Miscellaneous"])
        existing_accounts = extract_accounts(df_all)
        sel_acct = st.selectbox("Account Tag", existing_accounts if existing_accounts else ["HDFC CC 7500"])
        notes = st.text_input("Notes (Optional)")

        if st.form_submit_button("💾 Save Expense", use_container_width=True, type="primary"):
            if amount > 0 and merch.strip():
                _write_txn({
                    "RowID": str(uuid.uuid4())[:8], "Date": date.today().strftime("%d/%m/%Y"),
                    "Merchant": merch.strip().title(), "Amount": -abs(amount), "Type": "Expense",
                    "Category": sel_cat, "Subcategory": sel_sub, "PaymentMethod": "UPI",
                    "Tags": sel_acct, "Notes": notes, "Source": "quick_add", "AutoCat": "no"
                })
                st.toast(f"✅ Added {merch} ({sym}{amount:,.0f})", icon="✅"); st.rerun()

@st.dialog("✏️ Edit Transaction", width="small")
def dlg_edit(txn):
    cats_sorted, sub_map = load_cat_freq()
    df_all = _load_transactions()
    
    cur_cat = str(txn.get("Category","")).strip()
    cur_sub = str(txn.get("Subcategory","")).strip()
    orig_cat, orig_sub = cur_cat, cur_sub

    cat_opts = cats_sorted + ["➕ New category…"]
    cat_idx = cats_sorted.index(cur_cat) if cur_cat in cats_sorted else 0

    amount = st.number_input("Amount (₹)", value=abs(float(txn["Amount"])), min_value=0.0, step=1.0, format="%.0f")
    merch  = st.text_input("Merchant", value=txn["Merchant"])
    
    sel_cat_r = st.selectbox("Category", cat_opts, index=cat_idx)
    if sel_cat_r == "➕ New category…":
        nc = st.text_input("New category name")
        ns = st.text_input("First subcategory name")
        if st.button("✅ Create Category"):
            if nc.strip() and ns.strip():
                get_ss().worksheet("Categories").append_row([nc.strip(), ns.strip(),"","📌"])
                st.cache_data.clear(); st.rerun()
        sel_cat = cats_sorted[0] if cats_sorted else "Others"
    else: sel_cat = sel_cat_r

    subs = sub_map.get(sel_cat, [])
    sub_idx = subs.index(cur_sub) if cur_sub in subs else 0
    sub_opts = subs + ["➕ New subcategory…"] if subs else ["➕ New subcategory…"]
    sel_sub_r = st.selectbox("Subcategory", sub_opts, index=sub_idx)
    if sel_sub_r == "➕ New subcategory…":
        ns2 = st.text_input("New subcategory name")
        if st.button("✅ Add Subcategory"):
            if ns2.strip():
                get_ss().worksheet("Categories").append_row([sel_cat, ns2.strip(),"","📌"])
                st.cache_data.clear(); st.rerun()
        sel_sub = subs[0] if subs else ""
    else: sel_sub = sel_sub_r
    
    existing_accounts = extract_accounts(df_all)
    cur_tag = str(txn.get("Tags","")).strip()
    acct_opts = []
    for a in existing_accounts:
        if a and a not in acct_opts: acct_opts.append(a)
    if cur_tag and cur_tag not in acct_opts: acct_opts.append(cur_tag)
    acct_opts.append("✏️ New account…")
    acct_idx = acct_opts.index(cur_tag) if cur_tag in acct_opts else 0

    dlg_acct_raw = st.selectbox("Account Tag", acct_opts, index=acct_idx)
    dlg_acct = st.text_input("New Account Label", value=cur_tag) if dlg_acct_raw == "✏️ New account…" else dlg_acct_raw
    
    txn_dt = st.date_input("Date", value=txn["Date"].date() if hasattr(txn["Date"],"date") else date.today())
    notes  = st.text_input("Notes", value=txn.get("Notes",""))

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("💾 Save", use_container_width=True, type="primary"):
            if amount > 0 and merch.strip():
                upd = {
                    "RowID": txn["RowID"], "Date": txn_dt.strftime("%d/%m/%Y"),
                    "Merchant": merch.strip(), "Type": "Expense",
                    "Amount": -abs(amount), "Category": sel_cat, "Subcategory": sel_sub,
                    "PaymentMethod": txn.get("PaymentMethod","UPI"), "Tags": dlg_acct,
                    "Notes": notes, "Source": txn.get("Source","manual"), "AutoCat": "no",
                }
                _update_txn(txn["RowID"], upd)
                st.session_state.edit_txn = None

                if sel_cat != orig_cat or sel_sub != orig_sub:
                    others = df_all[(df_all["Merchant"].str.strip().str.lower() == merch.strip().lower()) & (df_all["RowID"].astype(str) != str(txn["RowID"]))]
                    to_update = others[~((others["Category"] == sel_cat) & (others["Subcategory"] == sel_sub))]
                    if not to_update.empty:
                        st.session_state.pending_bulk = {
                            "merchant": merch.strip(),
                            "cat": sel_cat,
                            "sub": sel_sub,
                            "skip_id": txn["RowID"]
                        }
                    else: st.toast("✅ Transaction updated!", icon="✅")
                else: st.toast("✅ Transaction updated!", icon="✅")
                st.rerun()
    with c2:
        if st.button("🗑️ Delete", use_container_width=True):
            _delete_txn(txn["RowID"])
            st.session_state.edit_txn = None; st.rerun()
    with c3:
        if st.button("✕ Close", use_container_width=True):
            st.session_state.edit_txn = None; st.rerun()

@st.dialog("🔄 Apply to Similar Transactions", width="small")
def dlg_bulk_suggest():
    pb = st.session_state.pending_bulk
    merchant, new_cat, new_sub, skip_id = pb["merchant"], pb["cat"], pb["sub"], pb["skip_id"]
    df_all = _load_transactions()
    others = df_all[(df_all["Merchant"].str.strip().str.lower() == merchant.strip().lower()) & (df_all["RowID"].astype(str) != str(skip_id))]
    to_update = others[~((others["Category"] == new_cat) & (others["Subcategory"] == new_sub))]

    if to_update.empty:
        st.session_state.pending_bulk = None; st.rerun(); return

    st.write(f"Found **{len(to_update)}** past transactions for **{merchant}** with different category.")
    st.dataframe(to_update[["Date","Merchant","Amount","Category","Subcategory"]].head(10), use_container_width=True)
    b1, b2 = st.columns(2)
    with b1:
        if st.button("✅ Approve All", use_container_width=True, type="primary"):
            _bulk_update_merchant_cat(to_update["RowID"].astype(str).tolist(), new_cat, new_sub)
            st.session_state.pending_bulk = None; st.rerun()
    with b2:
        if st.button("✕ Skip", use_container_width=True):
            st.session_state.pending_bulk = None
            st.toast("✅ Saved edit for this transaction.", icon="✅")
            st.rerun()

@st.dialog("🗂️ Review Uncategorised Transactions", width="small")
def dlg_review_misc():
    df_all = _load_transactions()
    cats_sorted, sub_map = load_cat_freq()
    misc = df_all[(df_all["Category"].astype(str).str.strip() == "Others") | (df_all["Subcategory"].astype(str).str.strip() == "Miscellaneous")]

    if misc.empty:
        st.success("🎉 No uncategorised transactions found!")
        if st.button("✕ Close", use_container_width=True):
            st.session_state.review_misc_page = False; st.rerun()
        return

    st.write(f"Found **{len(misc)}** uncategorised transactions.")
    bulk_cat = st.selectbox("Category (bulk)", cats_sorted, key="rm_bulk_cat")
    bulk_sub = st.selectbox("Subcategory (bulk)", sub_map.get(bulk_cat, ["Miscellaneous"]), key="rm_bulk_sub")

    b1, b2 = st.columns(2)
    with b1:
        if st.button(f"✅ Apply to All {len(misc)}", use_container_width=True, type="primary"):
            _bulk_update_merchant_cat(misc["RowID"].astype(str).tolist(), bulk_cat, bulk_sub)
            st.session_state.review_misc_page = False; st.rerun()
    with b2:
        if st.button("✕ Close", use_container_width=True):
            st.session_state.review_misc_page = False; st.rerun()

    st.write("---")
    st.caption("OR Recategorize By Merchant Group:")
    merch_groups = misc.groupby("Merchant").agg(count=("RowID","count"), total=("Amount", lambda x: x.abs().sum()), ids=("RowID", list)).reset_index().sort_values("total", ascending=False)
    for idx, mg in merch_groups.head(15).iterrows():
        mname, mcount, mtotal, mids = str(mg["Merchant"]), int(mg["count"]), float(mg["total"]), [str(x) for x in mg["ids"]]
        unique_k = f"{idx}_{hash(mname) & 0xffffff}"
        mc1, mc2, mc3 = st.columns([3, 3, 1])
        with mc1: st.write(f"**{mname[:22]}** ({mcount} txns, ₹{mtotal:,.0f})")
        with mc2:
            sc = st.selectbox("", cats_sorted, key=f"rm_c_{unique_k}", label_visibility="collapsed")
            ss = st.selectbox("", sub_map.get(sc, ["Miscellaneous"]), key=f"rm_s_{unique_k}", label_visibility="collapsed")
        with mc3:
            if st.button("✓", key=f"rm_ap_{unique_k}"):
                _bulk_update_merchant_cat(mids, sc, ss); st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
#  TOP HEADER & NAVIGATION (MOBILE NATIVE OPTIMIZED)
# ═══════════════════════════════════════════════════════════════════════════════

def render_top_bar():
    c_head, c_mode = st.columns([4, 2])
    with c_head:
        st.markdown("""
        <div style="display:flex;align-items:center;gap:10px;">
            <span style="font-size:1.8rem">💳</span>
            <div>
                <div style="font-size:1.3rem;font-weight:900;line-height:1.1">ClearSpend Engine</div>
                <div style="font-size:0.75rem;color:#8b949e">Multi-Account Expense Analytics</div>
            </div>
        </div>""", unsafe_allow_html=True)
    with c_mode:
        m_col1, m_col2 = st.columns(2)
        with m_col1:
            if st.button("💻 Desktop", key="mode_desk", type="primary" if st.session_state.view_mode=="desktop" else "secondary", use_container_width=True):
                st.session_state.view_mode = "desktop"; st.rerun()
        with m_col2:
            if st.button("📱 Mobile", key="mode_mob", type="primary" if st.session_state.view_mode=="mobile" else "secondary", use_container_width=True):
                st.session_state.view_mode = "mobile"; st.rerun()

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    
    NAV_MAP = {
        "home": "🏠 Home",
        "analytics": "📊 Analysis",
        "transactions": "📋 Spends",
        "categories": "🏷️ Categories",
        "add": "➕ Add & Import",
        "settings": "⚙️ Settings"
    }

    if st.session_state.view_mode == "mobile":
        sel_nav_label = st.selectbox(
            "📱 Select Screen:",
            list(NAV_MAP.values()),
            index=list(NAV_MAP.keys()).index(st.session_state.nav),
            key="mobile_nav_dropdown"
        )
        selected_nav_key = [k for k, v in NAV_MAP.items() if v == sel_nav_label][0]
        if selected_nav_key != st.session_state.nav:
            st.session_state.nav = selected_nav_key
            st.session_state.review_misc_page = False
            st.session_state.edit_txn = None
            st.session_state.pending_bulk = None
            st.rerun()
    else:
        cols = st.columns(len(NAV_MAP))
        for i, (key, label) in enumerate(NAV_MAP.items()):
            with cols[i]:
                if st.button(label, key=f"nav_btn_{key}", type="primary" if st.session_state.nav == key else "secondary", use_container_width=True):
                    st.session_state.nav = key
                    st.session_state.review_misc_page = False
                    st.session_state.edit_txn = None
                    st.session_state.pending_bulk = None
                    st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN 1 — HOME SCREEN (MATERIAL 3 DASHBOARD)
# ═══════════════════════════════════════════════════════════════════════════════

def screen_home():
    df = _load_transactions()
    settings = load_settings()
    sym = settings.get("currency_symbol","₹")
    budget = float(settings.get("monthly_budget", 30000))
    
    months_opts = get_descending_months(df)
    
    f_c1, f_c2 = st.columns(2)
    with f_c1: sel_month_str = st.selectbox("📅 Period:", months_opts, key="home_month_dd")
    with f_c2: sel_acct = st.selectbox("💳 Account:", ["All"] + extract_accounts(df), key="home_acct_dd")

    if not df.empty and "Date" in df.columns:
        sel_dt = datetime.strptime(sel_month_str, "%B %Y")
        ms, me = month_range(sel_dt.year, sel_dt.month)
        mdf = df[(df["Date"].dt.date >= ms) & (df["Date"].dt.date <= me)]
    else: mdf = df.copy()
    mdf = filter_by_account(mdf, sel_acct)
    
    exp_df = mdf[mdf["Amount"] < 0].copy() if not mdf.empty else pd.DataFrame()
    inc_df = mdf[mdf["Amount"] > 0].copy() if not mdf.empty else pd.DataFrame()
    
    spent  = abs(exp_df["Amount"].sum()) if not exp_df.empty else 0
    income = inc_df["Amount"].sum() if not inc_df.empty else 0
    rem_budget = max(budget - spent, 0)
    pct_used = min(spent / budget * 100, 100) if budget > 0 else 0

    # Material 3 Income vs Spending Side-by-Side Cards
    m1, m2 = st.columns(2)
    with m1:
        st.markdown(f"""
        <div class="kpi-card" style="background: rgba(46, 125, 50, 0.12); border-color: rgba(46, 125, 50, 0.3);">
            <div class="kpi-label" style="color:{C["income_light"]}">Income</div>
            <div class="kpi-value" style="color:{C["income_light"]}">{sym}{income:,.0f}</div>
        </div>""", unsafe_allow_html=True)
    with m2:
        st.markdown(f"""
        <div class="kpi-card" style="background: rgba(229, 57, 53, 0.12); border-color: rgba(229, 57, 53, 0.3);">
            <div class="kpi-label" style="color:{C["expense_light"]}">Spending</div>
            <div class="kpi-value" style="color:{C["expense_light"]}">{sym}{spent:,.0f}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    # Accounts Section Cards
    accounts = extract_accounts(mdf)
    if accounts:
        st.markdown('<div class="card-title">💳 Accounts Overview</div>', unsafe_allow_html=True)
        acct_cols = st.columns(min(len(accounts), 4))
        for idx, acct in enumerate(accounts[:4]):
            acct_amt = abs(exp_df[exp_df["Tags"].astype(str) == acct]["Amount"].sum()) if not exp_df.empty else 0
            with acct_cols[idx]:
                st.markdown(f"""
                <div class="kpi-card" style="border-top:3px solid {C["primary"] if idx%2==0 else C["warning"]};">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span style="font-weight:800;font-size:0.82rem;">{acct}</span>
                        <span>💳</span>
                    </div>
                    <div style="font-family:'JetBrains Mono',monospace;font-weight:700;font-size:1.1rem;margin-top:4px;color:{C["expense_light"]};">
                        {sym}{acct_amt:,.0f}
                    </div>
                </div>""", unsafe_allow_html=True)

    # Budgets Progress Bar Card
    st.markdown('<div class="card-title" style="margin-top:14px;">Budget Limit</div>', unsafe_allow_html=True)
    st.markdown(f"""
    <div class="card-panel" style="padding:14px;">
        <div style="display:flex;justify-content:space-between;font-size:0.8rem;font-weight:800;margin-bottom:6px;">
            <span>{sel_month_str} Budget</span>
            <span style="color:{C["primary"]};">{sym}{spent:,.0f} of {sym}{budget:,.0f} ({pct_used:.1f}%)</span>
        </div>
    </div>""", unsafe_allow_html=True)
    st.progress(pct_used / 100)

    # Category Donut Chart
    st.markdown('<div class="card-title" style="margin-top:14px;">Category Distribution</div>', unsafe_allow_html=True)
    if not exp_df.empty:
        exp_df["Abs"] = exp_df["Amount"].abs()
        cat_grp = exp_df.groupby("Category")["Abs"].sum().reset_index()
        fig_donut = px.pie(cat_grp, values="Abs", names="Category", hole=0.6, color_discrete_sequence=px.colors.qualitative.Bold)
        fig_donut.update_layout(height=220, margin=dict(l=0,r=0,t=10,b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color=C["text"], showlegend=True)
        st.plotly_chart(fig_donut, use_container_width=True, config={"displayModeBar":False})

    # Recent Transactions List (Grouped Date Cards)
    st.markdown('<div class="card-title" style="margin-top:14px">📋 Recent Transactions</div>', unsafe_allow_html=True)
    if not mdf.empty:
        n_rows = st.session_state.home_rows_n
        all_sorted = mdf.sort_values("Date", ascending=False)
        recent_df = all_sorted.head(n_rows)

        for day_dt, group in recent_df.groupby(recent_df["Date"].dt.date, sort=False):
            day_str = pd.Timestamp(day_dt).strftime("%d %B %Y %A") if pd.notna(day_dt) else "Recent"
            day_total = group["Amount"].sum()
            amt_color = C["income_light"] if day_total > 0 else C["expense_light"]
            amt_sign = "+" if day_total > 0 else "-"

            st.markdown(f"""
            <div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid {C["border"]};margin:10px 0 6px;">
                <span style="font-size:0.75rem;font-weight:800;color:{C["muted"]};">{day_str}</span>
                <span style="font-family:'JetBrains Mono',monospace;font-weight:700;font-size:0.8rem;color:{amt_color};">{amt_sign}{sym}{abs(day_total):,.0f}</span>
            </div>""", unsafe_allow_html=True)

            for idx, row in group.iterrows():
                rid = str(row.get("RowID", idx))
                c_ed, c_card = st.columns([0.15, 0.85])
                with c_ed:
                    if st.button("✏️", key=f"h_ed_{rid}"):
                        st.session_state.edit_txn = row.to_dict(); st.rerun()
                with c_card:
                    notes_html = f'<div class="txn-notes-tag">📝 {row["Notes"]}</div>' if str(row.get("Notes","")).strip() else ""
                    acct_badge = account_badge_html(row.get("Tags","")) if row.get("Tags") else ""
                    ico = cat_icon(row["Category"])
                    is_exp = row["Amount"] < 0
                    amt_cls = "amt-exp" if is_exp else "amt-inc"
                    sign_str = "-" if is_exp else "+"

                    st.markdown(f"""
                    <div class="mobile-card-row">
                        <div class="txn-avatar-circle">{ico}</div>
                        <div style="flex:1;min-width:0;">
                            <div style="font-weight:800;font-size:0.88rem;">{row["Merchant"]}</div>
                            <div style="font-size:0.72rem;color:{C["muted"]};margin-top:2px;">
                                <span class="cat-pill">{row["Category"]}</span> {acct_badge}
                            </div>
                            {notes_html}
                        </div>
                        <div class="{amt_cls}" style="font-size:0.92rem;">{sign_str}{sym}{abs(row["Amount"]):,.0f}</div>
                    </div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN 2 — ANALYSIS / INSIGHTS SCREEN
# ═══════════════════════════════════════════════════════════════════════════════

def screen_analytics():
    df = _load_transactions()
    settings = load_settings()
    sym = settings.get("currency_symbol", "₹")

    if df.empty: st.info("No transaction data available."); return

    # 1. 12-MONTH HISTORICAL SUMMARY
    st.markdown('<div class="card-title">🚀 12-Month Historical Summary</div>', unsafe_allow_html=True)
    all_exp = df[df["Amount"] < 0].copy()
    if not all_exp.empty:
        all_exp["Abs"] = all_exp["Amount"].abs()
        all_exp["YearMonth"] = all_exp["Date"].dt.to_period("M")
        
        now_dt = datetime.today()
        last_12_periods = pd.period_range(end=pd.Period(now_dt, freq="M"), periods=12, freq="M")
        m_summary = all_exp.groupby("YearMonth")["Abs"].sum().reindex(last_12_periods, fill_value=0).reset_index()
        m_summary.columns = ["Period", "Spend"]
        m_summary["MonthStr"] = m_summary["Period"].dt.strftime("%b %Y")

        # 12M Plotly Area / Trend Chart
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=m_summary["MonthStr"], y=m_summary["Spend"],
            mode="lines+markers", fill="tozeroy",
            line=dict(color=C["primary"], width=3),
            fillcolor="rgba(124, 109, 248, 0.15)"
        ))
        fig_trend.update_layout(height=200, margin=dict(l=0,r=0,t=10,b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color=C["text"])
        st.plotly_chart(fig_trend, use_container_width=True, config={"displayModeBar": False})

    # 2. PERIOD SELECTION & SUMMARY TABLE
    months_opts = get_descending_months(df)
    c1, c2 = st.columns(2)
    with c1: sel_month = st.selectbox("📅 Period:", months_opts, key="insights_m_dd")
    with c2: sel_acct = st.selectbox("💳 Account:", ["All"] + extract_accounts(df), key="insights_a_dd")

    sel_dt = datetime.strptime(sel_month, "%B %Y")
    ms, me = month_range(sel_dt.year, sel_dt.month)
    mdf = filter_by_account(df[(df["Date"].dt.date >= ms) & (df["Date"].dt.date <= me)], sel_acct)
    
    exp_df = mdf[mdf["Amount"] < 0].copy() if not mdf.empty else pd.DataFrame()
    inc_df = mdf[mdf["Amount"] > 0].copy() if not mdf.empty else pd.DataFrame()
    
    cur_exp = abs(exp_df["Amount"].sum()) if not exp_df.empty else 0
    cur_inc = inc_df["Amount"].sum() if not inc_df.empty else 0
    net_tot = cur_inc - cur_exp

    # Summary Card Table
    st.markdown(f"""
    <div class="card-panel">
        <div class="card-title">Summary — {sel_month}</div>
        <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid {C["border"]};">
            <span>🔴 Spending</span><strong class="amt-exp">-{sym}{cur_exp:,.0f}</strong>
        </div>
        <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid {C["border"]};">
            <span>🟢 Income</span><strong class="amt-inc">+{sym}{cur_inc:,.0f}</strong>
        </div>
        <div style="display:flex;justify-content:space-between;padding:6px 0;font-weight:800;">
            <span>Net Total</span><strong style="color:{C["primary"]};">{sym}{net_tot:,.0f}</strong>
        </div>
    </div>""", unsafe_allow_html=True)

    # Average Table
    days_in_m = calendar.monthrange(sel_dt.year, sel_dt.month)[1]
    st.markdown(f"""
    <div class="card-panel">
        <div class="card-title">Average Spends</div>
        <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid {C["border"]};">
            <span>Day</span><strong class="amt-exp">-{sym}{(cur_exp/days_in_m if days_in_m else 0):,.0f}</strong>
        </div>
        <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid {C["border"]};">
            <span>Week</span><strong class="amt-exp">-{sym}{(cur_exp/4.33):,.0f}</strong>
        </div>
        <div style="display:flex;justify-content:space-between;padding:6px 0;">
            <span>Month</span><strong class="amt-exp">-{sym}{cur_exp:,.0f}</strong>
        </div>
    </div>""", unsafe_allow_html=True)

    # 3. RANKED BREAKDOWNS
    st.markdown('<div class="card-title">Ranked Breakdowns (Descending)</div>', unsafe_allow_html=True)
    if not exp_df.empty:
        exp_df["Abs"] = exp_df["Amount"].abs()
        t1, t2, t3 = st.tabs(["By Category", "By Subcategory", "By Account"])
        with t1:
            cat_grp = exp_df.groupby("Category")["Abs"].sum().reset_index().sort_values("Abs", ascending=False)
            for _, row in cat_grp.iterrows():
                c_name, c_amt = str(row["Category"]), float(row["Abs"])
                pct = (c_amt / cur_exp * 100) if cur_exp > 0 else 0
                st.write(f"{cat_icon(c_name)} **{c_name}:** `{sym}{c_amt:,.0f}` ({pct:.0f}%)")
                st.progress(pct / 100)
        with t2:
            sub_grp = exp_df.groupby("Subcategory")["Abs"].sum().reset_index().sort_values("Abs", ascending=False)
            for _, row in sub_grp.head(10).iterrows():
                s_name, s_amt = str(row["Subcategory"]), float(row["Abs"])
                pct = (s_amt / cur_exp * 100) if cur_exp > 0 else 0
                st.write(f"📂 **{s_name}:** `{sym}{s_amt:,.0f}` ({pct:.0f}%)")
                st.progress(pct / 100)
        with t3:
            acct_grp = exp_df.groupby("Tags")["Abs"].sum().reset_index().sort_values("Abs", ascending=False)
            for _, row in acct_grp.iterrows():
                a_name, a_amt = str(row["Tags"]), float(row["Abs"])
                if not a_name or a_name == "nan": a_name = "Untagged"
                pct = (a_amt / cur_exp * 100) if cur_exp > 0 else 0
                st.write(f"{account_badge_html(a_name)}: `{sym}{a_amt:,.0f}` ({pct:.0f}%)", unsafe_allow_html=True)
                st.progress(pct / 100)

# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN 3 — SPENDS / TRANSACTIONS SCREEN
# ═══════════════════════════════════════════════════════════════════════════════

def screen_transactions():
    df = _load_transactions()
    settings = load_settings()
    sym = settings.get("currency_symbol","₹")

    q = st.text_input("", placeholder="🔍 Search merchant, notes, account, or category...", key="spends_search", label_visibility="collapsed")

    months_opts = get_descending_months(df)
    cats_sorted, sub_map = load_cat_freq()

    c1, c2, c3, c4 = st.columns(4)
    with c1: sel_month = st.selectbox("📅 Period:", months_opts, key="spends_m_dd")
    with c2: sel_acct = st.selectbox("💳 Account:", ["All"] + extract_accounts(df), key="spends_a_dd")
    with c3: sel_pm   = st.selectbox("↕️ Method:", ["All"] + PAYMENT_METHODS, key="spends_pm_dd")
    
    avail_subs = sub_map.get(st.session_state.filter_cat, []) if st.session_state.filter_cat != "All" else []
    with c4: sel_subcat = st.selectbox("📂 Subcategory:", ["All"] + avail_subs, key="spends_subcat_dd")

    filtered = df.copy()
    if not filtered.empty and "Date" in filtered.columns:
        sel_dt = datetime.strptime(sel_month, "%B %Y")
        ms, me = month_range(sel_dt.year, sel_dt.month)
        filtered = filtered[(filtered["Date"].dt.date >= ms) & (filtered["Date"].dt.date <= me)]
    
    filtered = filter_by_account(filtered, sel_acct)
    if sel_pm != "All" and not filtered.empty: filtered = filtered[filtered["PaymentMethod"] == sel_pm]
    if st.session_state.filter_cat != "All" and not filtered.empty: filtered = filtered[filtered["Category"] == st.session_state.filter_cat]
    if sel_subcat != "All" and not filtered.empty: filtered = filtered[filtered["Subcategory"] == sel_subcat]
    if q and not filtered.empty:
        ql = q.lower().strip()
        filtered = filtered[filtered.apply(lambda r: any(ql in str(r.get(col,"")).lower() for col in ["Merchant","Category","Subcategory","Notes","Tags"]), axis=1)]

    tot_sp = abs(filtered[filtered["Amount"] < 0]["Amount"].sum()) if not filtered.empty else 0
    st.markdown(f'<div class="summary-strip"><span>Showing: <b>{sel_month}</b></span><strong class="amt-exp">-{sym}{tot_sp:,.0f} ({len(filtered)} Txns)</strong></div>', unsafe_allow_html=True)

    if not filtered.empty:
        n_rows = st.session_state.spends_rows_n
        page = st.session_state.spends_page
        all_sorted = filtered.sort_values("Date", ascending=False)
        total_pages = max((len(all_sorted) - 1) // n_rows + 1, 1)
        if page > total_pages: page = total_pages
        
        start_idx = (page - 1) * n_rows
        spends_df = all_sorted.iloc[start_idx : start_idx + n_rows]

        # Date-Grouped Transaction Cards
        for day_dt, group in spends_df.groupby(spends_df["Date"].dt.date, sort=False):
            day_str = pd.Timestamp(day_dt).strftime("%d %B %Y %A") if pd.notna(day_dt) else "Transactions"
            day_total = group["Amount"].sum()
            amt_color = C["income_light"] if day_total > 0 else C["expense_light"]
            amt_sign = "+" if day_total > 0 else "-"

            st.markdown(f"""
            <div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid {C["border"]};margin:10px 0 6px;">
                <span style="font-size:0.75rem;font-weight:800;color:{C["muted"]};">{day_str}</span>
                <span style="font-family:'JetBrains Mono',monospace;font-weight:700;font-size:0.8rem;color:{amt_color};">{amt_sign}{sym}{abs(day_total):,.0f}</span>
            </div>""", unsafe_allow_html=True)

            for idx, row in group.iterrows():
                rid = str(row.get("RowID", idx))
                c_ed, c_card = st.columns([0.15, 0.85])
                with c_ed:
                    if st.button("✏️", key=f"sp_ed_{rid}"):
                        st.session_state.edit_txn = row.to_dict(); st.rerun()
                with c_card:
                    notes_html = f'<div class="txn-notes-tag">📝 {row["Notes"]}</div>' if str(row.get("Notes","")).strip() else ""
                    acct_badge = account_badge_html(row.get("Tags","")) if row.get("Tags") else ""
                    ico = cat_icon(row["Category"])
                    is_exp = row["Amount"] < 0
                    amt_cls = "amt-exp" if is_exp else "amt-inc"
                    sign_str = "-" if is_exp else "+"

                    st.markdown(f"""
                    <div class="mobile-card-row">
                        <div class="txn-avatar-circle">{ico}</div>
                        <div style="flex:1;min-width:0;">
                            <div style="font-weight:800;font-size:0.88rem;">{row["Merchant"]}</div>
                            <div style="font-size:0.72rem;color:{C["muted"]};margin-top:2px;">
                                <span class="cat-pill">{row["Category"]}</span> {acct_badge}
                            </div>
                            {notes_html}
                        </div>
                        <div class="{amt_cls}" style="font-size:0.92rem;">{sign_str}{sym}{abs(row["Amount"]):,.0f}</div>
                    </div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN 4 — CATEGORY CHART SCREEN
# ═══════════════════════════════════════════════════════════════════════════════

def screen_categories():
    df_txns = _load_transactions()
    cats_df = load_categories()
    cats_sorted, sub_map = load_cat_freq()

    st.markdown('<div class="card-title">🏷️ Category & Subcategory Manager</div>', unsafe_allow_html=True)

    st.markdown('<div class="card-panel" style="border-left: 4px solid var(--primary);">', unsafe_allow_html=True)
    st.markdown('### 🔄 Move Subcategory to Another Category')

    # Extract ALL unique subcategories from BOTH Categories Sheet & Transactions History
    subs_from_cats = [str(r.get("Subcategory","")).strip() for _, r in cats_df.iterrows() if r.get("Subcategory")]
    subs_from_txns = df_txns["Subcategory"].dropna().astype(str).str.strip().tolist() if not df_txns.empty and "Subcategory" in df_txns.columns else []

    all_subs_set = set(subs_from_cats + subs_from_txns)
    all_subs_flat = sorted([s for s in all_subs_set if s and s.lower() not in ("nan", "none", "")])

    # Map current parent category
    sub_parent_map = {}
    for _, row in cats_df.iterrows():
        sub = str(row.get("Subcategory","")).strip()
        cat = str(row.get("Category","")).strip()
        if sub: sub_parent_map[sub] = cat

    # Fallback parent lookup from transaction history
    if not df_txns.empty and "Subcategory" in df_txns.columns and "Category" in df_txns.columns:
        for _, row in df_txns.iterrows():
            sub = str(row.get("Subcategory","")).strip()
            cat = str(row.get("Category","")).strip()
            if sub and sub not in sub_parent_map and cat:
                sub_parent_map[sub] = cat

    c1, c2 = st.columns(2)
    with c1:
        sel_sub_move = st.selectbox("Select Subcategory:", all_subs_flat if all_subs_flat else ["Groceries"])
        cur_parent = sub_parent_map.get(sel_sub_move, "Others")
        st.info(f"Current Parent Category: **{cur_parent}**")
    with c2:
        target_cat_opts = [c for c in cats_sorted if c != cur_parent]
        target_cat = st.selectbox("Move To Category:", target_cat_opts if target_cat_opts else cats_sorted)

    retro = st.checkbox("Retrospectively update past transactions in Google Sheets", value=True)
    if st.button("💾 Move Subcategory", type="primary", use_container_width=True):
        old_c, updated = _move_subcategory(sel_sub_move, target_cat, retro)
        st.success(f"✅ Moved subcategory **{sel_sub_move}** to **{target_cat}**! Updated {updated} rows."); st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # Add New Category Form
    st.markdown('<div class="card-panel">', unsafe_allow_html=True)
    st.markdown('### ➕ Add New Category or Subcategory')
    with st.form("add_cat_mgr_form", clear_on_submit=True):
        a1, a2 = st.columns(2)
        with a1:
            nc = st.text_input("Category Name", placeholder="e.g. Pet Care")
            nk = st.text_input("Auto-Categorization Keywords (Comma separated)", placeholder="e.g. vet, clinic, dog food")
        with a2:
            ns = st.text_input("Subcategory Name", placeholder="e.g. Vet & Food")
            ni = st.text_input("Emoji Icon", value="📌")
        
        if st.form_submit_button("➕ Create Category Mapping", use_container_width=True, type="primary"):
            if nc.strip() and ns.strip():
                get_ss().worksheet("Categories").append_row([nc.strip(), ns.strip(), nk.strip(), ni.strip()])
                st.cache_data.clear()
                st.success(f"✅ Created {ni} {nc} › {ns}")
                st.rerun()
            else:
                st.error("Enter both Category and Subcategory names.")
    st.markdown('</div>', unsafe_allow_html=True)

    with st.expander("📂 Existing Category Mappings & Keyword Rules", expanded=True):
        if not cats_df.empty:
            st.dataframe(cats_df[["Category", "Subcategory", "Keywords", "Icon"]], use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN 5 — CREATE / ADD TRANSACTION FORM
# ═══════════════════════════════════════════════════════════════════════════════

def screen_add():
    df_all = _load_transactions()
    settings = load_settings()
    sym = settings.get("currency_symbol","₹")
    cats_sorted, sub_map = load_cat_freq()

    st.markdown('<div class="card-title">New Transaction</div>', unsafe_allow_html=True)

    # Segmented Type Selector
    ttype = st.radio("TRANSACTION TYPE", ["Expense", "Income"], horizontal=True)
    is_exp = (ttype == "Expense")

    with st.form("create_txn_form", clear_on_submit=True):
        amount = st.number_input(f"Amount ({sym})", min_value=0.0, step=1.0, format="%.0f")
        merch  = st.text_input("Merchant / Description", placeholder="e.g. Swiggy, Fuel Station, Marudhar Mart")
        
        c1, c2 = st.columns(2)
        with c1:
            sel_cat = st.selectbox("Select Category", cats_sorted)
            subs = sub_map.get(sel_cat, [])
            sel_sub = st.selectbox("Select Subcategory", subs if subs else ["Miscellaneous"])
        with c2:
            existing_accounts = extract_accounts(df_all)
            sel_acct = st.selectbox("Select Account", existing_accounts if existing_accounts else ["HDFC CC 7500"])
            txn_date = st.date_input("Date", value=date.today())

        notes = st.text_input("Notes")

        if st.form_submit_button("✓ Save Transaction", use_container_width=True, type="primary"):
            if amount > 0 and merch.strip():
                signed_amt = -abs(amount) if is_exp else abs(amount)
                _write_txn({
                    "RowID": str(uuid.uuid4())[:8], "Date": txn_date.strftime("%d/%m/%Y"),
                    "Merchant": merch.strip().title(), "Amount": signed_amt,
                    "Type": "Expense" if is_exp else "Income",
                    "Category": sel_cat, "Subcategory": sel_sub, "PaymentMethod": "UPI",
                    "Tags": sel_acct, "Notes": notes, "Source": "manual", "AutoCat": "no"
                })
                st.success(f"✅ Saved {merch} ({sym}{amount:,.0f})"); st.rerun()

    # Statement Upload
    with st.expander("📂 Bank Statement File Importer", expanded=False):
        uploaded = st.file_uploader("Upload CSV / XLSX Passbook Statement", type=["csv","xlsx"])
        if uploaded:
            raw = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
            st.write(f"Detected **{len(raw)}** rows in `{uploaded.name}`")
            st.dataframe(raw.head(3), use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  SCREEN 6 — SETTINGS & TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

def screen_settings():
    cats_df    = load_categories()
    budgets    = load_budgets()
    settings   = load_settings()
    rules_df   = load_email_rules()
    errors_df  = load_parse_errors()

    st.markdown('<div class="card-title">⚙️ Settings Engine</div>', unsafe_allow_html=True)
    
    st.markdown('<div class="card-panel">', unsafe_allow_html=True)
    rn1, rn2 = st.columns([4, 1])
    with rn1: st.write("Queue immediate background execution of `importAll()` in Google Apps Script.")
    with rn2:
        if st.button("▶ Run Now", type="primary", use_container_width=True):
            trigger_run_now(); st.success("✅ Trigger queued!"); st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    with st.expander("💱 Currency & Monthly Budget", expanded=False):
        sym = st.text_input("Currency Symbol", value=settings.get("currency_symbol","₹"))
        m_budget = st.number_input("Monthly Budget Limit", value=float(settings.get("monthly_budget","30000")), min_value=0.0)
        if st.button("Save General Config", type="primary"):
            ws = get_ss().worksheet("Settings")
            all_v = ws.get_all_values()
            upd = {"currency_symbol": sym, "monthly_budget": str(int(m_budget))}
            for k, v in upd.items():
                for i, row in enumerate(all_v[1:], start=2):
                    if row and row[0] == k: ws.update_cell(i, 2, v); break
            st.cache_data.clear(); st.success("✅ Config Saved!"); st.rerun()

    with st.expander("🎯 Set Category Budgets", expanded=False):
        cats = cats_df["Category"].unique().tolist() if not cats_df.empty else []
        bmap = dict(zip(budgets["Category"], budgets["MonthlyBudget"].astype(float))) if not budgets.empty else {}
        new_bmap = {}
        for cat in cats:
            val = st.number_input(f"{cat_icon(cat)} {cat}", value=float(bmap.get(cat,0)), min_value=0.0, step=500.0, key=f"set_b_{cat}")
            if val > 0: new_bmap[cat] = val
        if st.button("Save Budgets", type="primary"):
            ws = get_ss().worksheet("Budgets")
            ws.clear(); ws.append_row(["Category","MonthlyBudget"])
            if new_bmap: ws.append_rows([[c,a] for c,a in new_bmap.items()])
            st.cache_data.clear(); st.success("✅ Budgets saved!"); st.rerun()

    with st.expander("🤖 Edit Keyword Rules", expanded=False):
        kw_updates = {}
        for _, row in cats_df.iterrows():
            key = f"kw_{row['Category']}_{row['Subcategory']}"
            new_kw = st.text_input(f"{row.get('Icon','📌')} {row['Category']} › {row['Subcategory']}", value=row.get("Keywords",""), key=key)
            kw_updates[(row["Category"], row["Subcategory"])] = new_kw
        if st.button("Save Rules", type="primary"):
            ws = get_ss().worksheet("Categories")
            ws.clear(); ws.append_row(HEADERS["Categories"])
            rows_to_save = [[r["Category"], r["Subcategory"], kw_updates.get((r["Category"],r["Subcategory"]), r.get("Keywords","")), r.get("Icon","📌")] for _, r in cats_df.iterrows()]
            ws.append_rows(rows_to_save)
            st.cache_data.clear(); st.success("✅ Rules updated!"); st.rerun()

    with st.expander(f"📧 Email Import Rules ({len(rules_df)})", expanded=False):
        if not rules_df.empty:
            for _, rule in rules_df.iterrows():
                r_nm = str(rule.get("RuleName",""))
                is_active = str(rule.get("Active","TRUE")).upper() in ("TRUE","YES","1")
                st.write(f"**{r_nm}** ({'Active' if is_active else 'Off'})")
                c1, c2 = st.columns(2)
                with c1:
                    if st.button(f"Toggle Active: {r_nm}", key=f"tg_act_{r_nm}"):
                        _update_email_rule(r_nm, {"Active": "FALSE" if is_active else "TRUE"}); st.rerun()
                with c2:
                    if st.button(f"Delete: {r_nm}", key=f"del_rule_{r_nm}"):
                        _delete_email_rule(r_nm); st.rerun()
                st.markdown("---")

    with st.expander("📊 Recent Import Log & Parse Errors", expanded=False):
        log_df = load_importlog()
        if not log_df.empty: st.dataframe(log_df.tail(5), use_container_width=True)
        if not errors_df.empty: st.dataframe(errors_df.tail(5), use_container_width=True)

    with st.expander("🏷️ Merchant Aliases Manager", expanded=False):
        aliases = load_merchant_aliases()
        if aliases:
            for raw, canonical in list(aliases.items())[:15]:
                ac1, ac2, ac3 = st.columns([3, 3, 1])
                with ac1: st.write(f"`{raw}`")
                with ac2: st.write(f"→ **{canonical}**")
                with ac3:
                    if st.button("✕", key=f"del_al_{raw}"): delete_merchant_alias(raw); st.rerun()

    with st.expander("📤 Export Data CSV", expanded=False):
        df_exp = _load_transactions()
        if not df_exp.empty:
            st.download_button("⬇️ Download All Transactions as CSV", data=df_exp.to_csv(index=False).encode("utf-8"), file_name=f"clearspend_{date.today()}.csv", mime="text/csv", use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRYPOINT & FLOATING ACTION BUTTON
# ═══════════════════════════════════════════════════════════════════════════════

def run_setup():
    if not st.session_state.get("setup_ok"):
        try:
            ensure_sheets()
            st.session_state.setup_ok = True
        except Exception as ex:
            st.error(f"Setup failed: {ex}")
            st.stop()

def main():
    init_state()
    run_setup()
    inject_css()
    render_top_bar()

    nav = st.session_state.nav
    if nav == "home": screen_home()
    elif nav == "analytics": screen_analytics()
    elif nav == "transactions": screen_transactions()
    elif nav == "categories": screen_categories()
    elif nav == "add": screen_add()
    elif nav == "settings": screen_settings()

    # Floating Action Button FAB (➕ Quick Add Expense)
    st.markdown('<div class="fab-button-fixed">', unsafe_allow_html=True)
    if st.button("➕ Quick Add", key="global_fab_btn", type="primary"):
        st.session_state.show_quick_add = True
    st.markdown('</div>', unsafe_allow_html=True)

    if st.session_state.get("show_quick_add"): dlg_quick_add()
    if st.session_state.get("edit_txn"): dlg_edit(st.session_state.edit_txn)
    if st.session_state.get("pending_bulk"): dlg_bulk_suggest()
    if st.session_state.get("review_misc_page") and st.session_state.nav == "transactions": dlg_review_misc()

if __name__ == "__main__":
    main()
