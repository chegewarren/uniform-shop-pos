import os, base64, secrets, hashlib, requests
from datetime import datetime, date
from functools import wraps
from flask import Flask, request, jsonify, render_template, session, redirect
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

load_dotenv()
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

# ══════════════════════════════════════════════════════════════════
#  DATABASE
#  Add a free PostgreSQL database on Render → DATABASE_URL is set
#  automatically as an environment variable.
# ══════════════════════════════════════════════════════════════════
DATABASE_URL = os.getenv("DATABASE_URL", "")

@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id              SERIAL PRIMARY KEY,
                email           TEXT UNIQUE NOT NULL,
                password_hash   TEXT NOT NULL,
                name            TEXT NOT NULL,
                business_name   TEXT NOT NULL,
                phone           TEXT,
                mpesa_phone     TEXT,
                till_number     TEXT,
                paybill_number  TEXT,
                mpesa_shortcode TEXT,
                mpesa_passkey   TEXT,
                created_at      TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id         SERIAL PRIMARY KEY,
                user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
                name       TEXT NOT NULL,
                category   TEXT DEFAULT 'General',
                price      INTEGER NOT NULL DEFAULT 0,
                stock      INTEGER NOT NULL DEFAULT 0,
                reorder    INTEGER NOT NULL DEFAULT 5,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                id           SERIAL PRIMARY KEY,
                user_id      INTEGER REFERENCES users(id) ON DELETE CASCADE,
                txn_id       TEXT NOT NULL,
                customer     TEXT DEFAULT 'Walk-in',
                mpesa_code   TEXT,
                total_amount INTEGER NOT NULL,
                phone        TEXT,
                created_at   TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sale_items (
                id         SERIAL PRIMARY KEY,
                sale_id    INTEGER REFERENCES sales(id) ON DELETE CASCADE,
                product_id INTEGER REFERENCES products(id) ON DELETE SET NULL,
                name       TEXT NOT NULL,
                category   TEXT,
                price      INTEGER NOT NULL,
                qty        INTEGER NOT NULL,
                subtotal   INTEGER NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id          SERIAL PRIMARY KEY,
                user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
                description TEXT NOT NULL,
                amount      INTEGER NOT NULL,
                category    TEXT DEFAULT 'General',
                note        TEXT DEFAULT '',
                created_at  TIMESTAMP DEFAULT NOW()
            )
        """)

try:
    init_db()
    print("[DB] Tables ready")
except Exception as e:
    print(f"[DB ERROR] {e}")


# ══════════════════════════════════════════════════════════════════
#  AUTH
# ══════════════════════════════════════════════════════════════════
def hash_password(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def get_current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE id=%s", (uid,))
        return cur.fetchone()

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_id"):
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated

def api_login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_id"):
            return jsonify({"ok": False, "error": "Not logged in"}), 401
        return f(*args, **kwargs)
    return decorated


# ══════════════════════════════════════════════════════════════════
#  M-PESA
# ══════════════════════════════════════════════════════════════════
CONSUMER_KEY    = os.getenv("MPESA_CONSUMER_KEY",    "")
CONSUMER_SECRET = os.getenv("MPESA_CONSUMER_SECRET", "")
MPESA_ENV       = os.getenv("MPESA_ENV", "sandbox")
APP_URL         = os.getenv("APP_URL", "https://uniform-shop-pos-1.onrender.com")
CALLBACK_URL    = f"{APP_URL}/mpesa/callback"
BASE_URL        = ("https://sandbox.safaricom.co.ke" if MPESA_ENV == "sandbox"
                   else "https://api.safaricom.co.ke")

_SANDBOX_SHORTCODE = "174379"
_SANDBOX_PASSKEY   = "bfb279f9aa9bdbcf158e97dd71a467cd2e0c893059b10f78e6b72ada1ed2c919"
_token_cache = {"token": None, "expires": 0}

def mpesa_token():
    now = datetime.now().timestamp()
    if _token_cache["token"] and now < _token_cache["expires"] - 60:
        return _token_cache["token"]
    r = requests.get(
        f"{BASE_URL}/oauth/v1/generate?grant_type=client_credentials",
        auth=(CONSUMER_KEY, CONSUMER_SECRET), timeout=15
    )
    r.raise_for_status()
    data = r.json()
    _token_cache["token"]   = data["access_token"]
    _token_cache["expires"] = now + int(data.get("expires_in", 3600))
    return _token_cache["token"]

def format_phone(raw):
    p = str(raw).strip().replace("+","").replace(" ","").replace("-","")
    if p.startswith("0") and len(p) == 10:  return "254" + p[1:]
    if p.startswith("7") and len(p) == 9:   return "254" + p
    if p.startswith("254") and len(p) == 12: return p
    return "254" + p.lstrip("0")

def resolve_mode(user):
    till    = (user.get("till_number")    or "").strip()
    paybill = (user.get("paybill_number") or "").strip()
    phone   = (user.get("mpesa_phone")    or "").strip()
    passkey = (user.get("mpesa_passkey")  or "").strip()
    if MPESA_ENV == "sandbox":
        return {"mode":"sandbox","shortcode":_SANDBOX_SHORTCODE,
                "passkey":_SANDBOX_PASSKEY,"party_b":_SANDBOX_SHORTCODE,
                "tx_type":"CustomerPayBillOnline","acct_ref":None}
    if till:
        return {"mode":"till","shortcode":till,
                "passkey":passkey or _SANDBOX_PASSKEY,
                "party_b":till,"tx_type":"CustomerBuyGoodsOnline","acct_ref":None}
    if paybill:
        return {"mode":"paybill","shortcode":paybill,
                "passkey":passkey or _SANDBOX_PASSKEY,
                "party_b":paybill,"tx_type":"CustomerPayBillOnline","acct_ref":None}
    if phone:
        proxy = (user.get("mpesa_shortcode") or "").strip() or _SANDBOX_SHORTCODE
        return {"mode":"phone","shortcode":proxy,
                "passkey":passkey or _SANDBOX_PASSKEY,
                "party_b":proxy,"tx_type":"CustomerPayBillOnline",
                "acct_ref":format_phone(phone)}
    raise ValueError("No M-Pesa payment method configured. Update your account settings.")

def stk_push(user, customer_phone, amount, product_name):
    mode      = resolve_mode(user)
    token     = mpesa_token()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    password  = base64.b64encode(
        f"{mode['shortcode']}{mode['passkey']}{timestamp}".encode()
    ).decode()
    acct_ref  = mode["acct_ref"] or product_name[:12]
    payload   = {
        "BusinessShortCode": mode["shortcode"],
        "Password":          password,
        "Timestamp":         timestamp,
        "TransactionType":   mode["tx_type"],
        "Amount":            max(1, int(amount)),
        "PartyA":            customer_phone,
        "PartyB":            mode["party_b"],
        "PhoneNumber":       customer_phone,
        "CallBackURL":       CALLBACK_URL,
        "AccountReference":  acct_ref,
        "TransactionDesc":   "Uniform sale",
    }
    print(f"[STK] mode={mode['mode']} phone={customer_phone} amount={amount}")
    r = requests.post(
        f"{BASE_URL}/mpesa/stkpush/v1/processrequest",
        json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=15
    )
    resp = r.json()
    print(f"[STK] response={resp}")
    return resp

def stk_query(user, checkout_request_id):
    mode      = resolve_mode(user)
    token     = mpesa_token()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    password  = base64.b64encode(
        f"{mode['shortcode']}{mode['passkey']}{timestamp}".encode()
    ).decode()
    r = requests.post(
        f"{BASE_URL}/mpesa/stkpushquery/v1/query",
        json={"BusinessShortCode":mode["shortcode"],"Password":password,
              "Timestamp":timestamp,"CheckoutRequestID":checkout_request_id},
        headers={"Authorization": f"Bearer {token}"}, timeout=15
    )
    resp = r.json()
    print(f"[QUERY] {checkout_request_id}: {resp}")
    return resp

pending = {}


# ══════════════════════════════════════════════════════════════════
#  PAGE ROUTES
# ══════════════════════════════════════════════════════════════════
@app.route("/login")
def login_page():
    if session.get("user_id"):
        return redirect("/")
    return render_template("login.html")

@app.route("/")
@login_required
def index():
    return render_template("index.html")

@app.route("/sales")
@login_required
def sales_page():
    return render_template("sales.html")

@app.route("/expenses")
@login_required
def expenses_page():
    return render_template("expenses.html")

@app.route("/inventory")
@login_required
def inventory_page():
    return render_template("inventory.html")


# ══════════════════════════════════════════════════════════════════
#  AUTH API
# ══════════════════════════════════════════════════════════════════
@app.route("/api/signup", methods=["POST"])
def api_signup():
    d        = request.json or {}
    email    = (d.get("email")    or "").strip().lower()
    password = (d.get("password") or "").strip()
    name     = (d.get("name")     or "").strip()
    business = (d.get("business_name") or "").strip()

    if not all([email, password, name, business]):
        return jsonify({"ok":False,"error":"Please fill in all required fields"}), 400
    if len(password) < 6:
        return jsonify({"ok":False,"error":"Password must be at least 6 characters"}), 400

    mpesa_phone    = (d.get("mpesa_phone")    or "").strip()
    till_number    = (d.get("till_number")    or "").strip()
    paybill_number = (d.get("paybill_number") or "").strip()
    if not any([mpesa_phone, till_number, paybill_number]):
        return jsonify({"ok":False,
            "error":"Please provide at least one M-Pesa payment method"}), 400

    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO users
                  (email,password_hash,name,business_name,phone,
                   mpesa_phone,till_number,paybill_number)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (email, hash_password(password), name, business,
                  (d.get("phone") or "").strip(),
                  mpesa_phone, till_number, paybill_number))
            uid = cur.fetchone()["id"]
        session["user_id"] = uid
        return jsonify({"ok": True})
    except psycopg2.errors.UniqueViolation:
        return jsonify({"ok":False,"error":"An account with this email already exists"}), 400
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}), 500

@app.route("/api/login", methods=["POST"])
def api_login():
    d        = request.json or {}
    email    = (d.get("email")    or "").strip().lower()
    password = (d.get("password") or "").strip()
    if not email or not password:
        return jsonify({"ok":False,"error":"Please enter email and password"}), 400
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE email=%s", (email,))
        user = cur.fetchone()
    if not user or user["password_hash"] != hash_password(password):
        return jsonify({"ok":False,"error":"Incorrect email or password"}), 401
    session["user_id"] = user["id"]
    return jsonify({"ok": True})

@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})

@app.route("/api/forgot_password", methods=["POST"])
def api_forgot_password():
    email = (request.json or {}).get("email","").strip().lower()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE email=%s", (email,))
        row = cur.fetchone()
    if not row:
        return jsonify({"ok":False,"error":"No account found with that email"}), 404
    temp_pw = secrets.token_hex(4).upper()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET password_hash=%s WHERE email=%s",
                    (hash_password(temp_pw), email))
    return jsonify({"ok":True,"temp_password":temp_pw})

@app.route("/api/me")
@api_login_required
def api_me():
    user = get_current_user()
    mode = ("Till" if user.get("till_number") else
            "Paybill" if user.get("paybill_number") else
            "Phone" if user.get("mpesa_phone") else "Not set")
    return jsonify({"ok":True,"user":{
        "name":          user["name"],
        "business_name": user["business_name"],
        "email":         user["email"],
        "mpesa_mode":    mode,
    }})


# ══════════════════════════════════════════════════════════════════
#  PRODUCTS / INVENTORY API
# ══════════════════════════════════════════════════════════════════
@app.route("/api/products")
@api_login_required
def get_products():
    user = get_current_user()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM products WHERE user_id=%s ORDER BY category, name
        """, (user["id"],))
        rows = cur.fetchall()
    return jsonify({"ok":True,"products":[dict(r) for r in rows]})

@app.route("/api/products", methods=["POST"])
@api_login_required
def add_product():
    user = get_current_user()
    d    = request.json or {}
    name = (d.get("name") or "").strip()
    if not name:
        return jsonify({"ok":False,"error":"Product name is required"}), 400
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO products (user_id,name,category,price,stock,reorder)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING *
        """, (user["id"], name,
              d.get("category","General"),
              int(d.get("price",0)),
              int(d.get("stock",0)),
              int(d.get("reorder",5))))
        row = dict(cur.fetchone())
    return jsonify({"ok":True,"product":row})

@app.route("/api/products/<int:pid>", methods=["PUT"])
@api_login_required
def update_product(pid):
    user = get_current_user()
    d    = request.json or {}
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE products SET name=%s,category=%s,price=%s,stock=%s,reorder=%s
            WHERE id=%s AND user_id=%s RETURNING *
        """, (d.get("name"), d.get("category","General"),
              int(d.get("price",0)), int(d.get("stock",0)),
              int(d.get("reorder",5)), pid, user["id"]))
        row = cur.fetchone()
    if not row:
        return jsonify({"ok":False,"error":"Product not found"}), 404
    return jsonify({"ok":True,"product":dict(row)})

@app.route("/api/products/<int:pid>", methods=["DELETE"])
@api_login_required
def delete_product(pid):
    user = get_current_user()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM products WHERE id=%s AND user_id=%s", (pid, user["id"]))
    return jsonify({"ok":True})


# ══════════════════════════════════════════════════════════════════
#  PAYMENT API
# ══════════════════════════════════════════════════════════════════
@app.route("/api/pay", methods=["POST"])
@api_login_required
def pay():
    user = get_current_user()
    try:
        d        = request.json
        phone    = format_phone(d["phone"])
        customer = d.get("customer") or "Walk-in"
        items    = d.get("items", [])
        amount   = int(d.get("total") or
                       sum(int(i["product"]["price"]) * int(i["qty"]) for i in items))
        first    = items[0]["product"]["name"] if items else "Uniforms"

        resp = stk_push(user=user, customer_phone=phone,
                        amount=amount, product_name=first)

        if str(resp.get("ResponseCode")) == "0":
            ckid = resp["CheckoutRequestID"]
            pending[ckid] = {
                "user_id":   user["id"],
                "phone":     phone,
                "customer":  customer,
                "items":     items,
                "amount":    amount,
                "timestamp": datetime.now(),
                "status":    "pending",
            }
            return jsonify({"ok":True,"checkout_id":ckid})
        else:
            err = (resp.get("errorMessage") or resp.get("ResponseDescription")
                   or resp.get("ResultDesc") or "M-Pesa request failed.")
            return jsonify({"ok":False,"error":err}), 400
    except ValueError as e:
        return jsonify({"ok":False,"error":str(e)}), 400
    except Exception as e:
        print(f"[PAY ERROR] {e}")
        return jsonify({"ok":False,"error":str(e)}), 500

@app.route("/mpesa/callback", methods=["POST"])
def mpesa_callback():
    body = request.json or {}
    print(f"[CALLBACK] {body}")
    stk  = body.get("Body",{}).get("stkCallback",{})
    ckid = stk.get("CheckoutRequestID")
    code = stk.get("ResultCode")
    if ckid in pending:
        entry = pending[ckid]
        if code == 0:
            items = stk.get("CallbackMetadata",{}).get("Item",[])
            mpesa_code = next((i["Value"] for i in items
                               if i["Name"]=="MpesaReceiptNumber"), "N/A")
            entry["status"] = "completed"
            _record_sale(entry, mpesa_code)
        else:
            entry["status"] = "failed"
    return jsonify({"ResultCode":0,"ResultDesc":"Accepted"})

@app.route("/api/status/<ckid>")
@api_login_required
def check_status(ckid):
    entry = pending.get(ckid)
    if not entry:
        return jsonify({"status":"completed"})
    if entry["status"] == "completed":
        return jsonify({"status":"completed"})
    if entry["status"] == "failed":
        pending.pop(ckid, None)
        return jsonify({"status":"failed","reason":"Payment cancelled or failed"})
    # Fallback — query Safaricom directly
    try:
        user = get_current_user()
        if user:
            q    = stk_query(user, ckid)
            code = q.get("ResultCode")
            if code == 0:
                entry["status"] = "completed"
                _record_sale(entry, q.get("MpesaReceiptNumber","QUERY-OK"))
                return jsonify({"status":"completed"})
            if code in (1032, 1, 17, 2001):
                entry["status"] = "failed"
                pending.pop(ckid, None)
                return jsonify({"status":"failed",
                                "reason":q.get("ResultDesc","Payment failed")})
    except Exception as e:
        print(f"[QUERY ERROR] {e}")
    return jsonify({"status":"pending"})

def _record_sale(entry, mpesa_code):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            # Generate transaction ID
            cur.execute("SELECT COUNT(*) as c FROM sales WHERE user_id=%s",
                        (entry["user_id"],))
            n      = cur.fetchone()["c"] + 1
            txn_id = f"TXN-{n:04d}"
            ts     = entry["timestamp"]

            cur.execute("""
                INSERT INTO sales (user_id,txn_id,customer,mpesa_code,total_amount,phone,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (entry["user_id"], txn_id, entry["customer"],
                  mpesa_code, entry["amount"], entry["phone"], ts))
            sale_id = cur.fetchone()["id"]

            for item in entry["items"]:
                p        = item["product"]
                qty      = int(item["qty"])
                subtotal = int(p["price"]) * qty
                cur.execute("""
                    INSERT INTO sale_items
                      (sale_id,product_id,name,category,price,qty,subtotal)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                """, (sale_id, p.get("id"), p["name"],
                      p.get("category",""), p["price"], qty, subtotal))
                if p.get("id"):
                    cur.execute("""
                        UPDATE products SET stock=GREATEST(stock-%s,0)
                        WHERE id=%s AND user_id=%s
                    """, (qty, p["id"], entry["user_id"]))

        print(f"[SALE] {txn_id} | KES {entry['amount']} | {mpesa_code}")
    except Exception as e:
        print(f"[SALE ERROR] {e}")


# ══════════════════════════════════════════════════════════════════
#  SALES RECORDS API
# ══════════════════════════════════════════════════════════════════
@app.route("/api/sales")
@api_login_required
def get_sales():
    user   = get_current_user()
    period = request.args.get("period", "daily")
    year   = request.args.get("year",  date.today().year)
    month  = request.args.get("month", date.today().month)
    day    = request.args.get("date",  date.today().isoformat())

    with get_db() as conn:
        cur = conn.cursor()
        base = """
            SELECT s.id, s.txn_id, s.customer, s.mpesa_code,
                   s.total_amount, s.phone, s.created_at,
                   json_agg(json_build_object(
                     'name',si.name,'category',si.category,
                     'qty',si.qty,'price',si.price,'subtotal',si.subtotal
                   )) AS items
            FROM sales s
            LEFT JOIN sale_items si ON si.sale_id = s.id
            WHERE s.user_id=%s
        """
        if period == "daily":
            cur.execute(base + " AND DATE(s.created_at)=%s GROUP BY s.id ORDER BY s.created_at DESC",
                        (user["id"], day))
        elif period == "monthly":
            cur.execute(base + """ AND EXTRACT(YEAR FROM s.created_at)=%s
                          AND EXTRACT(MONTH FROM s.created_at)=%s
                          GROUP BY s.id ORDER BY s.created_at DESC""",
                        (user["id"], year, month))
        elif period == "yearly":
            cur.execute(base + " AND EXTRACT(YEAR FROM s.created_at)=%s GROUP BY s.id ORDER BY s.created_at DESC",
                        (user["id"], year))
        rows = cur.fetchall()

    sales            = [dict(r) for r in rows]
    total_revenue    = sum(r["total_amount"] for r in rows)
    total_txns       = len(rows)
    return jsonify({"ok":True,"sales":sales,
                    "summary":{"total_revenue":total_revenue,"total_transactions":total_txns}})


# ══════════════════════════════════════════════════════════════════
#  EXPENSES API
# ══════════════════════════════════════════════════════════════════
@app.route("/api/expenses")
@api_login_required
def get_expenses():
    user   = get_current_user()
    period = request.args.get("period","daily")
    year   = request.args.get("year",  date.today().year)
    month  = request.args.get("month", date.today().month)
    day    = request.args.get("date",  date.today().isoformat())
    with get_db() as conn:
        cur = conn.cursor()
        if period == "daily":
            cur.execute("SELECT * FROM expenses WHERE user_id=%s AND DATE(created_at)=%s ORDER BY created_at DESC",
                        (user["id"], day))
        elif period == "monthly":
            cur.execute("""SELECT * FROM expenses WHERE user_id=%s
                AND EXTRACT(YEAR FROM created_at)=%s AND EXTRACT(MONTH FROM created_at)=%s
                ORDER BY created_at DESC""", (user["id"], year, month))
        elif period == "yearly":
            cur.execute("SELECT * FROM expenses WHERE user_id=%s AND EXTRACT(YEAR FROM created_at)=%s ORDER BY created_at DESC",
                        (user["id"], year))
        rows  = cur.fetchall()
        total = sum(r["amount"] for r in rows)
    return jsonify({"ok":True,"expenses":[dict(r) for r in rows],
                    "summary":{"total":total,"count":len(rows)}})

@app.route("/api/expenses", methods=["POST"])
@api_login_required
def add_expense():
    user = get_current_user()
    d    = request.json or {}
    desc = (d.get("description") or "").strip()
    amt  = int(d.get("amount", 0))
    if not desc or amt <= 0:
        return jsonify({"ok":False,"error":"Description and amount are required"}), 400
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO expenses (user_id,description,amount,category,note)
            VALUES (%s,%s,%s,%s,%s) RETURNING *
        """, (user["id"], desc, amt, d.get("category","General"), d.get("note","")))
        row = dict(cur.fetchone())
    return jsonify({"ok":True,"expense":row})

@app.route("/api/expenses/<int:eid>", methods=["DELETE"])
@api_login_required
def delete_expense(eid):
    user = get_current_user()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM expenses WHERE id=%s AND user_id=%s", (eid, user["id"]))
    return jsonify({"ok":True})


# ══════════════════════════════════════════════════════════════════
#  SETTINGS API
# ══════════════════════════════════════════════════════════════════
@app.route("/api/settings", methods=["GET"])
@api_login_required
def get_settings():
    user = get_current_user()
    safe = {k:v for k,v in dict(user).items() if k != "password_hash"}
    return jsonify({"ok":True,"settings":safe})

@app.route("/api/settings", methods=["POST"])
@api_login_required
def update_settings():
    user   = get_current_user()
    d      = request.json or {}
    fields = ["name","business_name","phone","mpesa_phone",
              "till_number","paybill_number","mpesa_shortcode","mpesa_passkey"]
    updates = {f:(d.get(f) or "").strip() for f in fields if f in d}
    if d.get("new_password"):
        if len(d["new_password"]) < 6:
            return jsonify({"ok":False,"error":"Password must be at least 6 characters"}), 400
        updates["password_hash"] = hash_password(d["new_password"])
    if not updates:
        return jsonify({"ok":True})
    set_clause = ", ".join(f"{k}=%s" for k in updates)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE users SET {set_clause} WHERE id=%s",
                    (*updates.values(), user["id"]))
    return jsonify({"ok":True})


# ══════════════════════════════════════════════════════════════════
#  HEALTH
# ══════════════════════════════════════════════════════════════════
@app.route("/health")
def health():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) as c FROM users")
            users = cur.fetchone()["c"]
        db_ok = True
    except Exception as e:
        db_ok  = False
        users  = str(e)
    return jsonify({"ok":db_ok,"mpesa_env":MPESA_ENV,
                    "callback":CALLBACK_URL,"users":users})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
