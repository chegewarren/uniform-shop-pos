import os, json, base64, requests, uuid, hashlib
from datetime import datetime
from flask import Flask, request, jsonify, render_template, session, redirect, url_for
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "uniformshop-secret-2024")

SHEET_ID    = os.getenv("SHEET_ID", "13OzCQ27gqNRvzdX0xP1seb8kJdHoevu61gaw9GMysc4")
API_KEY     = os.getenv("GOOGLE_API_KEY")
SHEETS_BASE = "https://sheets.googleapis.com/v4/spreadsheets"
APPS_SCRIPT = os.getenv("APPS_SCRIPT_URL", "https://script.google.com/macros/s/AKfycbymBs07cFuLPeH8pnv5j6lOjGUnyqkhs4Vlf5AW0ILwbkdSQr9IPvI4RKOCg1josNNhaw/exec")

def sheets_get(range_):
    url = f"{SHEETS_BASE}/{SHEET_ID}/values/{range_}?key={API_KEY}"
    r = requests.get(url, timeout=10)
    return r.json()

def script_post(data):
    try:
        r = requests.post(APPS_SCRIPT, json=data, timeout=15, allow_redirects=True)
        return r.json()
    except Exception as e:
        print(f"[SCRIPT ERROR] {e}")
        return {"ok": False, "error": str(e)}

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# ── M-Pesa ────────────────────────────────────────────────────────
CONSUMER_KEY    = os.getenv("MPESA_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("MPESA_CONSUMER_SECRET")
PASSKEY         = os.getenv("MPESA_PASSKEY", "bfb279f9aa9bdbcf158e97dd71a467cd2e0c893059b10f78e6b72ada1ed2c919")
CALLBACK_URL    = os.getenv("CALLBACK_URL", "https://uniform-shop-pos-1.onrender.com/mpesa/callback")
MPESA_ENV       = os.getenv("MPESA_ENV", "sandbox")
BASE_URL = ("https://sandbox.safaricom.co.ke" if MPESA_ENV == "sandbox"
            else "https://api.safaricom.co.ke")

def mpesa_token():
    r = requests.get(
        f"{BASE_URL}/oauth/v1/generate?grant_type=client_credentials",
        auth=(CONSUMER_KEY, CONSUMER_SECRET), timeout=15
    )
    return r.json()["access_token"]

def stk_push(phone, amount, account_ref, description, shortcode):
    token     = mpesa_token()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    password  = base64.b64encode(f"{shortcode}{PASSKEY}{timestamp}".encode()).decode()
    # Determine transaction type
    tx_type = "CustomerBuyGoodsOnline" if len(str(shortcode)) <= 6 else "CustomerPayBillOnline"
    payload = {
        "BusinessShortCode": shortcode,
        "Password":          password,
        "Timestamp":         timestamp,
        "TransactionType":   tx_type,
        "Amount":            int(amount),
        "PartyA":            phone,
        "PartyB":            shortcode,
        "PhoneNumber":       phone,
        "CallBackURL":       CALLBACK_URL,
        "AccountReference":  account_ref,
        "TransactionDesc":   description
    }
    r = requests.post(
        f"{BASE_URL}/mpesa/stkpush/v1/processrequest",
        json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=15
    )
    return r.json()

pending = {}

# ── Auth middleware ───────────────────────────────────────────────
def get_current_user():
    return session.get("user")

def login_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user"):
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated

# ── Auth Routes ───────────────────────────────────────────────────
@app.route("/login")
def login_page():
    if session.get("user"):
        return redirect(url_for("index"))
    return render_template("login.html")

@app.route("/api/signup", methods=["POST"])
def signup():
    data = request.json
    email    = data.get("email", "").strip().lower()
    password = data.get("password", "").strip()
    name     = data.get("name", "").strip()
    phone    = data.get("phone", "").strip()
    business = data.get("business_name", "").strip()
    till     = data.get("till_number", "").strip()
    paybill  = data.get("paybill_number", "").strip()

    if not all([email, password, name, phone, business]):
        return jsonify({"ok": False, "error": "All fields are required"}), 400
    if len(password) < 6:
        return jsonify({"ok": False, "error": "Password must be at least 6 characters"}), 400

    user_id  = str(uuid.uuid4())[:8].upper()
    pwd_hash = hash_password(password)
    created  = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    result = script_post({
        "action":       "signup",
        "user_id":      user_id,
        "business_name": business,
        "name":         name,
        "email":        email,
        "phone":        phone,
        "password":     pwd_hash,
        "till_number":  till,
        "paybill_number": paybill,
        "created_at":   created
    })

    if not result.get("ok"):
        return jsonify({"ok": False, "error": result.get("error", "Signup failed")}), 400

    user = {"id": user_id, "name": name, "email": email, "phone": phone,
            "business_name": business, "till_number": till, "paybill_number": paybill}
    session["user"] = user
    return jsonify({"ok": True, "user": user})

@app.route("/api/login", methods=["POST"])
def login():
    data     = request.json
    email    = data.get("email", "").strip().lower()
    password = data.get("password", "").strip()
    pwd_hash = hash_password(password)

    result = script_post({
        "action":   "login",
        "email":    email,
        "password": pwd_hash
    })

    if result.get("ok"):
        user = result.get("user")
        session["user"] = user
        return jsonify({"ok": True, "user": user})
    else:
        return jsonify({"ok": False, "error": result.get("error", "Invalid email or password")}), 401

@app.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"ok": True})

@app.route("/api/me")
def me():
    user = get_current_user()
    if not user:
        return jsonify({"ok": False, "error": "Not logged in"}), 401
    return jsonify({"ok": True, "user": user})

# ── Main Routes ───────────────────────────────────────────────────
@app.route("/")
@login_required
def index():
    return render_template("index.html")

@app.route("/api/products")
@login_required
def get_products():
    try:
        data = sheets_get("Inventory!A3:H200")
        rows = data.get("values", [])
        products = []
        for i, row in enumerate(rows, start=3):
            while len(row) < 8:
                row.append("")
            name = row[1].strip()
            if not name:
                continue
            try: stock = int(str(row[5]).replace(",","").replace(" ","")) if row[5] else 0
            except: stock = 0
            try: price = int(str(row[4]).replace(",","").replace(" ","")) if row[4] else 0
            except: price = 0
            try: reorder = int(str(row[6]).replace(",","").replace(" ","")) if row[6] else 0
            except: reorder = 0
            products.append({
                "row": i, "id": row[0], "name": name,
                "category": row[2], "price": price,
                "stock": stock, "reorder": reorder,
            })
        return jsonify({"ok": True, "products": products})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/pay", methods=["POST"])
@login_required
def pay():
    data     = request.json
    phone    = data["phone"].strip().replace("+","").replace(" ","")
    if phone.startswith("0"):
        phone = "254" + phone[1:]
    elif not phone.startswith("254"):
        phone = "254" + phone
    product  = data["product"]
    qty      = int(data.get("qty", 1))
    amount   = int(product["price"]) * qty
    customer = data.get("customer_name", "Walk-in")

    if qty > int(product.get("stock", 0)):
        return jsonify({"ok": False,
            "error": f"Not enough stock! Only {product.get('stock', 0)} available."}), 400

    # Use user's till number
    user = get_current_user()
    shortcode = user.get("till_number") or user.get("paybill_number") or os.getenv("MPESA_SHORTCODE", "174379")

    try:
        resp = stk_push(phone=phone, amount=amount,
                        account_ref=product["name"][:12],
                        description=f"Uniform: {product['name']}",
                        shortcode=shortcode)
        if resp.get("ResponseCode") == "0":
            ckid = resp["CheckoutRequestID"]
            pending[ckid] = {
                "phone": phone, "customer": customer,
                "product": product, "qty": qty, "amount": amount,
                "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            }
            return jsonify({"ok": True, "checkout_id": ckid})
        else:
            return jsonify({"ok": False,
                "error": resp.get("errorMessage",
                resp.get("ResponseDescription","M-Pesa error"))}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/mpesa/callback", methods=["POST"])
def mpesa_callback():
    body = request.json or {}
    stk  = body.get("Body", {}).get("stkCallback", {})
    ckid = stk.get("CheckoutRequestID")
    code = stk.get("ResultCode")
    if code == 0 and ckid in pending:
        sale = pending.pop(ckid)
        items = stk.get("CallbackMetadata", {}).get("Item", [])
        mpesa_code = next((i["Value"] for i in items if i["Name"] == "MpesaReceiptNumber"), "N/A")
        _record_sale(sale, mpesa_code)
    elif ckid in pending:
        pending.pop(ckid)
    return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"})

@app.route("/api/cash", methods=["POST"])
@login_required
def cash_payment():
    data     = request.json
    product  = data["product"]
    qty      = int(data.get("qty", 1))
    amount   = int(data.get("amount", int(product["price"]) * qty))
    customer = data.get("customer", "Walk-in")

    if qty > int(product.get("stock", 0)):
        return jsonify({"ok": False,
            "error": f"Not enough stock! Only {product.get('stock', 0)} available."}), 400
    try:
        data_rows = sheets_get("Sales Records!A3:A500")
        rows      = data_rows.get("values", [])
        next_num  = len([r for r in rows if r and r[0] and r[0] != "TOTALS"]) + 1
        txn_id    = f"TXN-{next_num:04d}"

        script_post({
            "action": "add_sale", "txn_id": txn_id,
            "date": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "customer": customer, "item": product["name"],
            "category": product["category"], "qty": qty,
            "price": product["price"], "amount": amount,
            "mpesa_code": "CASH"
        })
        script_post({"action": "deduct_stock", "item": product["name"], "qty": qty})
        print(f"[CASH] {txn_id} — {product['name']} x{qty} = KES {amount}")
        return jsonify({"ok": True, "txn_id": txn_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/status/<ckid>")
def check_status(ckid):
    return jsonify({"status": "pending" if ckid in pending else "completed"})

@app.route("/api/sales")
@login_required
def get_sales():
    try:
        data = sheets_get("Sales Records!A3:L500")
        rows = data.get("values", [])
        headers = ["Transaction ID","Date","Customer Name","Customer Type",
                   "Item","Category","Size","Qty","Unit Price (KES)",
                   "Discount (%)","Total Revenue (KES)","Notes"]
        sales = []
        for row in rows:
            while len(row) < 12:
                row.append("")
            if row[0] and row[0] != "TOTALS":
                sales.append(dict(zip(headers, row)))
        return jsonify({"ok": True, "sales": sales[-50:]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/test_sale", methods=["POST"])
@login_required
def test_sale():
    data = request.json or {}
    result = script_post({
        "action": "add_sale", "txn_id": "TEST-001",
        "date": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "customer": data.get("customer", "Test Customer"),
        "item": data.get("item", "White Shirt"),
        "category": "Tops", "qty": 1, "price": 450,
        "amount": 450, "mpesa_code": "TESTCODE123"
    })
    return jsonify(result)

def _record_sale(sale, mpesa_code):
    try:
        data     = sheets_get("Sales Records!A3:A500")
        rows     = data.get("values", [])
        next_num = len([r for r in rows if r and r[0] and r[0] != "TOTALS"]) + 1
        txn_id   = f"TXN-{next_num:04d}"
        p        = sale["product"]
        script_post({
            "action": "add_sale", "txn_id": txn_id,
            "date": sale["timestamp"], "customer": sale["customer"],
            "item": p["name"], "category": p["category"],
            "qty": sale["qty"], "price": p["price"],
            "amount": sale["amount"], "mpesa_code": mpesa_code
        })
        script_post({"action": "deduct_stock", "item": p["name"], "qty": sale["qty"]})
        print(f"[SALE] {txn_id} — {p['name']} x{sale['qty']} = KES {sale['amount']} | {mpesa_code}")
    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
