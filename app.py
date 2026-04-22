import os, json, base64, requests, uuid, hashlib
from datetime import datetime
from flask import Flask, request, jsonify, render_template, session, redirect, url_for
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "uniformshop-secret-2024")

APPS_SCRIPT = os.getenv("APPS_SCRIPT_URL", "https://script.google.com/macros/s/AKfycbymBs07cFuLPeH8pnv5j6lOjGUnyqkhs4Vlf5AW0ILwbkdSQr9IPvI4RKOCg1josNNhaw/exec")

CONSUMER_KEY    = os.getenv("MPESA_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("MPESA_CONSUMER_SECRET")
PASSKEY         = os.getenv("MPESA_PASSKEY", "bfb279f9aa9bdbcf158e97dd71a467cd2e0c893059b10f78e6b72ada1ed2c919")
CALLBACK_URL    = os.getenv("CALLBACK_URL", "https://uniform-shop-pos-1.onrender.com/mpesa/callback")
MPESA_ENV       = os.getenv("MPESA_ENV", "production")
BASE_URL = ("https://sandbox.safaricom.co.ke" if MPESA_ENV == "sandbox"
            else "https://api.safaricom.co.ke")

def hash_password(p):
    return hashlib.sha256(p.encode()).hexdigest()

def script_post(data):
    try:
        r = requests.post(APPS_SCRIPT, json=data, timeout=15, allow_redirects=True)
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

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

# ── M-Pesa ────────────────────────────────────────────────────────
def mpesa_token():
    r = requests.get(
        f"{BASE_URL}/oauth/v1/generate?grant_type=client_credentials",
        auth=(CONSUMER_KEY, CONSUMER_SECRET), timeout=15
    )
    return r.json()["access_token"]

def stk_push(phone, amount, account_ref, description, shortcode, passkey=None):
    token     = mpesa_token()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    pk        = passkey or PASSKEY
    password  = base64.b64encode(f"{shortcode}{pk}{timestamp}".encode()).decode()
    if MPESA_ENV == "sandbox":
        tx_type = "CustomerPayBillOnline"
    else:
        tx_type = "CustomerBuyGoodsOnline" if len(str(shortcode)) <= 7 else "CustomerPayBillOnline"
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

# ── Auth ──────────────────────────────────────────────────────────
@app.route("/login")
def login_page():
    if session.get("user"):
        return redirect(url_for("index"))
    return render_template("login.html")

@app.route("/api/signup", methods=["POST"])
def signup():
    data        = request.json
    email       = data.get("email","").strip().lower()
    password    = data.get("password","").strip()
    name        = data.get("name","").strip()
    phone       = data.get("phone","").strip()
    business    = data.get("business_name","").strip()
    mpesa_phone = data.get("mpesa_phone","").strip()
    till        = data.get("till_number","").strip()
    paybill     = data.get("paybill_number","").strip()

    if not all([email, password, name, phone, business]):
        return jsonify({"ok": False, "error": "All fields are required"}), 400
    if len(password) < 6:
        return jsonify({"ok": False, "error": "Password must be at least 6 characters"}), 400

    user_id  = str(uuid.uuid4())[:8].upper()
    pwd_hash = hash_password(password)
    created  = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    result = script_post({
        "action": "signup", "user_id": user_id,
        "business_name": business, "name": name,
        "email": email, "phone": phone,
        "password": pwd_hash, "till_number": till,
        "paybill_number": paybill, "created_at": created,
        "mpesa_phone": mpesa_phone
    })

    if not result.get("ok"):
        return jsonify({"ok": False, "error": result.get("error","Signup failed")}), 400

    user = {"id": user_id, "name": name, "email": email, "phone": phone,
            "business_name": business, "mpesa_phone": mpesa_phone,
            "till_number": till, "paybill_number": paybill}
    session["user"] = user
    return jsonify({"ok": True, "user": user})

@app.route("/api/login", methods=["POST"])
def login():
    data     = request.json
    email    = data.get("email","").strip().lower()
    password = data.get("password","").strip()
    result   = script_post({"action":"login","email":email,"password":hash_password(password)})
    if result.get("ok"):
        session["user"] = result["user"]
        return jsonify({"ok": True, "user": result["user"]})
    return jsonify({"ok": False, "error": result.get("error","Invalid email or password")}), 401

@app.route("/api/forgot_password", methods=["POST"])
def forgot_password():
    email    = request.json.get("email","").strip().lower()
    temp_pwd = str(uuid.uuid4())[:8].upper()
    result   = script_post({"action":"reset_password","email":email,"new_password":hash_password(temp_pwd)})
    if result.get("ok"):
        return jsonify({"ok": True, "temp_password": temp_pwd})
    return jsonify({"ok": False, "error": result.get("error","Email not found")}), 404

@app.route("/api/change_password", methods=["POST"])
@login_required
def change_password():
    data = request.json
    user = get_current_user()
    if len(data.get("new_password","")) < 6:
        return jsonify({"ok": False, "error": "Password must be at least 6 characters"}), 400
    check = script_post({"action":"login","email":user["email"],"password":hash_password(data.get("old_password",""))})
    if not check.get("ok"):
        return jsonify({"ok": False, "error": "Current password is incorrect"}), 401
    result = script_post({"action":"reset_password","email":user["email"],"new_password":hash_password(data["new_password"])})
    return jsonify({"ok": result.get("ok", False)})

@app.route("/api/update_profile", methods=["POST"])
@login_required
def update_profile():
    data = request.json
    user = get_current_user()
    result = script_post({
        "action": "update_profile", "email": user["email"],
        "mpesa_phone": data.get("mpesa_phone",""),
        "till_number": data.get("till_number",""),
        "paybill_number": data.get("paybill_number","")
    })
    if result.get("ok"):
        user.update({"mpesa_phone": data.get("mpesa_phone",""),
                     "till_number": data.get("till_number",""),
                     "paybill_number": data.get("paybill_number","")})
        session["user"] = user
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Failed to update"}), 500

@app.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"ok": True})

@app.route("/api/me")
def me():
    user = get_current_user()
    if not user:
        return jsonify({"ok": False}), 401
    return jsonify({"ok": True, "user": user})

# ── Main ──────────────────────────────────────────────────────────
@app.route("/")
@login_required
def index():
    return render_template("index.html")

# ── Inventory ─────────────────────────────────────────────────────
@app.route("/api/products")
@login_required
def get_products():
    user = get_current_user()
    result = script_post({"action": "get_inventory", "user_id": user["id"]})
    if result.get("ok"):
        return jsonify({"ok": True, "products": result.get("products", [])})
    return jsonify({"ok": False, "error": result.get("error","Failed")}), 500

@app.route("/api/inventory/add", methods=["POST"])
@login_required
def add_inventory():
    user = get_current_user()
    data = request.json
    result = script_post({
        "action": "add_inventory", "user_id": user["id"],
        "item_id":   f"INV-{str(uuid.uuid4())[:4].upper()}",
        "name":      data.get("name",""),
        "category":  data.get("category",""),
        "cost":      data.get("cost", 0),
        "price":     data.get("price", 0),
        "stock":     data.get("stock", 0),
        "reorder":   data.get("reorder", 5)
    })
    return jsonify(result)

@app.route("/api/inventory/update", methods=["POST"])
@login_required
def update_inventory():
    user = get_current_user()
    data = request.json
    result = script_post({
        "action": "update_inventory", "user_id": user["id"],
        "item_id": data.get("item_id"),
        "name":    data.get("name"),
        "category":data.get("category"),
        "cost":    data.get("cost"),
        "price":   data.get("price"),
        "stock":   data.get("stock"),
        "reorder": data.get("reorder")
    })
    return jsonify(result)

@app.route("/api/inventory/delete", methods=["POST"])
@login_required
def delete_inventory():
    user = get_current_user()
    result = script_post({
        "action": "delete_inventory",
        "user_id": user["id"],
        "item_id": request.json.get("item_id")
    })
    return jsonify(result)

# ── Expenses ──────────────────────────────────────────────────────
@app.route("/api/expenses")
@login_required
def get_expenses():
    user = get_current_user()
    result = script_post({"action": "get_expenses", "user_id": user["id"]})
    return jsonify(result)

@app.route("/api/expenses/add", methods=["POST"])
@login_required
def add_expense():
    user = get_current_user()
    data = request.json
    result = script_post({
        "action":      "add_expense",
        "user_id":     user["id"],
        "exp_id":      f"EXP-{str(uuid.uuid4())[:4].upper()}",
        "date":        datetime.now().strftime("%d/%m/%Y"),
        "category":    data.get("category",""),
        "description": data.get("description",""),
        "amount":      data.get("amount", 0),
        "payment":     data.get("payment","Cash")
    })
    return jsonify(result)

@app.route("/api/expenses/delete", methods=["POST"])
@login_required
def delete_expense():
    user = get_current_user()
    result = script_post({
        "action":  "delete_expense",
        "user_id": user["id"],
        "exp_id":  request.json.get("exp_id")
    })
    return jsonify(result)

# ── Sales ─────────────────────────────────────────────────────────
@app.route("/api/sales")
@login_required
def get_sales():
    user = get_current_user()
    result = script_post({"action": "get_sales", "user_id": user["id"]})
    return jsonify(result)

@app.route("/api/pay", methods=["POST"])
@login_required
def pay():
    data        = request.json
    phone       = data["phone"].strip().replace("+","").replace(" ","")
    if phone.startswith("0"): phone = "254" + phone[1:]
    elif not phone.startswith("254"): phone = "254" + phone

    product     = data["product"]
    qty         = int(data.get("qty", 1))
    amount      = int(product["price"]) * qty
    customer    = data.get("customer_name","Walk-in")
    destination = data.get("destination","phone")

    if qty > int(product.get("stock",0)):
        return jsonify({"ok": False, "error": f"Only {product.get('stock',0)} in stock!"}), 400

    user = get_current_user()

    if MPESA_ENV == "sandbox":
        shortcode = "174379"
    else:
        if destination == "till":
            shortcode = user.get("till_number","")
        elif destination == "paybill":
            shortcode = user.get("paybill_number","")
        else:
            mp = user.get("mpesa_phone","").replace("+","").replace(" ","")
            if mp.startswith("0"): mp = "254" + mp[1:]
            shortcode = mp

        if not shortcode:
            return jsonify({"ok": False, "error": f"No {destination} number set. Please update in Settings."}), 400

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
                "user_id": user["id"]
            }
            return jsonify({"ok": True, "checkout_id": ckid})
        return jsonify({"ok": False,
            "error": resp.get("errorMessage", resp.get("ResponseDescription","M-Pesa error"))}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/mpesa/callback", methods=["POST"])
def mpesa_callback():
    body = request.json or {}
    stk  = body.get("Body",{}).get("stkCallback",{})
    ckid = stk.get("CheckoutRequestID")
    code = stk.get("ResultCode")
    if code == 0 and ckid in pending:
        sale = pending.pop(ckid)
        items = stk.get("CallbackMetadata",{}).get("Item",[])
        mpesa_code = next((i["Value"] for i in items if i["Name"]=="MpesaReceiptNumber"),"N/A")
        _record_sale(sale, mpesa_code, "mpesa")
    elif ckid in pending:
        pending.pop(ckid)
    return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"})

@app.route("/api/cash", methods=["POST"])
@login_required
def cash_payment():
    data    = request.json
    product = data["product"]
    qty     = int(data.get("qty",1))
    amount  = int(data.get("amount", int(product["price"]) * qty))
    customer= data.get("customer","Walk-in")
    user    = get_current_user()

    if qty > int(product.get("stock",0)):
        return jsonify({"ok": False, "error": f"Only {product.get('stock',0)} in stock!"}), 400
    try:
        txn_id = f"TXN-{str(uuid.uuid4())[:6].upper()}"
        result = script_post({
            "action": "add_sale", "user_id": user["id"],
            "txn_id": txn_id,
            "date": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "customer": customer, "item": product["name"],
            "category": product["category"], "qty": qty,
            "price": product["price"], "amount": amount,
            "mpesa_code": "CASH", "payment_method": "Cash"
        })
        script_post({"action":"deduct_stock","user_id":user["id"],
                     "item": product["name"],"qty": qty})
        return jsonify({"ok": True, "txn_id": txn_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/status/<ckid>")
def check_status(ckid):
    return jsonify({"status":"pending" if ckid in pending else "completed"})

def _record_sale(sale, mpesa_code, method):
    try:
        txn_id = f"TXN-{str(uuid.uuid4())[:6].upper()}"
        p = sale["product"]
        script_post({
            "action": "add_sale", "user_id": sale["user_id"],
            "txn_id": txn_id,
            "date": sale["timestamp"], "customer": sale["customer"],
            "item": p["name"], "category": p["category"],
            "qty": sale["qty"], "price": p["price"],
            "amount": sale["amount"], "mpesa_code": mpesa_code,
            "payment_method": "M-Pesa"
        })
        script_post({"action":"deduct_stock","user_id":sale["user_id"],
                     "item": p["name"],"qty": sale["qty"]})
        print(f"[SALE] {txn_id} — {p['name']} x{sale['qty']} = KES {sale['amount']}")
    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    port = int(os.getenv("PORT",5000))
    app.run(debug=False, host="0.0.0.0", port=port)
