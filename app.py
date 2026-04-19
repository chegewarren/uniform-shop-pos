import os, json, base64, requests
from datetime import datetime
from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

SHEET_ID    = os.getenv("SHEET_ID", "13OzCQ27gqNRvzdX0xP1seb8kJdHoevu61gaw9GMysc4")
API_KEY     = os.getenv("GOOGLE_API_KEY")
SHEETS_BASE = "https://sheets.googleapis.com/v4/spreadsheets"

def sheets_get(range_):
    url = f"{SHEETS_BASE}/{SHEET_ID}/values/{range_}?key={API_KEY}"
    r = requests.get(url, timeout=10)
    return r.json()

def sheets_append(range_, values):
    url = f"{SHEETS_BASE}/{SHEET_ID}/values/{range_}:append?valueInputOption=USER_ENTERED&key={API_KEY}"
    r = requests.post(url, json={"values": values}, timeout=10)
    return r.json()

CONSUMER_KEY    = os.getenv("MPESA_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("MPESA_CONSUMER_SECRET")
SHORTCODE       = os.getenv("MPESA_SHORTCODE", "174379")
PASSKEY         = os.getenv("MPESA_PASSKEY", "bfb279f9aa9bdbcf158e97dd71a467cd2e0c893059b10f78e6b72ada1ed2c919")
CALLBACK_URL    = os.getenv("CALLBACK_URL")
MPESA_ENV       = os.getenv("MPESA_ENV", "sandbox")
BASE_URL = ("https://sandbox.safaricom.co.ke" if MPESA_ENV == "sandbox"
            else "https://api.safaricom.co.ke")

def mpesa_token():
    r = requests.get(
        f"{BASE_URL}/oauth/v1/generate?grant_type=client_credentials",
        auth=(CONSUMER_KEY, CONSUMER_SECRET), timeout=15
    )
    return r.json()["access_token"]

def stk_push(phone, amount, account_ref, description):
    token     = mpesa_token()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    password  = base64.b64encode(f"{SHORTCODE}{PASSKEY}{timestamp}".encode()).decode()
    callback  = CALLBACK_URL or "https://uniform-shop-pos-1.onrender.com/mpesa/callback"
    payload = {
        "BusinessShortCode": SHORTCODE,
        "Password":          password,
        "Timestamp":         timestamp,
        "TransactionType":   "CustomerPayBillOnline",
        "Amount":            int(amount),
        "PartyA":            phone,
        "PartyB":            SHORTCODE,
        "PhoneNumber":       phone,
        "CallBackURL":       callback,
        "AccountReference":  account_ref,
        "TransactionDesc":   description
    }
    r = requests.post(
        f"{BASE_URL}/mpesa/stkpush/v1/processrequest",
        json=payload, headers={"Authorization": f"Bearer {token}"}, timeout=15
    )
    return r.json()
pending = {}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/products")
def get_products():
    try:
        data = sheets_get("Inventory!A3:J200")
        rows = data.get("values", [])
        products = []
        for i, row in enumerate(rows, start=3):
            while len(row) < 10:
                row.append("")
            name = row[1].strip()
            if not name:
                continue
            try:
                stock = int(str(row[5]).replace(",","")) if row[5] else 0
            except:
                stock = 0
            try:
                price = int(str(row[4]).replace(",","")) if row[4] else 0
            except:
                price = 0
            try:
                reorder = int(str(row[6]).replace(",","")) if row[6] else 0
            except:
                reorder = 0
            products.append({
                "row": i, "id": row[0], "name": name,
                "category": row[2], "price": price,
                "stock": stock, "reorder": reorder,
            })
        return jsonify({"ok": True, "products": products})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/pay", methods=["POST"])
def pay():
    data    = request.json
    phone   = data["phone"].strip().replace("+","").replace(" ","")
    if phone.startswith("0"):
        phone = "254" + phone[1:]
    elif not phone.startswith("254"):
        phone = "254" + phone
    product  = data["product"]
    qty      = int(data.get("qty", 1))
    amount   = int(product["price"]) * qty
    customer = data.get("customer_name", "Walk-in")
    try:
        resp = stk_push(phone=phone, amount=amount,
                        account_ref=product["name"][:12],
                        description=f"Uniform: {product['name']}")
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

@app.route("/api/status/<ckid>")
def check_status(ckid):
    return jsonify({"status": "pending" if ckid in pending else "completed"})

@app.route("/api/sales")
def get_sales():
    try:
        data = sheets_get("Sales Records!A3:L200")
        rows = data.get("values", [])
        headers = ["Transaction ID","Date","Customer Name","Customer Type",
                   "Item","Category","Size","Qty","Unit Price (KES)",
                   "Discount (%)","Total Revenue (KES)","Notes"]
        sales = []
        for row in rows:
            while len(row) < 12:
                row.append("")
            if row[0]:
                sales.append(dict(zip(headers, row)))
        return jsonify({"ok": True, "sales": sales[-50:]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

def _record_sale(sale, mpesa_code):
    try:
        data     = sheets_get("Sales Records!A3:A500")
        rows     = data.get("values", [])
        next_num = len([r for r in rows if r and r[0]]) + 1
        txn_id   = f"TXN-{next_num:04d}"
        p        = sale["product"]
        sheets_append("Sales Records!A:L", [[
            txn_id, sale["timestamp"], sale["customer"],
            "M-Pesa Customer", p["name"], p["category"],
            "", sale["qty"], p["price"], 0,
            sale["amount"], f"MPesa: {mpesa_code}"
        ]])
        print(f"[SALE] {txn_id} — {p['name']} x{sale['qty']} = KES {sale['amount']} | {mpesa_code}")
    except Exception as e:
        # Deduct stock from Inventory
        inv_data = sheets_get("Inventory!A3:F200")
        inv_rows = inv_data.get("values", [])
        for idx, inv_row in enumerate(inv_rows, start=3):
            while len(inv_row) < 6:
                inv_row.append("")
            if inv_row[1].strip().lower() == p["name"].strip().lower():
                current = int(str(inv_row[5]).replace(",","")) if inv_row[5] else 0
                new_stock = max(0, current - sale["qty"])
                sheets_update(f"Inventory!F{idx}", [[new_stock]])
                break
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
