"""Fake e-commerce order event generator."""
import random
import uuid
from datetime import datetime, timezone

CATEGORIES = {
    "Electronics": (500, 50000),
    "Fashion": (200, 5000),
    "Grocery": (50, 2000),
    "Books": (100, 1500),
    "Home": (300, 20000),
}
CITIES = ["Jabalpur", "Bhopal", "Indore", "Delhi", "Mumbai", "Bengaluru", "Pune", "Chennai"]
PAYMENTS = ["UPI", "Card", "COD", "NetBanking"]


def generate_order(bad=False):
    """Return one order event. bad=True makes an invalid record (negative amount)."""
    category = random.choice(list(CATEGORIES))
    low, high = CATEGORIES[category]
    quantity = random.randint(1, 4)
    unit_price = round(random.uniform(low, high), 2)
    amount = round(unit_price * quantity, 2)
    if bad:
        amount = -1.0
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": "C%04d" % random.randint(1, 500),
        "category": category,
        "city": random.choice(CITIES),
        "payment_method": random.choice(PAYMENTS),
        "quantity": quantity,
        "unit_price": unit_price,
        "amount": amount,
        "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }
