import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))

from event_gen import CATEGORIES, generate_order  # noqa: E402


def test_event_has_all_fields():
    e = generate_order()
    for key in ["order_id", "customer_id", "category", "city", "payment_method",
                "quantity", "unit_price", "amount", "event_time"]:
        assert key in e
    assert e["category"] in CATEGORIES


def test_amount_matches_quantity_times_price():
    e = generate_order()
    assert abs(e["amount"] - e["quantity"] * e["unit_price"]) < 0.05


def test_bad_event_has_negative_amount():
    assert generate_order(bad=True)["amount"] < 0


def test_order_ids_unique():
    ids = {generate_order()["order_id"] for _ in range(200)}
    assert len(ids) == 200
