#!/usr/bin/env python3
"""Canonical MongoDB seed — the Mongo analogue of seeds/common/*.sql.

Reproduces the benchmark fixture in Python (the project's scripting language;
see dev/bench/smoke.py, dev/cdc/harm_mongo.py), sized to mirror the SQL
`orders`/`users` fixture: rivet.users=150000, rivet.orders=150000. Idempotent —
each collection is dropped and rebuilt.

Run:   RIVET_MONGO_URI="mongodb://127.0.0.1:27017/rivet" python3 seeds/common/mongo.py
Needs: pip install pymongo
"""
import datetime
import os

from pymongo import MongoClient

URI = os.environ.get("RIVET_MONGO_URI", "mongodb://127.0.0.1:27017/rivet")
USERS = int(os.environ.get("RIVET_SEED_USERS", "150000"))
ORDERS = int(os.environ.get("RIVET_SEED_ORDERS", "150000"))
PRODUCTS = ["widget", "gadget", "sprocket", "cog", "bearing", "gasket"]
STATUSES = ["pending", "shipped", "delivered", "cancelled"]


def main() -> None:
    client = MongoClient(URI)
    db = client.get_default_database()  # the `/rivet` in the URI
    now = datetime.datetime.now(datetime.timezone.utc)

    db.users.drop()
    db.users.insert_many(
        [
            {"_id": i, "name": f"user{i}", "email": f"user{i}@example.com", "created_at": now}
            for i in range(1, USERS + 1)
        ]
    )

    db.orders.drop()
    db.orders.insert_many(
        [
            {
                "_id": i,
                "user_id": ((i - 1) % USERS) + 1,
                "product": PRODUCTS[i % len(PRODUCTS)],
                "quantity": (i % 9) + 1,
                "price": round(i % 500, 2),
                "status": STATUSES[i % len(STATUSES)],
                "ordered_at": now,
                "updated_at": now,
            }
            for i in range(1, ORDERS + 1)
        ]
    )

    print(
        f"seeded {db.name}.users={db.users.count_documents({})} "
        f"{db.name}.orders={db.orders.count_documents({})}"
    )


if __name__ == "__main__":
    main()
