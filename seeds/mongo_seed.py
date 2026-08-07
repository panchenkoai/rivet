#!/usr/bin/env python3
"""Canonical MongoDB seed — the Mongo analogue of seeds/common/*.sql.

Reproduces the benchmark fixture in Python (the project's scripting language;
see dev/bench/smoke.py, dev/cdc/harm_mongo.py), sized to mirror the SQL
`orders`/`users`/`content_items` fixture: rivet.users=150000,
rivet.orders=150000, rivet.content_items=5000. Idempotent — each collection is
dropped and rebuilt.

`content_items` is the WIDE one, and it is why the count is small: its purpose is
a document whose body dwarfs its key, not volume. Without it Mongo was the only
engine whose blessed path never read a wide row — `GOLDEN_TABLES` asks for three
collections and this seed made two, so every `content_items` cell exported ZERO
rows and reported success, which the gate caught as `0 parquet, manifest=True`.

Run:   RIVET_MONGO_URI="mongodb://127.0.0.1:27017/rivet" python3 seeds/common/mongo.py
Needs: pip install pymongo
"""
import datetime
import os

from pymongo import MongoClient

URI = os.environ.get("RIVET_MONGO_URI", "mongodb://127.0.0.1:27017/rivet")
USERS = int(os.environ.get("RIVET_SEED_USERS", "150000"))
ORDERS = int(os.environ.get("RIVET_SEED_ORDERS", "150000"))
# 5000, matching the size docs/bench/matrix.yaml already names for this
# collection. Wide documents, few of them.
CONTENT = int(os.environ.get("RIVET_SEED_CONTENT_ITEMS", "5000"))
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

    # The wide analogue of the SQL `content_items`: a body and raw_html that
    # dominate the document, plus the nested `metadata` a JSONB column becomes in
    # a document store.
    db.content_items.drop()
    db.content_items.insert_many(
        [
            {
                "_id": i,
                "title": f"item {i}",
                "body": f"body-{i} " * 120,
                "raw_html": f"<p>item {i}</p>" * 60,
                "metadata": {"source": "seed", "rev": i % 7, "tags": ["a", "b", "c"]},
                "author_name": f"author{i % 50}",
                "author_email": f"author{i % 50}@example.com",
                "category": ["news", "blog", "docs"][i % 3],
                "status": ["draft", "published", "archived"][i % 3],
                "view_count": i * 3,
                "word_count": 120,
                "created_at": now,
                "updated_at": now,
            }
            for i in range(1, CONTENT + 1)
        ]
    )

    print(
        f"seeded {db.name}.users={db.users.count_documents({})} "
        f"{db.name}.orders={db.orders.count_documents({})} "
        f"{db.name}.content_items={db.content_items.count_documents({})}"
    )


if __name__ == "__main__":
    main()
