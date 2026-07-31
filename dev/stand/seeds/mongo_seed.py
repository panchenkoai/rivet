#!/usr/bin/env python3
"""Deterministic Mongo seed generator — stdlib only.

Prints one MongoDB Extended-JSON document per line, for piping into
`mongoimport` inside the mongo container (the official images bundle it):

    python3 mongo_seed.py orders 200000 50000 \
      | docker exec -i stand-mongo80-batch-1 \
          mongoimport --quiet -d rivet -c orders --drop

Same value formulas as the SQL seeds (a pure function of the row number n).
Types are pinned with $numberInt/$numberLong so every mongo version stores
identical BSON — the shells don't get a vote (legacy `mongo` would store
doubles, mongosh int32s).

usage: mongo_seed.py {orders|events} COUNT USER_MOD
"""
import sys

STATUSES = ["new", "paid", "shipped", "done", "cancelled"]
EVENTS = ["view", "click", "signup", "login", "logout", "purchase", "refund", "error"]

MS_2021 = 1609459200000  # 2021-01-01T00:00:00Z
MS_2022 = 1640995200000  # 2022-01-01T00:00:00Z


def orders(count: int, user_mod: int) -> None:
    out = sys.stdout
    for n in range(1, count + 1):
        order_date = MS_2021 + (n % 730) * 86400000
        created_at = order_date + (n % 86400) * 1000
        out.write(
            '{"_id":{"$numberInt":"%d"},'
            '"user_id":{"$numberInt":"%d"},'
            '"amount":%.2f,'
            '"status":"%s",'
            '"order_date":{"$date":{"$numberLong":"%d"}},'
            '"created_at":{"$date":{"$numberLong":"%d"}}}\n'
            % (n, (n % user_mod) + 1, ((n * 17) % 500000) / 100,
               STATUSES[n % 5], order_date, created_at)
        )


def events(count: int, user_mod: int) -> None:
    out = sys.stdout
    for n in range(1, count + 1):
        occurred_at = MS_2022 + (n % 31536000) * 1000
        out.write(
            '{"_id":{"$numberLong":"%d"},'
            '"user_id":{"$numberInt":"%d"},'
            '"event_type":"%s",'
            '"payload":{"seq":{"$numberInt":"%d"},"grp":{"$numberInt":"%d"}},'
            '"occurred_at":{"$date":{"$numberLong":"%d"}}}\n'
            % (n, (n % user_mod) + 1, EVENTS[n % 8], n, n % 100, occurred_at)
        )


def main() -> None:
    if len(sys.argv) != 4 or sys.argv[1] not in ("orders", "events"):
        sys.exit(__doc__)
    count, user_mod = int(sys.argv[2]), int(sys.argv[3])
    (orders if sys.argv[1] == "orders" else events)(count, user_mod)


if __name__ == "__main__":
    main()
