// Standard MongoDB fixture for demos and manual runs, seeded into the `rivet`
// database. Unlike the SQL engines, the live Mongo suite self-seeds each test's
// collection through the Rig (tests/common/mongo.rs: seed_int_id / seed_string_id),
// so nothing here is a hard test dependency — this is the Mongo analogue of the
// `seed` binary's benchmark dataset, sized to mirror the SQL `orders`/`users`
// fixture (users=1000, orders=5000).
//
// Run via:  docker compose exec -T mongo mongosh "mongodb://127.0.0.1:27017/rivet?directConnection=true" < dev/mongo/seed.js
// Idempotent: each collection is dropped and rebuilt.

const USERS = 1000;
const ORDERS = 5000;
const BATCH = 5000;

const products = ["widget", "gadget", "sprocket", "cog", "bearing", "gasket"];
const statuses = ["pending", "shipped", "delivered", "cancelled"];
const now = new Date();

function bulkInsert(coll, total, make) {
  coll.drop();
  let batch = [];
  for (let i = 1; i <= total; i++) {
    batch.push(make(i));
    if (batch.length === BATCH) {
      coll.insertMany(batch);
      batch = [];
    }
  }
  if (batch.length) coll.insertMany(batch);
}

bulkInsert(db.users, USERS, (i) => ({
  _id: i,
  name: "user" + i,
  email: "user" + i + "@example.com",
  created_at: now,
}));

bulkInsert(db.orders, ORDERS, (i) => ({
  _id: i,
  user_id: ((i - 1) % USERS) + 1,
  product: products[i % products.length],
  quantity: (i % 9) + 1,
  price: Math.round((i % 500) * 100) / 100,
  status: statuses[i % statuses.length],
  ordered_at: now,
  updated_at: now,
}));

print("seeded rivet.users=" + db.users.countDocuments() + " rivet.orders=" + db.orders.countDocuments());
