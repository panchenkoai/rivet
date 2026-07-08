// Runs once on first init of the auth-enabled `mongo-auth` service (with root
// auth already established by MONGO_INITDB_ROOT_*). Creates a LEAST-PRIVILEGE
// read-only login and seeds a small collection, so the harm-permission live test
// can prove that a login without `clusterMonitor` (no `serverStatus` — the
// source-harm probe) still exports fine, the harm counters just degrade to absent.
db.getSiblingDB("harmdb").createUser({
  user: "reader",
  pwd: "readpass",
  roles: [{ role: "read", db: "harmdb" }],
});
db.getSiblingDB("harmdb").t.insertMany([
  { _id: 1, v: "a" },
  { _id: 2, v: "b" },
  { _id: 3, v: "c" },
]);
