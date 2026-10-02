"""End-to-end checks for the per-message send/receive tracking feature."""

import threading

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

# Both directions; same message "m" is sent twice on A->B so the records must
# stay distinct (repeated sends are never merged).
P = {
    "A": {"states": [0, 1, 2], "initial": 0, "finals": [2]},
    "B": {"states": [0, 1, 2], "initial": 0, "finals": [2]},
    "transitions": [
        {"id": "sAB1", "from": "A", "to": "B", "action": "send", "message": "m",
         "source": 0, "target": 0},
        {"id": "rAB", "from": "A", "to": "B", "action": "receive", "message": "m",
         "source": 0, "target": 1},
        {"id": "sAB2", "from": "A", "to": "B", "action": "send", "message": "m",
         "source": 1, "target": 1},
        {"id": "sBA", "from": "B", "to": "A", "action": "send", "message": "n",
         "source": 0, "target": 0},
        {"id": "rBA", "from": "B", "to": "A", "action": "receive", "message": "n",
         "source": 0, "target": 1},
    ],
}

# Simple one-send/one-receive protocol, completable.
Q = {
    "A": {"states": [0, 1], "initial": 0, "finals": [1]},
    "B": {"states": [0, 1], "initial": 0, "finals": [1]},
    "transitions": [
        {"id": "t1", "from": "A", "to": "B", "action": "send", "message": "m",
         "source": 0, "target": 1},
        {"id": "t4", "from": "A", "to": "B", "action": "receive", "message": "m",
         "source": 0, "target": 1},
    ],
}

failures = []


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f" :: {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def create(protocol):
    r = client.post("/v1/sessions", json=protocol)
    assert r.status_code == 201, r.text
    return r.json()["id"]


def run(sid, tid, ver):
    return client.post(f"/v1/sessions/{sid}/transitions",
                       json={"transition_id": tid, "expected_version": ver})


def track(sid, ver):
    return client.post(f"/v1/sessions/{sid}/message-tracks",
                       json={"expected_version": ver})


# 1. empty trace -> two empty lists, session id and version echoed
sid = create(P)
r = track(sid, 0)
check("empty trace 200", r.status_code == 200, r.text)
b = r.json()
check("empty echo id/version", b.get("session_id") == sid and b.get("version") == 0, b)
check("empty lists", b.get("A_to_B") == [] and b.get("B_to_A") == [], b)
check("empty only two directions", set(b) == {"session_id", "version", "A_to_B", "B_to_A"}, b)

# 2. full trace: sAB1(1) sBA(2) rBA(3) rAB(4) sAB2(5)
for ver, tid in enumerate(("sAB1", "sBA", "rBA", "rAB", "sAB2")):
    rr = run(sid, tid, ver)
    assert rr.status_code == 200, rr.text

r = track(sid, 5)
check("track 200", r.status_code == 200, r.text)
b = r.json()
check("echo id/version after steps", b["session_id"] == sid and b["version"] == 5, b)

ab = b["A_to_B"]
ba = b["B_to_A"]
check("two A_to_B sends kept separate", len(ab) == 2, ab)
check("one B_to_A send", len(ba) == 1, ba)
check("A_to_B ordered by send step", [x["send_step"] for x in ab] == [1, 5], ab)

first = ab[0]
check("record field set", set(first) == {
    "direction", "message", "send_transition_id", "send_step",
    "receive_transition_id", "receive_step", "interval_steps"}, first)
check("first send direction/message", first["direction"] == "A_to_B"
      and first["message"] == "m", first)
check("first send ids", first["send_transition_id"] == "sAB1"
      and first["send_step"] == 1, first)
check("first receive ids", first["receive_transition_id"] == "rAB"
      and first["receive_step"] == 4, first)
# Between steps 1 and 4 lie steps 2 and 3; the two endpoints are excluded.
check("first interval excludes both steps", first["interval_steps"] == 2, first)

second = ab[1]
check("duplicate message is a separate record",
      second["message"] == "m"
      and second["send_transition_id"] == "sAB2"
      and second["send_step"] == 5, second)
check("unreceived -> three nulls",
      second["receive_transition_id"] is None
      and second["receive_step"] is None
      and second["interval_steps"] is None, second)

b_rec = ba[0]
check("B_to_A pairing",
      b_rec == {"direction": "B_to_A", "message": "n",
                "send_transition_id": "sBA", "send_step": 2,
                "receive_transition_id": "rBA", "receive_step": 3,
                "interval_steps": 0}, b_rec)

# 3. adjacent send/receive -> interval 0
sq = create(Q)
run(sq, "t1", 0)
run(sq, "t4", 1)
b = track(sq, 2).json()
check("adjacent interval 0", b["A_to_B"][0]["interval_steps"] == 0
      and b["A_to_B"][0]["receive_step"] == 2, b["A_to_B"])
check("adjacent B empty", b["B_to_A"] == [], b)

# send only -> unreceived record
so = create(Q)
run(so, "t1", 0)
b = track(so, 1).json()
check("pending send unreceived",
      b["A_to_B"][0]["send_transition_id"] == "t1"
      and b["A_to_B"][0]["receive_step"] is None
      and b["A_to_B"][0]["interval_steps"] is None, b)

# 4. unknown session -> 404
r = client.post("/v1/sessions/does-not-exist/message-tracks",
                json={"expected_version": 0})
check("unknown 404", r.status_code == 404, (r.status_code, r.text))
check("unknown body", r.json() == {"detail": "unknown session"}, r.json())

# 5. stale expected_version -> 409 with current_version
r = track(sid, 0)
check("stale 409", r.status_code == 409, (r.status_code, r.text))
check("stale current_version",
      r.json() == {"detail": {"error": "version_conflict", "current_version": 5}},
      r.json())
r = track(sid, 99)
check("future version 409", r.status_code == 409 and
      r.json()["detail"]["current_version"] == 5, r.text)

# unknown takes precedence over stale (path 404 even with a wrong version)
r = client.post("/v1/sessions/does-not-exist/message-tracks",
                json={"expected_version": 99})
check("unknown beats stale 404", r.status_code == 404, r.status_code)

# 6. body validation -> 422
r = client.post(f"/v1/sessions/{sid}/message-tracks", json={})
check("missing version 422", r.status_code == 422, r.status_code)
r = client.post(f"/v1/sessions/{sid}/message-tracks", json={"expected_version": -1})
check("negative version 422", r.status_code == 422, r.status_code)
r = client.post(f"/v1/sessions/{sid}/message-tracks", json={"expected_version": 1.5})
check("non-integer version 422", r.status_code == 422, r.status_code)
r = client.post(f"/v1/sessions/{sid}/message-tracks",
                json={"expected_version": 5, "extra": 1})
check("extra field 422", r.status_code == 422, r.status_code)

# 7. read-only: tracking never writes or consumes a version
before = client.get(f"/v1/sessions/{sid}").json()
track(sid, 5)
track(sid, 5)
after = client.get(f"/v1/sessions/{sid}").json()
check("no write", before["version"] == after["version"] == 5
      and before["trace"] == after["trace"], (before, after))

# 8. locked session can still be tracked
client.put(f"/v1/sessions/{sq}/lock", json={"expected_version": 2, "locked": True})
r = track(sq, 3)
check("locked still trackable 200", r.status_code == 200, (r.status_code, r.text))
check("locked result complete",
      r.json()["A_to_B"][0]["receive_transition_id"] == "t4", r.text)

# 9. protocol replacement: tracking follows the new definitions
rp = create(P)
run(rp, "sAB1", 0)
new_protocol = {**P, "transitions": [
    {**t, **({"message": "ZZ"} if t["id"] == "sAB1" else {})}
    for t in P["transitions"]
]}
r = client.put(f"/v1/sessions/{rp}/protocol",
               json={"expected_version": 1, "protocol": new_protocol})
check("replace 200", r.status_code == 200, r.text)
r = track(rp, 2)
check("recomputed under new protocol", r.status_code == 200
      and r.json()["A_to_B"][0]["message"] == "ZZ", (r.status_code, r.text))
# the stale version still answers 409 rather than old-definition data
r = track(rp, 1)
check("old version after replace 409", r.status_code == 409
      and r.json()["detail"]["current_version"] == 2, r.text)

# 10. concurrency: tracking vs execution on the same version either returns
#     the complete named-version result or 409, never a partial mix.
target = create(P)
run(target, "sAB1", 0)  # version 1, one pending A->B message
results = {}
barrier = threading.Barrier(8)


def track_worker(tag):
    barrier.wait()
    rr = track(target, 1)
    body = rr.json() if rr.status_code == 200 else None
    results[tag] = (
        rr.status_code,
        None if body is None else (
            len(body["A_to_B"]),
            body["A_to_B"][0]["send_transition_id"],
            body["A_to_B"][0]["receive_step"],
            len(body["B_to_A"]),
        ),
    )


def execute_worker():
    barrier.wait()
    results["execute"] = (run(target, "sBA", 1).status_code, None)


threads = [threading.Thread(target=track_worker, args=(f"trk{i}",))
           for i in range(7)]
threads.append(threading.Thread(target=execute_worker))
for t in threads:
    t.start()
for t in threads:
    t.join()

check("concurrent execute 200", results["execute"][0] == 200, results)
for i in range(7):
    code, snapshot = results[f"trk{i}"]
    check(f"concurrent track{i} 200/409", code in (200, 409), results)
    # a 200 is the whole v1 snapshot: exactly one still-unreceived A->B send
    # and no B->A activity; the post-commit sBA step can never leak in
    check(f"concurrent track{i} whole snapshot",
          code == 409 or snapshot == (1, "sAB1", None, 0), results)

final = client.get(f"/v1/sessions/{target}").json()
check("session advanced exactly once", final["version"] == 2
      and [s["transition_id"] for s in final["trace"]] == ["sAB1", "sBA"],
      (final["version"], final["trace"]))

r = track(target, 2)
check("post-commit tracking sees both sends",
      r.status_code == 200
      and len(r.json()["A_to_B"]) == 1
      and len(r.json()["B_to_A"]) == 1
      and r.json()["B_to_A"][0]["receive_step"] is None,
      (r.status_code, r.text))

# 11. old endpoints still work
r = client.get("/healthz")
check("healthz intact", r.status_code == 200)
r = client.post("/v1/sessions", json=Q)
check("sessions create intact", r.status_code == 201)
r = client.post("/v1/deadlocks", json=Q)
check("deadlocks intact", r.status_code == 200)
r = client.post("/v1/session-comparisons", json={
    "left_session_id": target, "right_session_id": target,
    "left_expected_version": 2, "right_expected_version": 2})
check("comparisons intact", r.status_code == 200)

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL MESSAGE-TRACK CHECKS PASSED")
