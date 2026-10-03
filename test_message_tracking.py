"""End-to-end checks for the read-only per-message tracking feature.

The tracker walks a drill trace resolved against the session's current
protocol: every send produces its own record (repeated sends of the same
message name are never merged), paired with the later same-direction receive
that consumes the one-slot queue; unreceived sends keep the three receive
columns null. The endpoint is version pinned, read-only and works on locked
sessions.
"""

import os
import tempfile

_tmp = tempfile.NamedTemporaryFile(prefix="sessions-track-", suffix=".db", delete=False)
_tmp.close()
os.environ["SESSIONS_DB"] = _tmp.name

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

failures = []


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f" :: {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def track(sid, version):
    return client.post(
        f"/v1/sessions/{sid}/message-tracking",
        json={"expected_version": version},
    )


def create(protocol):
    r = client.post("/v1/sessions", json=protocol)
    assert r.status_code == 201, r.text
    return r.json()["id"]


def run(sid, tid, ver):
    return client.post(
        f"/v1/sessions/{sid}/transitions",
        json={"transition_id": tid, "expected_version": ver},
    )


# Protocols -------------------------------------------------------------------

P = {
    "A": {"states": [0, 1], "initial": 0, "finals": [1]},
    "B": {"states": [0, 1], "initial": 0, "finals": [1]},
    "transitions": [
        {"id": "t1", "from": "A", "to": "B", "action": "send", "message": "m",
         "source": 0, "target": 1},
        {"id": "t4", "from": "A", "to": "B", "action": "receive", "message": "m",
         "source": 0, "target": 1},
    ],
}

# Both directions, sends self-loop on state 0 so every send/receive
# interleaving stays enabled.
PINTER = {
    "A": {"states": [0, 1], "initial": 0, "finals": [1]},
    "B": {"states": [0, 1], "initial": 0, "finals": [1]},
    "transitions": [
        {"id": "sAB", "from": "A", "to": "B", "action": "send", "message": "m",
         "source": 0, "target": 0},
        {"id": "rAB", "from": "A", "to": "B", "action": "receive", "message": "m",
         "source": 0, "target": 1},
        {"id": "sBA", "from": "B", "to": "A", "action": "send", "message": "n",
         "source": 0, "target": 0},
        {"id": "rBA", "from": "B", "to": "A", "action": "receive", "message": "n",
         "source": 0, "target": 1},
    ],
}

# The same message name "m" is sent and received twice; the two sends must
# remain two records (no merging by name). B starts non-final and receives m
# once per B state (r1: 0->1, r2 self-loops at 1), so the full exchange ends
# in the completion configuration.
PREPEAT = {
    # A never reaches a final state during the two exchanges, so the trace is
    # allowed to keep going after the first send/receive round.
    "A": {"states": [0, 1], "initial": 0, "finals": [1]},
    "B": {"states": [0, 1], "initial": 0, "finals": [1]},
    "transitions": [
        {"id": "s1", "from": "A", "to": "B", "action": "send", "message": "m",
         "source": 0, "target": 0},
        {"id": "r1", "from": "A", "to": "B", "action": "receive", "message": "m",
         "source": 0, "target": 1},
        {"id": "s2", "from": "A", "to": "B", "action": "send", "message": "m",
         "source": 0, "target": 0},
        {"id": "r2", "from": "A", "to": "B", "action": "receive", "message": "m",
         "source": 1, "target": 1},
    ],
}

# 1. empty trace -> two empty lists, version echoed
sid = create(P)
r = track(sid, 0)
check("empty 200", r.status_code == 200, (r.status_code, r.text))
check("empty body", r.json() == {
    "session_id": sid, "version": 0, "A_to_B": [], "B_to_A": []}, r.json())

# 2. sent but not yet received -> null receive columns
assert run(sid, "t1", 0).status_code == 200
r = track(sid, 1)
check("unreceived 200", r.status_code == 200, r.status_code)
b = r.json()
check("unreceived echo", b["session_id"] == sid and b["version"] == 1, b)
check("unreceived record", b["A_to_B"] == [{
    "direction": "A_to_B", "message": "m",
    "send_transition_id": "t1", "send_step": 1,
    "receive_transition_id": None, "receive_step": None,
    "steps_between": None}], b["A_to_B"])
check("unreceived other direction empty", b["B_to_A"] == [], b["B_to_A"])

# 3. received -> pair completed, no steps between adjacent steps
assert run(sid, "t4", 1).status_code == 200
r = track(sid, 2)
b = r.json()
check("received record", b["A_to_B"] == [{
    "direction": "A_to_B", "message": "m",
    "send_transition_id": "t1", "send_step": 1,
    "receive_transition_id": "t4", "receive_step": 2,
    "steps_between": 0}], b["A_to_B"])

# 4. interleaving: steps strictly between the send/receive end steps
inter = create(PINTER)
for tid, ver in (("sAB", 0), ("sBA", 1), ("rAB", 2), ("rBA", 3)):
    assert run(inter, tid, ver).status_code == 200
r = track(inter, 4)
check("interleave 200", r.status_code == 200, (r.status_code, r.text))
b = r.json()
check("A_to_B across one foreign step", b["A_to_B"] == [{
    "direction": "A_to_B", "message": "m",
    "send_transition_id": "sAB", "send_step": 1,
    "receive_transition_id": "rAB", "receive_step": 3,
    "steps_between": 1}], b["A_to_B"])
check("B_to_A across one foreign step", b["B_to_A"] == [{
    "direction": "B_to_A", "message": "n",
    "send_transition_id": "sBA", "send_step": 2,
    "receive_transition_id": "rBA", "receive_step": 4,
    "steps_between": 1}], b["B_to_A"])

# 5. mid-drill: both directions sent, neither received -> both unreceived
mid = create(PINTER)
assert run(mid, "sAB", 0).status_code == 200
assert run(mid, "sBA", 1).status_code == 200
b = track(mid, 2).json()
check("mid A_to_B unreceived", b["A_to_B"][0]["receive_step"] is None
      and b["A_to_B"][0]["send_step"] == 1, b["A_to_B"])
check("mid B_to_A unreceived", b["B_to_A"][0]["receive_step"] is None
      and b["B_to_A"][0]["send_step"] == 2, b["B_to_A"])
check("mid null columns", b["A_to_B"][0]["receive_transition_id"] is None
      and b["A_to_B"][0]["steps_between"] is None)

# 6. same-name message sent twice stays two records
rep = create(PREPEAT)
for tid, ver in (("s1", 0), ("r1", 1), ("s2", 2), ("r2", 3)):
    assert run(rep, tid, ver).status_code == 200
b = track(rep, 4).json()
check("repeat two records", len(b["A_to_B"]) == 2, b["A_to_B"])
check("repeat not merged",
      [r["send_transition_id"] for r in b["A_to_B"]] == ["s1", "s2"]
      and [r["receive_transition_id"] for r in b["A_to_B"]] == ["r1", "r2"],
      b["A_to_B"])
check("repeat ordered by send step",
      [r["send_step"] for r in b["A_to_B"]] == [1, 3]
      and [r["receive_step"] for r in b["A_to_B"]] == [2, 4]
      and [r["steps_between"] for r in b["A_to_B"]] == [0, 0], b["A_to_B"])

# 7. only the second same-name send unreceived
rep2 = create(PREPEAT)
for tid, ver in (("s1", 0), ("r1", 1), ("s2", 2)):
    assert run(rep2, tid, ver).status_code == 200
b = track(rep2, 3).json()
check("first of repeat received",
      b["A_to_B"][0]["receive_transition_id"] == "r1", b["A_to_B"])
check("second of repeat unreceived",
      b["A_to_B"][1]["receive_transition_id"] is None
      and b["A_to_B"][1]["receive_step"] is None
      and b["A_to_B"][1]["steps_between"] is None, b["A_to_B"])

# 8. unknown session -> 404
r = track("deadbeef", 0)
check("unknown 404", r.status_code == 404
      and r.json()["detail"] == "unknown session", (r.status_code, r.text))

# 9. stale version -> 409 with current_version
r = track(sid, 0)
check("stale 409", r.status_code == 409, (r.status_code, r.text))
check("stale detail", r.json()["detail"] == {
    "error": "version_conflict", "current_version": 2}, r.json())
# unknown takes precedence over a stale-looking version
r = track("deadbeef", 99)
check("unknown beats stale 404", r.status_code == 404, r.status_code)

# 10. malformed bodies -> 422 (same plain-int version rules as every other
#     session endpoint: negative/missing rejected; bool/string/float coerce)
for bad_body, name in [
    ({}, "missing expected_version"),
    ({"expected_version": -1}, "negative version"),
    ({"expected_version": 2, "extra": 1}, "extra field"),
]:
    r = client.post(f"/v1/sessions/{sid}/message-tracking", json=bad_body)
    check(f"{name} 422", r.status_code == 422, (r.status_code, r.text))

# 11. read-only: tracking never writes and consumes no version
before = client.get(f"/v1/sessions/{mid}").json()
track(mid, 2)
track(mid, 2)
after = client.get(f"/v1/sessions/{mid}").json()
check("read-only version", before["version"] == after["version"] == 2)
check("read-only trace", before["trace"] == after["trace"])

# 12. a locked session can still be tracked (query-like, read-only)
lk = create(P)
assert run(lk, "t1", 0).status_code == 200
r = client.put(f"/v1/sessions/{lk}/lock",
               json={"expected_version": 1, "locked": True})
assert r.status_code == 200, r.text
r = track(lk, 2)
check("locked track 200", r.status_code == 200, (r.status_code, r.text))
check("locked track sees trace", r.json()["A_to_B"][0]["send_step"] == 1
      and r.json()["A_to_B"][0]["receive_step"] is None, r.json())
check("locked unchanged", client.get(f"/v1/sessions/{lk}").json()["version"] == 2)

# 13. protocol replacement: tracking uses the new definitions
#     t1 keeps id/message/direction; change its target (A 0->0 self loop),
#     then the trace [t1, t4] replay succeeds and records reflect new defs.
rp = create(P)
assert run(rp, "t1", 0).status_code == 200
assert run(rp, "t4", 1).status_code == 200
P2 = {
    "A": {"states": [0, 1], "initial": 0, "finals": [1]},
    "B": {"states": [0, 1], "initial": 0, "finals": [1]},
    "transitions": [
        {"id": "t1", "from": "A", "to": "B", "action": "send", "message": "m",
         "source": 0, "target": 0},
        {"id": "t4", "from": "A", "to": "B", "action": "receive", "message": "m",
         "source": 0, "target": 1},
    ],
}
r = client.put(f"/v1/sessions/{rp}/protocol",
               json={"protocol": P2, "expected_version": 2})
check("replace setup 200", r.status_code == 200, (r.status_code, r.text))
b = track(rp, 3).json()
check("post-replace pair intact", b["A_to_B"] == [{
    "direction": "A_to_B", "message": "m",
    "send_transition_id": "t1", "send_step": 1,
    "receive_transition_id": "t4", "receive_step": 2,
    "steps_between": 0}], b)

# replace that makes the stored trace non-replayable (step 2 receives ZZ while
# the slot still holds m) is rejected at 422; tracking keeps answering against
# the retained revision, never the rejected one.
P3 = {
    "A": {"states": [0, 1], "initial": 0, "finals": [1]},
    "B": {"states": [0, 1], "initial": 0, "finals": [1]},
    "transitions": [
        {"id": "t1", "from": "A", "to": "B", "action": "send", "message": "m",
         "source": 0, "target": 0},
        {"id": "t4", "from": "A", "to": "B", "action": "receive", "message": "ZZ",
         "source": 0, "target": 1},
    ],
}
r = client.put(f"/v1/sessions/{rp}/protocol",
               json={"protocol": P3, "expected_version": 3})
check("rejected replace 422", r.status_code == 422, (r.status_code, r.text))
b = track(rp, 3).json()
check("track stays on committed revision",
      b["A_to_B"][0]["message"] == "m", b)

# 14. concurrency: tracking vs execution -> whole named snapshot or 409
import threading

target = create(PINTER)
results = {}
barrier = threading.Barrier(6)


def track_worker(tag):
    barrier.wait()
    rr = track(target, 0)
    body = rr.json() if rr.status_code == 200 else None
    results[tag] = (
        rr.status_code,
        None if body is None else (
            len(body["A_to_B"]), len(body["B_to_A"])),
    )


def execute_worker():
    barrier.wait()
    results["execute"] = (run(target, "sAB", 0).status_code, None)


threads = [threading.Thread(target=track_worker, args=(f"track{i}",))
           for i in range(5)]
threads.append(threading.Thread(target=execute_worker))
for t in threads:
    t.start()
for t in threads:
    t.join()
check("concurrent execute 200", results["execute"][0] == 200, results)
for i in range(5):
    code, counts = results[f"track{i}"]
    check(f"concurrent track{i} 200/409", code in (200, 409), results)
    # a 200 must be the complete v0 snapshot (no records); the post-commit
    # send can never leak into an accepted tracking response
    check(f"concurrent track{i} whole snapshot",
          code == 409 or counts == (0, 0), results)
final = client.get(f"/v1/sessions/{target}").json()
check("concurrent version advanced once", final["version"] == 1
      and [s["transition_id"] for s in final["trace"]] == ["sAB"],
      (final["version"], final["trace"]))
# the new version then tracks exactly that one unreceived send
b = track(target, 1).json()
check("post-commit tracking sees one send",
      len(b["A_to_B"]) == 1 and b["A_to_B"][0]["send_transition_id"] == "sAB"
      and b["A_to_B"][0]["receive_step"] is None
      and b["B_to_A"] == [], b)

# 15. a branched session is tracked on its own independent trace/version
src = create(P)
assert run(src, "t1", 0).status_code == 200  # source v1, unreceived m
r = client.post(f"/v1/sessions/{src}/branches",
                json={"expected_version": 1, "prefix_length": 1})
check("branch setup 201", r.status_code == 201, (r.status_code, r.text))
branch = r.json()["id"]
b = track(branch, 0).json()
check("branch tracked at version 0", b["version"] == 0
      and b["session_id"] == branch, b)
check("branch has own unreceived record", b["A_to_B"][0]["send_step"] == 1
      and b["A_to_B"][0]["receive_step"] is None, b["A_to_B"])

# 16. old endpoints still work
r = client.get("/healthz")
check("healthz intact", r.status_code == 200)
r = client.post("/v1/sessions", json=P)
check("sessions create intact", r.status_code == 201)
r = client.post("/v1/deadlocks", json=P)
check("deadlocks intact", r.status_code == 200)
r = client.post("/v1/session-comparisons",
                json={"left_session_id": sid, "right_session_id": sid,
                      "left_expected_version": 2, "right_expected_version": 2})
check("comparisons intact", r.status_code == 200)

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL TRACKING CHECKS PASSED")
