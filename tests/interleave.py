#!/usr/bin/env python3
"""交错场景实测（仅标准库）：并发复测、空交集、重新登记交错、失败原子性、跨班并发。

用法: python3 tests/interleave.py [base_url]
"""
import json
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000").rstrip("/")
FAILED = []


def call(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(BASE + urllib.parse.quote(path, safe="/"),
                                 data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  | {detail}" if detail else ""))
    if not cond:
        FAILED.append(name)


def chain3():
    """三段串联 IN->A->B->OUT，S3 原读数与守恒矛盾。"""
    segs = [
        {"key": "S1", "from_node": "IN", "to_node": "A",
         "capacity": 1000, "meter_lo": 100, "meter_hi": 200},
        {"key": "S2", "from_node": "A", "to_node": "B",
         "capacity": 1000, "meter_lo": 100, "meter_hi": 200},
        {"key": "S3", "from_node": "B", "to_node": "OUT",
         "capacity": 1000, "meter_lo": 300, "meter_hi": 400},
    ]
    return {"ports": ["IN", "OUT"], "segments": segs}


# ---------- 场景 1：多轮并发复测，两笔都入库且诊断用全集 ----------
SHIFT1 = "交错一班"
s, r0 = call("POST", f"/shifts/{SHIFT1}/readings", chain3())
check("一班初始登记 201 且账不平", s == 201 and r0["balanced"] is False, str(s))

for round_i in range(5):
    s_before, before = call("GET", f"/shifts/{SHIFT1}/readings")
    n_before = len(before["remeasures"])
    barrier = threading.Barrier(2)
    out = {}

    def worker(tag, seg, val):
        barrier.wait()
        out[tag] = call("POST", f"/shifts/{SHIFT1}/remeasurements",
                        {"remeasures": [{"segment_key": seg,
                                         "rlo": val, "rhi": val}]})

    t1 = threading.Thread(target=worker, args=("A", "S1", 100 + round_i))
    t2 = threading.Thread(target=worker, args=("B", "S2", 200 - round_i))
    t1.start(); t2.start(); t1.join(); t2.join()
    codes = sorted(v[0] for v in out.values())
    s_mid, mid = call("GET", f"/shifts/{SHIFT1}/readings")
    s_lat, latest = call("GET", f"/shifts/{SHIFT1}/diagnoses/latest")
    used = set(latest.get("inputs", {}).get("remeasures", {}))
    stored = {(x["segment_key"], x["rlo"]) for x in mid["remeasures"]}
    check(f"第{round_i}轮两笔复测均 201", codes == [201, 201], str(codes))
    check(f"第{round_i}轮两笔复测读数均入库",
          len(mid["remeasures"]) == n_before + 2,
          f"{n_before} -> {len(mid['remeasures'])}")
    check(f"第{round_i}轮最新诊断使用全部已入库有效复测",
          {"S1", "S2"} <= used, str(sorted(used)))

# 最终：S1/S2 复测把交集锁成单点，最新诊断 inputs 必须含两管段
s, latest = call("GET", f"/shifts/{SHIFT1}/diagnoses/latest")
used = latest["inputs"]["remeasures"]
check("最终最新诊断同时含 S1、S2 复测", set(used) == {"S1", "S2"},
      json.dumps(used, ensure_ascii=False))
s, readings = call("GET", f"/shifts/{SHIFT1}/readings")
check("最终复测读数共 10 笔（5 轮 × 2）",
      len(readings["remeasures"]) == 10,
      str(len(readings["remeasures"])))

# ---------- 场景 2：复测区间交集为空 —— 读数保留、诊断报账不平 ----------
SHIFT2 = "交错二班"
s, _ = call("POST", f"/shifts/{SHIFT2}/readings", chain3())
s, ra = call("POST", f"/shifts/{SHIFT2}/remeasurements",
             {"remeasures": [{"segment_key": "S3", "rlo": 150, "rhi": 150}]})
check("空交班前复测账平", s == 201 and ra["balanced"] is True)
diag_before = ra["diagnosis_id"]
s, rb = call("POST", f"/shifts/{SHIFT2}/remeasurements",
             {"remeasures": [{"segment_key": "S3", "rlo": 160, "rhi": 160}]})
check("交集为空的复测仍 201 入库", s == 201, str(s))
check("空交集诊断报账不平", rb["balanced"] is False)
check("空交集明确标注 empty_remeasure",
      rb.get("empty_remeasure") == ["S3"], json.dumps(rb.get("empty_remeasure")))
check("空交集提示信息区分于单表失准", "交集为空" in rb["message"], rb["message"])
s, rr = call("GET", f"/shifts/{SHIFT2}/readings")
check("空交集后两笔复测读数都保留",
      len(rr["remeasures"]) == 2
      and rr["effective_remeasures"]["S3"] == {"rlo": 160, "rhi": 150},
      json.dumps(rr["effective_remeasures"], ensure_ascii=False))
s, hist = call("GET", f"/shifts/{SHIFT2}/diagnoses")
check("诊断历史保留（登记+两次复测共 3 条）",
      len(hist["diagnoses"]) == 3, str(len(hist["diagnoses"])))

# 再补一笔放宽区间无法“解开”空交集（交集仍空），读数继续保留
s, rc = call("POST", f"/shifts/{SHIFT2}/remeasurements",
             {"remeasures": [{"segment_key": "S3", "rlo": 100, "rhi": 200}]})
check("再补复测后交集仍空、仍账不平",
      s == 201 and rc["balanced"] is False
      and rc.get("empty_remeasure") == ["S3"])
s, rr = call("GET", f"/shifts/{SHIFT2}/readings")
check("三笔复测读数全部保留", len(rr["remeasures"]) == 3)

# ---------- 场景 3：重新登记先入库，复测按新登记核对；移除管段整批退回 ----------
SHIFT3 = "交错三班"
s, _ = call("POST", f"/shifts/{SHIFT3}/readings", chain3())
# 新登记移除 S3，只剩 S1/S2
s, rr3 = call("POST", f"/shifts/{SHIFT3}/readings",
              {"ports": ["IN", "OUT"], "segments": [
                  {"key": "S1", "from_node": "IN", "to_node": "A",
                   "capacity": 1000, "meter_lo": 100, "meter_hi": 200},
                  {"key": "S2", "from_node": "A", "to_node": "OUT",
                   "capacity": 1000, "meter_lo": 100, "meter_hi": 200}]})
check("重新登记 201", s == 201, str(s))
s, readings3 = call("GET", f"/shifts/{SHIFT3}/readings")
check("当前登记只剩 S1/S2",
      {x["segment_key"] for x in readings3["registration"]["segments"]}
      == {"S1", "S2"})
# 整批含已移除的 S3 -> 422 整批退回
s, rej = call("POST", f"/shifts/{SHIFT3}/remeasurements",
              {"remeasures": [
                  {"segment_key": "S1", "rlo": 150, "rhi": 150},
                  {"segment_key": "S3", "rlo": 150, "rhi": 150}]})
check("批内含已移除管段 -> 422 整批退回", s == 422, rej.get("detail", ""))
s, readings3b = call("GET", f"/shifts/{SHIFT3}/readings")
check("整批退回后无任何复测入库",
      readings3b["remeasures"] == [],
      json.dumps(readings3b["remeasures"], ensure_ascii=False))
s, latest3 = call("GET", f"/shifts/{SHIFT3}/diagnoses/latest")
check("整批退回后最新诊断仍是重新登记那次",
      latest3["id"] == rr3["diagnosis_id"],
      f"{latest3['id']} vs {rr3['diagnosis_id']}")
# 合法批（仅 S1）正常入库并按新登记诊断
s, ok3 = call("POST", f"/shifts/{SHIFT3}/remeasurements",
              {"remeasures": [{"segment_key": "S1",
                               "rlo": 150, "rhi": 150}]})
check("按新登记的合法复测 201 且账平",
      s == 201 and ok3["balanced"] is True, str(s))

# ---------- 场景 4：重新登记与复测并发交错，任一顺序下状态自洽 ----------
SHIFT4 = "交错四班"
s, _ = call("POST", f"/shifts/{SHIFT4}/readings", chain3())
barrier = threading.Barrier(2)
race_out = {}


def do_rereg():
    barrier.wait()
    race_out["rereg"] = call("POST", f"/shifts/{SHIFT4}/readings", chain3())


def do_rem():
    barrier.wait()
    race_out["rem"] = call(
        "POST", f"/shifts/{SHIFT4}/remeasurements",
        {"remeasures": [{"segment_key": "S3", "rlo": 150, "rhi": 150}]})


t1 = threading.Thread(target=do_rereg)
t2 = threading.Thread(target=do_rem)
t1.start(); t2.start(); t1.join(); t2.join()
check("交错两请求都得到确定响应（201）",
      sorted(v[0] for v in race_out.values()) == [201, 201],
      str({k: v[0] for k, v in race_out.items()}))
s, final4 = call("GET", f"/shifts/{SHIFT4}/readings")
s, latest4 = call("GET", f"/shifts/{SHIFT4}/diagnoses/latest")
# 自洽性：最新诊断引用的登记 = 当前最新登记，且诊断所用复测都在该登记内
reg_id = final4["registration"]["id"]
check("最新诊断挂在当前最新登记上",
      latest4["registration_id"] == reg_id,
      f"{latest4['registration_id']} vs {reg_id}")
diag_keys = {x["key"] for x in latest4["inputs"]["segments"]}
rem_keys = set(latest4["inputs"]["remeasures"])
check("诊断所用复测管段均在最新登记中", rem_keys <= diag_keys,
      f"{sorted(rem_keys)} vs {sorted(diag_keys)}")
# 若复测后执行，复测必已入库且诊断使用它；若复测先执行，复测也必在库中
check("复测读数无论胜负都已入库（或整批合法）",
      any(x["segment_key"] == "S3" for x in final4["remeasures"]),
      json.dumps(final4["remeasures"], ensure_ascii=False))

# ---------- 场景 5：复测失败时读数与诊断都不入库 ----------
SHIFT5 = "交错五班"
s, r5 = call("POST", f"/shifts/{SHIFT5}/readings", chain3())
s, h5 = call("GET", f"/shifts/{SHIFT5}/diagnoses")
n_diag_before = len(h5["diagnoses"])
s, bad = call("POST", f"/shifts/{SHIFT5}/remeasurements",
              {"remeasures": [{"segment_key": "S9", "rlo": 1, "rhi": 2}]})
check("非法复测 422", s == 422)
s, r5b = call("GET", f"/shifts/{SHIFT5}/readings")
check("失败后无复测读数入库", r5b["remeasures"] == [])
s, h5b = call("GET", f"/shifts/{SHIFT5}/diagnoses")
check("失败后诊断数量不变", len(h5b["diagnoses"]) == n_diag_before,
      f"{n_diag_before} -> {len(h5b['diagnoses'])}")
# 入参校验失败（区间倒置）同样不入库
s, bad2 = call("POST", f"/shifts/{SHIFT5}/remeasurements",
               {"remeasures": [{"segment_key": "S1", "rlo": 9, "rhi": 1}]})
check("区间倒置 422", s == 422)
s, r5c = call("GET", f"/shifts/{SHIFT5}/readings")
check("校验失败后仍无复测入库", r5c["remeasures"] == [])

# ---------- 场景 6：一班长时间诊断期间，其他班仍可写入 ----------
SHIFT6A = "长算班甲"
SHIFT6B = "长算班乙"
s, _ = call("POST", f"/shifts/{SHIFT6A}/readings", chain3())
s, _ = call("POST", f"/shifts/{SHIFT6B}/readings", chain3())
write_done = threading.Event()


def slow_other_shift_writes():
    # 甲班持锁诊断期间，乙班连续写登记/复测，应全部快速成功
    ok = True
    detail = []
    for i in range(3):
        sc, _ = call("POST", f"/shifts/{SHIFT6B}/readings", chain3())
        ok = ok and sc == 201
        detail.append(sc)
    sc, _ = call("POST", f"/shifts/{SHIFT6B}/remeasurements",
                 {"remeasures": [{"segment_key": "S3",
                                  "rlo": 150, "rhi": 150}]})
    ok = ok and sc == 201
    detail.append(sc)
    write_done.result = (ok, detail)
    write_done.set()


th = threading.Thread(target=slow_other_shift_writes)
# 甲班发起一次写（诊断在锁内），同时乙班并发写
start_barrier = threading.Barrier(2)
jia_out = {}


def jia_write():
    start_barrier.wait()
    jia_out["r"] = call(
        "POST", f"/shifts/{SHIFT6A}/remeasurements",
        {"remeasures": [{"segment_key": "S3", "rlo": 140, "rhi": 160}]})


def yi_write():
    start_barrier.wait()
    slow_other_shift_writes()


t1 = threading.Thread(target=jia_write)
t2 = threading.Thread(target=yi_write)
t1.start(); t2.start(); t1.join(); t2.join()
ok, detail = getattr(write_done, "result", (False, []))
check("甲班诊断期间乙班登记/复测全部成功（跨班不阻塞）", ok, str(detail))
check("甲班自身复测也成功", jia_out["r"][0] == 201, str(jia_out["r"][0]))
s, hb = call("GET", f"/shifts/{SHIFT6B}/diagnoses")
check("乙班诊断历史完整（初始 1 次 + 线程内 3 次登记 + 1 次复测 = 5 条）",
      len(hb["diagnoses"]) == 5, str(len(hb["diagnoses"])))

# ---------- 场景 7：登记失败原子性（构造 solver 失败不易，改为校验失败不落库） ----------
SHIFT7 = "校验班"
s, r7 = call("POST", f"/shifts/{SHIFT7}/readings", chain3())
s, bad7 = call("POST", f"/shifts/{SHIFT7}/readings",
               {"ports": ["IN", "OUT"],
                "segments": [{"key": f"V{i}", "from_node": "IN",
                              "to_node": "OUT", "capacity": 10,
                              "meter_lo": 0, "meter_hi": 10}
                             for i in range(21)]})
check("21 管段登记 422", s == 422)
s, shifts = call("GET", "/shifts")
s, r7b = call("GET", f"/shifts/{SHIFT7}/readings")
check("校验失败后原登记保持不变（仍 3 段）",
      len(r7b["registration"]["segments"]) == 3)

print()
if FAILED:
    print(f"共 {len(FAILED)} 项失败: {FAILED}")
    sys.exit(1)
print("全部交错用例通过")
