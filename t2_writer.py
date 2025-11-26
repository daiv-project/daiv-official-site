#!/usr/bin/env python3
# /opt/gamma_writer/t2_writer.py
# usage: python3 t2_writer.py --preflight --registry /opt/gamma_writer/analysis_run.json

import argparse, json, os, re, sys, uuid
import uuid
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager

# =============== File Lock (non-blocking) ===============
class FileLock:
    def __init__(self, path="/tmp/t2_writer.lock"):
        self.path = path
        self.fd = None

    def try_acquire(self):
        import fcntl
        try:
            self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(self.fd, 0)
            os.write(self.fd, str(os.getpid()).encode())
            return True
        except BlockingIOError:
            return False

    def release(self):
        if self.fd is not None:
            try:
                import fcntl
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            except Exception:
                pass
            os.close(self.fd)
            self.fd = None

# =============== DB helpers (MariaDB) ===============
def db_connect():
    try:
        import pymysql
    except ImportError:
        print("[PREFLIGHT] ❌ ERR: pymysql not installed. Run: pip install pymysql", file=sys.stderr)
        sys.exit(2)
    host = os.getenv("DB_HOST", "127.0.0.1")

    user = os.getenv("DB_USER", "your DB")
    password = os.getenv("DB_PASS", "your_DB_pass")
    db = os.getenv("DB_NAME", "your_db_namme")
    port = int(os.getenv("DB_PORT", "3306"))
    conn = pymysql.connect(host=host, user=user, password=password, db=db,
                           port=port, charset="utf8mb4",
                           cursorclass=pymysql.cursors.DictCursor,
                           autocommit=True)
    return conn

def one(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()

def all_rows(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

def exists_unique_index_by_name(conn, table, index_name):
    sql = f"""
    SELECT COUNT(DISTINCT index_name) AS n
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = '{table}'
      AND index_name = '{index_name}'
      AND non_unique = 0;
    """
    return one(conn, sql)["n"] >= 1

def exists_unique_index_by_cols(conn, table, cols_in_order):
    rows = all_rows(conn, f"""
        SELECT index_name, non_unique, seq_in_index, column_name
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = '{table}'
        ORDER BY index_name, seq_in_index
    """)
    from collections import defaultdict
    idx = defaultdict(list)
    uniq = {}
    for r in rows:
        idx[r["index_name"]].append(r["column_name"])
        uniq[r["index_name"]] = (r["non_unique"] == 0)
    target = tuple(cols_in_order)
    for name, col_list in idx.items():
        if uniq.get(name, False) and tuple(col_list) == target:
            return True
    return False

def column_exists(conn, table, column):
    sql = f"""
    SELECT COUNT(*) AS n
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = '{table}'
      AND column_name = '{column}';
    """
    return one(conn, sql)["n"] == 1

def column_type(conn, table, column):
    sql = f"""
    SELECT data_type
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = '{table}'
      AND column_name = '{column}';
    """
    row = one(conn, sql)
    return row["data_type"] if row else None

def ensure_table_key_columns(conn, table, errs):
    need = {
        "unit_id": ("char","varchar"),
        "day_jst": ("date",),
        "run_id":  ("char","varchar"),
    }
    for col, types in need.items():
        if not column_exists(conn, table, col):
            errs.append(f"{table}.{col} missing")
        else:
            dt = column_type(conn, table, col)
            if dt not in types:
                errs.append(f"{table}.{col} type={dt} expected {types}")

# ---- u_log writer (schema-aware) ----
def today_jst_str():
    return (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")

def now_iso_utc():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def insert_u_log(conn, payload, unit_id="system", run_id="preflight", day_jst=None):
    if day_jst is None:
        day_jst = today_jst_str()
    cols = ["unit_id","run_id","day_jst","u"]
    vals = [unit_id, run_id, day_jst, json.dumps(payload, ensure_ascii=False)]
    # created_at があればUTCで入れる
    if column_exists(conn, "u_log", "created_at"):
        cols.append("created_at")
        vals.append(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    # id列がある場合のみ付与
    if column_exists(conn, "u_log", "id"):
        cols.insert(0, "id")
        vals.insert(0, str(uuid.uuid4()))
    sql = f"INSERT INTO u_log ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))})"
    with conn.cursor() as cur:
        cur.execute(sql, vals)

# =============== Registry checks ===============
HEX64 = re.compile(r"^[0-9a-f]{64}$")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def parse_date_or_none(s):
    if s is None: return None
    if not isinstance(s, str) or not ISO_DATE.fullmatch(s): return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def validate_eval_windows(reg, errs):
    ew = reg.get("eval_windows")
    if not isinstance(ew, dict):
        errs.append("eval_windows missing or not an object")
        return
    tz = ew.get("timezone")
    half_open = ew.get("half_open")
    if tz != "Asia/Tokyo":
        errs.append("eval_windows.timezone must be 'Asia/Tokyo'")
    if half_open is not True:
        errs.append("eval_windows.half_open must be true (JST half-open)")
    for k in ["train","val","test","holdout"]:
        if k not in ew or not isinstance(ew[k], dict):
            errs.append(f"eval_windows.{k} missing or not an object")
            return
    train_s = parse_date_or_none(ew["train"].get("start"))
    train_e = parse_date_or_none(ew["train"].get("end"))
    val_s   = parse_date_or_none(ew["val"].get("start"))
    val_e   = parse_date_or_none(ew["val"].get("end"))
    test_s  = parse_date_or_none(ew["test"].get("start"))
    test_e  = parse_date_or_none(ew["test"].get("end"))
    hold_s  = parse_date_or_none(ew["holdout"].get("start"))
    if any(x is None for x in [train_s,train_e,val_s,val_e,test_s,test_e,hold_s]):
        errs.append("eval_windows contains non-ISO dates (YYYY-MM-DD)")
        return
    if not (train_s < train_e <= val_s < val_e <= test_s < test_e):
        errs.append("eval_windows order invalid: require train.start < train.end ≤ val.start < val.end ≤ test.start < test.end")
    if test_s == test_e:
        errs.append("eval_windows.test must span > 0 days (start < end)")

def validate_registry(reg):
    errs, warns, info = [], [], []

    def need(key, typ=None):
        if key not in reg:
            errs.append(f"missing key: {key}")
            return
        if typ and not isinstance(reg[key], typ):
            errs.append(f"type mismatch: {key} expected {typ} got {type(reg[key])}")

    # required roots
    for k in ["schema_version","tag_set_hash","feature_spec_hash","data_snapshot_hash",
              "runtime_env_hash","tag_list","random_state","prior_post",
              "window_spec","assignment","y_snapshot_ts","eval_query_template_id","eval_windows","y_spec"]:
        need(k)

    # hex64 hashes
    for k in ["tag_set_hash","feature_spec_hash","data_snapshot_hash","runtime_env_hash"]:
        if k in reg and not (isinstance(reg[k], str) and HEX64.fullmatch(reg[k])):
            errs.append(f"{k} must be 64-hex")

    # tag_list & unknown
    if "tag_list" in reg and "unknown" not in reg["tag_list"]:
        errs.append("tag_list must include 'unknown'")

    # prior/post
    if "prior_post" in reg and reg["prior_post"].get("use_post_in_eval") is not False:
        errs.append("prior_post.use_post_in_eval must be false")

    # versions naming (t2/suggest 分離)
    if "t2_model_version" not in reg:
        errs.append("t2_model_version missing")
    if "suggest_method_version" not in reg:
        errs.append("suggest_method_version missing")

    # assignment rule must be pure function
    if "assignment" in reg:
        rule = reg["assignment"].get("rule")
        if rule != "hash_uniform_v1":
            errs.append("assignment.rule must be 'hash_uniform_v1'")

    # eval_query_template_id 固定
    if reg.get("eval_query_template_id") != "prior_only_v1":
        errs.append("eval_query_template_id must be 'prior_only_v1'")

    # y_spec 互換（name or main_kpi のどちらでも）
    yspec = reg.get("y_spec")
    if isinstance(yspec, dict):
        y_name = yspec.get("name") or yspec.get("main_kpi")
        if y_name != "wins":
            errs.append("y_spec.name (or main_kpi) must be 'wins'")
    else:
        errs.append("y_spec must be an object")

    # eval_windows 厳格チェック
    validate_eval_windows(reg, errs)

    # recommended keys
    for k in ["numeric_tolerance","min_match_rate","min_daily_runs_for_metrics",
              "rare_tag_min_weekly_count","text_max_len_chars","warp_seed","lock_owner","lock_resource","lock_ttl_sec"]:
        if k not in reg:
            warns.append(f"recommend add: {k}")

    # lock_owner 非空
    if "lock_owner" in reg and (reg["lock_owner"] is None or str(reg["lock_owner"]).strip() == ""):
        errs.append("lock_owner must be non-empty (e.g., 'host:pid' or branch name)")

    return errs, warns, info

# =============== Preflight ===============
def preflight(registry_path):
    # load registry first
    try:
        with open(registry_path, "r", encoding="utf-8") as f:
            reg = json.load(f)
    except Exception as e:
        print(f"[PREFLIGHT] ❌ ERR: registry load error: {e}", file=sys.stderr)
        return 1

    errs, warns, info = validate_registry(reg)

    conn = None
    try:
        conn = db_connect()

        # INFO: DB version
        row = one(conn, "SELECT @@version AS v, @@version_comment AS c")
        if row:
            print(f"[PREFLIGHT] INFO: MariaDB {row['v']} ({row['c']})")

        # WARN: charset/collation
        col = one(conn, "SHOW VARIABLES LIKE 'collation_database'")
        chs = one(conn, "SHOW VARIABLES LIKE 'character_set_database'")
        if col and not str(col["Value"]).startswith("utf8mb4"):
            warns.append(f"database collation = {col['Value']} (recommend utf8mb4_*)")
        if chs and chs["Value"] != "utf8mb4":
            warns.append(f"character_set_database = {chs['Value']} (recommend utf8mb4)")

        # Lock after DB open: if busy → u_log 記録して終了
        lock = FileLock()
        if not lock.try_acquire():
            payload = {"type":"t2_error","reason":"lock_busy","at":now_iso_utc()}
            try:
                insert_u_log(conn, payload)
            except Exception as e:
                print(f"[PREFLIGHT] WARN: failed to write u_log for lock_busy: {e}", file=sys.stderr)
            print("[PREFLIGHT] ❌ ERR: lock_busy: another t2_writer is running", file=sys.stderr)
            return 1

        # Table/key sanity
        db_errs = []
        ensure_table_key_columns(conn, "t1_log", db_errs)
        ensure_table_key_columns(conn, "t2_log", db_errs)

        # Unique constraints by name
        if not exists_unique_index_by_name(conn, "t1_log", "uk_t1_unit_day_run"):
            db_errs.append("missing UNIQUE index by name t1_log.uk_t1_unit_day_run")
        if not exists_unique_index_by_name(conn, "t2_log", "uk_t2_unit_day_run"):
            db_errs.append("missing UNIQUE index by name t2_log.uk_t2_unit_day_run")
        if not exists_unique_index_by_name(conn, "suggest_output", "uk_suggest_run_id"):
            db_errs.append("missing UNIQUE index by name suggest_output.uk_suggest_run_id")

        # Unique constraints by column combination (保険)
        cols = ["unit_id","day_jst","run_id"]
        if not exists_unique_index_by_cols(conn, "t1_log", cols):
            db_errs.append("missing UNIQUE index (by cols) t1_log(unit_id,day_jst,run_id)")
        if not exists_unique_index_by_cols(conn, "t2_log", cols):
            db_errs.append("missing UNIQUE index (by cols) t2_log(unit_id,day_jst,run_id)")

        # delivered_at
        if not column_exists(conn, "suggest_output", "delivered_at"):
            db_errs.append("missing column suggest_output.delivered_at")
        else:
            dt = column_type(conn, "suggest_output", "delivered_at")
            if dt == "timestamp":
                warns.append("suggest_output.delivered_at is TIMESTAMP (server TZ dependent). Prefer DATETIME + app UTC policy.")
            elif dt not in ("datetime","timestamp"):
                warns.append(f"suggest_output.delivered_at type is {dt} (expected DATETIME)")

        # release lock at end of checks
        lock.release()

        if errs or db_errs:
            print("[PREFLIGHT] ❌ FAIL")
            for e in errs:   print(" - ERR:", e)
            for e in db_errs:print(" - ERR(DB):", e)
            for w in warns: print(" - WARN:", w)
            return 1

        print("[PREFLIGHT] ✅ PASS")
        for w in warns: print(" - WARN:", w)
        return 0

    finally:
        if conn:
            conn.close()

# =============== CLI ===============
#def run_mode(registry_path, days, limit):
def run_mode(registry_path, days, limit, unit_id=None):
    
    from math import isfinite
    # registry load & validate
    try:
        with open(registry_path, "r", encoding="utf-8") as f:
            reg = json.load(f)
    except Exception as e:
        print(f"[RUN] ❌ ERR: registry load error: {e}", file=sys.stderr)
        return 1
    errs, warns, _ = validate_registry(reg)
    if errs:
        print("[RUN] ❌ ERR: registry validation failed")
        for e in errs: print(" - ERR:", e)
        for w in warns: print(" - WARN:", w)
        return 1

    conn = None
    lock = FileLock()
    try:
        conn = db_connect()
        if not lock.try_acquire():
            payload = {"type":"t2_error","reason":"lock_busy","at":now_iso_utc()}
            try: insert_u_log(conn, payload)
            except Exception: pass
            print("[RUN] ❌ ERR: lock_busy: another t2_writer is running", file=sys.stderr)
            return 1

        from_day, to_day = day_range_from_days(days)
        rows = all_rows_params(conn, """
            SELECT t1.unit_id, t1.day_jst, t1.run_id, t1.t1_text, t1.t1_features, t1.created_at
            FROM t1_log t1
            LEFT JOIN t2_log t2
              ON t2.unit_id=t1.unit_id AND t2.day_jst=t1.day_jst AND t2.run_id=t1.run_id
            WHERE t2.run_id IS NULL
              AND t1.day_jst BETWEEN %s AND %s
            ORDER BY t1.unit_id, t1.day_jst, t1.created_at ASC, t1.run_id ASC
            LIMIT %s
        """, (from_day, to_day, int(limit)))

        print(f"[RUN] INFO: picked {len(rows)} candidates in [{from_day}, {to_day}] limit={int(limit)}")

        # batch smoke stats
        processed = 0
        unknown_vals = []
        max_vals = []
        nextk_cache = {}

        for r in rows:
            unit_id = r["unit_id"]; day_jst = r["day_jst"]; run_id = r["run_id"]
            t1_text = r.get("t1_text") or ""
            created_at = r.get("created_at")
            ca_date = _date_of_created_at(created_at)

            # 遅着runの警告（停止しない）
            if ca_date and isinstance(day_jst, (datetime,)) and hasattr(day_jst, "date"):
                dj = day_jst.date()
            else:
                dj = day_jst
            if ca_date and dj and ca_date > dj:
                try:
                    insert_u_log(conn, {"type":"t2_warn","reason":"out_of_order_arrival",
                                        "unit_id":unit_id,"run_id":run_id,"day_jst":str(day_jst),
                                        "created_at":str(created_at)})
                except Exception:
                    pass

            # 推論（スタブ）
            p_z = _det_pz_stub(reg, t1_text, run_id)

            # G1: 1件スモーク
            s = sum(p_z.values())
            bad = (abs(s-1.0) > 1e-6) or any((v<0 or v>1 or not isfinite(v)) for v in p_z.values())
            if bad:
                try:
                    insert_u_log(conn, {"type":"t2_error","reason":"pz_invalid",
                                        "unit_id":unit_id,"run_id":run_id,"sum":s})
                except Exception:
                    pass
                continue

            # order_k
            k = get_next_k_cached(conn, nextk_cache, unit_id, day_jst)

            # 冪等UPSERT
            with conn.cursor() as cur:
                row_id = str(uuid.uuid4())
                cur.execute("""
                    INSERT IGNORE INTO t2_log
                      (id, unit_id, day_jst, run_id, order_k, p_z, tag_set_hash, model_version, created_at)
                    VALUES
                      (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (row_id, unit_id, day_jst, run_id, k,
                      json.dumps(p_z, ensure_ascii=False),
                      reg.get("tag_set_hash"), reg.get("t2_model_version")))

            processed += 1
            unknown_vals.append(float(p_z.get("unknown", 0.0)))
            max_vals.append(float(max(p_z.values()) if p_z else 0.0))

        # 10件スモーク（バッチ）
        if processed >= 10:
            unk_ratio = sum(unknown_vals)/len(unknown_vals) if unknown_vals else 0.0
            mean_max = sum(max_vals)/len(max_vals) if max_vals else 0.0
            if (unk_ratio >= 0.6) or not (0.3 <= mean_max <= 0.9):
                try:
                    insert_u_log(conn, {"type":"t2_error","reason":"batch_smoke_fail",
                                        "unknown_ratio":unk_ratio,"mean_max":mean_max})
                except Exception:
                    pass
                print(f"[RUN] ❌ ERR: batch_smoke_fail unknown_ratio={unk_ratio:.3f} mean_max={mean_max:.3f}", file=sys.stderr)
                return 1

        # カバレッジ率（参考）
        #totals = one_params(conn, "SELECT COUNT(*) AS n FROM t1_log WHERE day_jst BETWEEN %s AND %s", (from_day, to_day)) or {"n":0}
        #dones  = one_params(conn, "SELECT COUNT(*) AS n FROM t2_log WHERE day_jst BETWEEN %s AND %s", (from_day, to_day)) or {"n":0}
        #cov = (dones["n"]/totals["n"]) if totals["n"] else 1.0
        #print(f"[RUN] ✅ DONE: inserted_or_seen={processed}, coverage={cov:.3f} ({dones['n']}/{totals['n']})")
        # unit_id + day_jst ごとにカバー率を計算する
       # unit_id + day_jst ごとにカバー率を計算する
  # unit_id + day_jst ごとにカバー率を計算する
        # unit_id + day_jst ごとにカバー率を計算する
        totals = one_params(conn,
            "SELECT COUNT(*) AS n FROM t1_log WHERE unit_id=%s AND day_jst BETWEEN %s AND %s",
            (unit_id, from_day, to_day)) or {"n":0}

        dones  = one_params(conn,
            "SELECT COUNT(*) AS n FROM t2_log WHERE unit_id=%s AND day_jst BETWEEN %s AND %s",
            (unit_id, from_day, to_day)) or {"n":0}

        cov = (dones["n"]/totals["n"]) if totals["n"] else 1.0
        print(f"[RUN] ✅ DONE for unit={unit_id}: inserted_or_seen={processed}, coverage={cov:.3f} ({dones['n']}/{totals['n']})")

                   
        

        
        return 0

    finally:
        try: lock.release()
        except Exception: pass
        if conn: conn.close()


def jst_today_date():
    return (datetime.utcnow() + timedelta(hours=9)).date()



def day_range_from_days(days):
    to_d = jst_today_date()
    from_d = to_d - timedelta(days=int(days or 0))
    return from_d.isoformat(), to_d.isoformat()



def one_params(conn, sql, params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()



def all_rows_params(conn, sql, params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()



def get_next_k_cached(conn, cache, unit_id, day_jst):
    key = (unit_id, str(day_jst))
    if key in cache:
        val = cache[key]
        cache[key] = val + 1
        return val
    row = one_params(conn,
        "SELECT COALESCE(MAX(order_k),0)+1 AS next_k FROM t2_log WHERE unit_id=%s AND day_jst=%s",
        (unit_id, day_jst)
    )
    nk = int(row["next_k"] if row and row.get("next_k") is not None else 1)
    cache[key] = nk + 1
    return nk



def _date_of_created_at(x):
    if isinstance(x, datetime):
        return x.date()
    try:
        return datetime.strptime(str(x).split(".")[0], "%Y-%m-%d %H:%M:%S").date()
    except Exception:
        try:
            return datetime.strptime(str(x), "%Y-%m-%d").date()
        except Exception:
            return None



def _softmax(xs):
    from math import exp
    if not xs:
        return []
    m = max(xs)
    exps = [exp(x - m) for x in xs]
    s = sum(exps) or 1.0
    return [v / s for v in exps]



def _det_pz_stub(reg, text, run_id):
    import hashlib
    from math import isfinite
    """
    決定的BoW→softmaxスタブ。
    - tag_list（'unknown'除く）の各タグ出現回数 + 小さなハッシュjitterでscore
    - softmax→残差を 'unknown' に付与→合計を厳密に1へ
    """
    tags = [t for t in reg.get("tag_list", []) if isinstance(t, str)]
    if not tags:
        return {"unknown": 1.0}
    base = [t for t in tags if t != "unknown"]
    txt = (text or "")
    txt_l = txt.lower()
    scores = []
    for t in base:
        c = txt_l.count(t.lower())
        h = hashlib.sha1((run_id + "|" + t + "|" + reg.get("tag_set_hash", "")).encode("utf-8")).hexdigest()
        jitter = (int(h[:8], 16) / (2**32)) * 0.01  # <= 0.01
        scores.append(c + 0.001 + jitter)
    probs = _softmax(scores)
    pz = {t: float(round(p, 12)) for t, p in zip(base, probs)}
    s = sum(pz.values())
    pz["unknown"] = float(round(max(0.0, 1.0 - s), 12))
    # 最終クランプ＆再正規化
    for k, v in list(pz.items()):
        if not isfinite(v) or v < 0:
            pz[k] = 0.0
    tot = sum(pz.values())
    if tot <= 0:
        return {"unknown": 1.0}
    for k in pz:
        pz[k] = float(pz[k] / tot)
    return pz



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preflight", action="store_true", help="run registry/DB checks only")
    ap.add_argument("--run", action="store_true", help="process unprocessed t1 runs into t2_log (idempotent)")
    ap.add_argument("--registry", default="/opt/gamma_writer/analysis_run.json")
    ap.add_argument("--days", type=int, default=0)
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--unit", type=str, default=None, help="target unit_id (e.g., u1, u2, u3)")
    args = ap.parse_args()

    if args.preflight:
        code = preflight(args.registry)
        sys.exit(code)
    if args.run:
        #code = run_mode(args.registry, args.days, args.limit)
        code = run_mode(args.registry, args.days, args.limit, args.unit)
        sys.exit(code)
    ap.print_help()
    sys.exit(2)

if __name__ == "__main__":
    main()
# =============== Run mode (main processing) ===============
import hashlib
from math import exp, isfinite

