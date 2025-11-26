-- DAIV System Database Schema

-- t1_log: Time-bound physical reality (Behavioral Log)
-- 時間に縛られた物理的現実（行動ログ）
CREATE TABLE t1_log (
    unit_id VARCHAR(64) NOT NULL COMMENT 'Observer ID',
    day_jst DATE NOT NULL COMMENT 'Timestamp (Date)',
    run_id VARCHAR(64) NOT NULL COMMENT 'Unique Execution ID',
    t1_text TEXT COMMENT 'Raw qualitative data',
    t1_features JSON COMMENT 'Quantified features',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (unit_id, day_jst, run_id),
    UNIQUE KEY uk_t1_unit_day_run (unit_id, day_jst, run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- t2_log: Time-discharged semantic structure (Inner Vision)
-- 時間から解放された意味構造（内部ビジョン）
CREATE TABLE t2_log (
    id VARCHAR(64) NOT NULL PRIMARY KEY,
    unit_id VARCHAR(64) NOT NULL,
    day_jst DATE NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    order_k INT NOT NULL COMMENT 'Sequential order (Tag Index) without physical time',
    p_z JSON NOT NULL COMMENT 'Probability distribution of semantic tags (Omega projection)',
    tag_set_hash VARCHAR(64) COMMENT 'Version hash of tag definitions',
    model_version VARCHAR(64),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_t2_unit_day_run (unit_id, day_jst, run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- u_log: System execution logs
-- システム実行ログ
CREATE TABLE u_log (
    id VARCHAR(64) PRIMARY KEY,
    unit_id VARCHAR(64),
    run_id VARCHAR(64),
    day_jst DATE,
    u JSON COMMENT 'System logs and error reports',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
