-- CC Switch 开发数据库初始化脚本
-- 生成日期：2026-06-20
-- 用途：创建所有必要的表结构

-- 1. Providers 表（供应商管理）
CREATE TABLE IF NOT EXISTS providers (
    id TEXT NOT NULL,
    app_type TEXT NOT NULL,
    name TEXT NOT NULL,
    settings_config TEXT NOT NULL,
    website_url TEXT,
    category TEXT,
    created_at INTEGER,
    sort_index INTEGER,
    notes TEXT,
    icon TEXT,
    icon_color TEXT,
    meta TEXT NOT NULL DEFAULT '{}',
    is_current BOOLEAN NOT NULL DEFAULT 0,
    in_failover_queue BOOLEAN NOT NULL DEFAULT 0,
    PRIMARY KEY (id, app_type)
);

-- 2. Provider Endpoints 表（供应商端点）
CREATE TABLE IF NOT EXISTS provider_endpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_id TEXT NOT NULL,
    app_type TEXT NOT NULL,
    url TEXT NOT NULL,
    added_at INTEGER,
    FOREIGN KEY (provider_id, app_type) REFERENCES providers(id, app_type) ON DELETE CASCADE
);

-- 3. MCP Servers 表（MCP 服务器管理）
CREATE TABLE IF NOT EXISTS mcp_servers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    server_config TEXT NOT NULL,
    description TEXT,
    homepage TEXT,
    docs TEXT,
    tags TEXT NOT NULL DEFAULT '[]',
    enabled_claude BOOLEAN NOT NULL DEFAULT 0,
    enabled_codex BOOLEAN NOT NULL DEFAULT 0,
    enabled_gemini BOOLEAN NOT NULL DEFAULT 0,
    enabled_opencode BOOLEAN NOT NULL DEFAULT 0,
    enabled_hermes BOOLEAN NOT NULL DEFAULT 0
);

-- 4. Prompts 表（提示词管理）
CREATE TABLE IF NOT EXISTS prompts (
    id TEXT NOT NULL,
    app_type TEXT NOT NULL,
    name TEXT NOT NULL,
    content TEXT NOT NULL,
    description TEXT,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    created_at INTEGER,
    updated_at INTEGER,
    PRIMARY KEY (id, app_type)
);

-- 5. Skills 表（技能管理）
CREATE TABLE IF NOT EXISTS skills (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    directory TEXT NOT NULL,
    repo_owner TEXT,
    repo_name TEXT,
    repo_branch TEXT DEFAULT 'main',
    readme_url TEXT,
    enabled_claude BOOLEAN NOT NULL DEFAULT 0,
    enabled_codex BOOLEAN NOT NULL DEFAULT 0,
    enabled_gemini BOOLEAN NOT NULL DEFAULT 0,
    enabled_opencode BOOLEAN NOT NULL DEFAULT 0,
    enabled_hermes BOOLEAN NOT NULL DEFAULT 0,
    installed_at INTEGER NOT NULL DEFAULT 0,
    content_hash TEXT,
    updated_at INTEGER NOT NULL DEFAULT 0
);

-- 6. Skill Repos 表（技能仓库管理）
CREATE TABLE IF NOT EXISTS skill_repos (
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    branch TEXT NOT NULL DEFAULT 'main',
    enabled BOOLEAN NOT NULL DEFAULT 1,
    PRIMARY KEY (owner, name)
);

-- 7. Settings 表（应用设置）
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- 8. Proxy Config 表（代理配置）
CREATE TABLE IF NOT EXISTS proxy_config (
    app_type TEXT PRIMARY KEY CHECK (app_type IN ('claude','codex','gemini')),
    proxy_enabled INTEGER NOT NULL DEFAULT 0,
    listen_address TEXT NOT NULL DEFAULT '127.0.0.1',
    listen_port INTEGER NOT NULL DEFAULT 15721,
    enable_logging INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 0,
    auto_failover_enabled INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    streaming_first_byte_timeout INTEGER NOT NULL DEFAULT 60,
    streaming_idle_timeout INTEGER NOT NULL DEFAULT 120,
    non_streaming_timeout INTEGER NOT NULL DEFAULT 600,
    circuit_failure_threshold INTEGER NOT NULL DEFAULT 4,
    circuit_success_threshold INTEGER NOT NULL DEFAULT 2,
    circuit_timeout_seconds INTEGER NOT NULL DEFAULT 60,
    circuit_error_rate_threshold REAL NOT NULL DEFAULT 0.6,
    circuit_min_requests INTEGER NOT NULL DEFAULT 10,
    default_cost_multiplier TEXT NOT NULL DEFAULT '1',
    pricing_model_source TEXT NOT NULL DEFAULT 'response',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 9. Proxy Request Logs 表（代理请求日志）
CREATE TABLE IF NOT EXISTS proxy_request_logs (
    request_id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    app_type TEXT NOT NULL,
    model TEXT NOT NULL,
    request_model TEXT,
    pricing_model TEXT,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    input_cost_usd TEXT NOT NULL DEFAULT '0',
    output_cost_usd TEXT NOT NULL DEFAULT '0',
    cache_read_cost_usd TEXT NOT NULL DEFAULT '0',
    cache_creation_cost_usd TEXT NOT NULL DEFAULT '0',
    total_cost_usd TEXT NOT NULL DEFAULT '0',
    latency_ms INTEGER NOT NULL,
    first_token_ms INTEGER,
    duration_ms INTEGER,
    status_code INTEGER NOT NULL,
    error_message TEXT,
    session_id TEXT,
    provider_type TEXT,
    is_streaming INTEGER NOT NULL DEFAULT 0,
    cost_multiplier TEXT NOT NULL DEFAULT '1.0',
    created_at INTEGER NOT NULL,
    data_source TEXT NOT NULL DEFAULT 'proxy'
);

-- 10. Model Pricing 表（模型定价）
CREATE TABLE IF NOT EXISTS model_pricing (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    app_type TEXT NOT NULL,
    model_id TEXT NOT NULL,
    input_price_per_1m TEXT NOT NULL DEFAULT '0',
    output_price_per_1m TEXT NOT NULL DEFAULT '0',
    cache_read_price_per_1m TEXT NOT NULL DEFAULT '0',
    cache_creation_price_per_1m TEXT NOT NULL DEFAULT '0',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (provider_id, app_type) REFERENCES providers(id, app_type) ON DELETE CASCADE
);

-- 11. Provider Health 表（供应商健康检查）
CREATE TABLE IF NOT EXISTS provider_health (
    provider_id TEXT NOT NULL,
    app_type TEXT NOT NULL,
    last_check INTEGER,
    status TEXT NOT NULL DEFAULT 'unknown',
    latency_ms INTEGER,
    error_message TEXT,
    PRIMARY KEY (provider_id, app_type),
    FOREIGN KEY (provider_id, app_type) REFERENCES providers(id, app_type) ON DELETE CASCADE
);

-- 12. Usage Daily Rollups 表（用量统计）
CREATE TABLE IF NOT EXISTS usage_daily_rollups (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    app_type TEXT NOT NULL,
    date INTEGER NOT NULL,
    model TEXT NOT NULL,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost_usd TEXT NOT NULL DEFAULT '0',
    request_count INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (provider_id, app_type) REFERENCES providers(id, app_type) ON DELETE CASCADE
);

-- 13. Session Log Sync 表（会话日志同步）
CREATE TABLE IF NOT EXISTS session_log_sync (
    id TEXT PRIMARY KEY,
    app_type TEXT NOT NULL,
    session_id TEXT NOT NULL,
    provider_id TEXT,
    model TEXT,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost_usd TEXT NOT NULL DEFAULT '0',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- 14. Stream Check Logs 表（流式检查日志）
CREATE TABLE IF NOT EXISTS stream_check_logs (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    app_type TEXT NOT NULL,
    check_type TEXT NOT NULL,
    status TEXT NOT NULL,
    latency_ms INTEGER,
    error_message TEXT,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (provider_id, app_type) REFERENCES providers(id, app_type) ON DELETE CASCADE
);

-- 15. Proxy Live Backup 表（代理实时备份）
CREATE TABLE IF NOT EXISTS proxy_live_backup (
    id TEXT PRIMARY KEY,
    app_type TEXT NOT NULL,
    config TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- 插入默认设置
INSERT OR IGNORE INTO settings (key, value) VALUES ('schema_version', '11');
INSERT OR IGNORE INTO settings (key, value) VALUES ('app_version', '3.16.3');

-- 初始化 Proxy Config（三个应用）
INSERT OR IGNORE INTO proxy_config (app_type, max_retries, streaming_first_byte_timeout, streaming_idle_timeout, non_streaming_timeout, circuit_failure_threshold, circuit_success_threshold, circuit_timeout_seconds, circuit_error_rate_threshold, circuit_min_requests)
VALUES 
    ('claude', 6, 90, 180, 600, 8, 3, 90, 0.7, 15),
    ('codex', 3, 60, 120, 600, 4, 2, 60, 0.6, 10),
    ('gemini', 3, 60, 120, 600, 4, 2, 60, 0.6, 10);

-- 完成初始化
