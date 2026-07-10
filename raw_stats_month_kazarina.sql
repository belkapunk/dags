-- 1. Таблица для сырых данных за месяц
CREATE TABLE IF NOT EXISTS raw_stats_month_kazarina (
    lti_user_id VARCHAR(255),
    is_correct BOOLEAN,
    attempt_type VARCHAR(100),
    created_at TIMESTAMP,
    oauth_consumer_key VARCHAR(255),
    lis_result_sourcedid VARCHAR(255),
    lis_outcome_service_url TEXT
);

-- 2. Таблица для агрегированных данных за месяц
CREATE TABLE IF NOT EXISTS agg_stats_month_kazarina (
    lti_user_id VARCHAR(255),
    attempt_type VARCHAR(100),
    cnt_attempt INT,
    cnt_correct INT,
    date TIMESTAMP
);
