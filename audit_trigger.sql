--Создание таблиц
DROP TABLE IF EXISTS users_audit;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

--Функция логирования изменений
CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.name IS DISTINCT FROM OLD.name THEN
        INSERT INTO users_audit(user_id, changed_at, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, CURRENT_TIMESTAMP, current_user, 'name', OLD.name, NEW.name);
    END IF;

    IF NEW.email IS DISTINCT FROM OLD.email THEN
        INSERT INTO users_audit(user_id, changed_at, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, CURRENT_TIMESTAMP, current_user, 'email', OLD.email, NEW.email);
    END IF;

    IF NEW.role IS DISTINCT FROM OLD.role THEN
        INSERT INTO users_audit(user_id, changed_at, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, CURRENT_TIMESTAMP, current_user, 'role', OLD.role, NEW.role);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Создание триггера
DROP TRIGGER IF EXISTS trg_log_user_changes ON users;

CREATE TRIGGER trg_log_user_changes
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_changes();

--Установка расширения pg_cron
CREATE EXTENSION IF NOT EXISTS pg_cron;

--Функция экспорта данных в CSV за текущий день
DROP FUNCTION IF EXISTS export_users_audit();

CREATE OR REPLACE FUNCTION export_users_audit()
RETURNS void AS $$
DECLARE
    file_path TEXT;
BEGIN
    file_path := '/tmp/users_audit_export_' || to_char(CURRENT_DATE, 'YYYY_MM_DD') || '.csv';

    EXECUTE format($fmt$
        COPY (
            SELECT * FROM users_audit WHERE changed_at::date = CURRENT_DATE
        ) TO '%s' WITH CSV HEADER
    $fmt$, file_path);
END;
$$ LANGUAGE plpgsql;

--Создание нового задания pg_cron на 3:00 ночи
SELECT cron.schedule('daily_export_users_audit', '0 3 * * *', $$SELECT export_users_audit();$$);




