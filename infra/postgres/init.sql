-- registro: tabla fuente, solo lectura para el ejecutor (D1). PK id creciente (supuesto del PDF).
CREATE TABLE registro (
    id          BIGSERIAL PRIMARY KEY,
    provider_id TEXT NOT NULL,
    endpoint    TEXT NOT NULL
);

-- 10k filas / 50 proveedores (entorno dev del PDF). id 7 queda fuera de la allowlist: mensaje poison → DLQ.
INSERT INTO registro (provider_id, endpoint)
SELECT 'p-' || lpad((g % 50)::text, 3, '0'),
       CASE WHEN g = 7 THEN '/internal/admin'
            ELSE '/providers/p-' || lpad((g % 50)::text, 3, '0') || '/records/' || g END
FROM generate_series(1, 10000) AS g;

-- Usuario de base con solo SELECT sobre registro (Seguridad · Entradas).
CREATE ROLE dqe_ro LOGIN PASSWORD 'dqe_ro';
GRANT CONNECT ON DATABASE dqe TO dqe_ro;
GRANT USAGE ON SCHEMA public TO dqe_ro;
GRANT SELECT ON registro TO dqe_ro;
