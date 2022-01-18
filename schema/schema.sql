
-- add the new table and views
-- the following assumes that you are in this directory
-- and you have the openaq-db repo cloned (not as a submodule)
\i ../../openaq-db/openaqdb/tables/exports.sql
\i ../../openaq-db/openaqdb/idempotent/exports_views.sql

-- and now we can populate the export logs
SELECT reset_export_logs();
