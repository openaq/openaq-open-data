
-- add the new table and views
-- the following assumes that you are in this directory
-- and you have the openaq-db repo cloned (not as a submodule)
\i ../../openaq-db/openaqdb/tables/exports.sql
\i ../../openaq-db/openaqdb/idempotent/exports_views.sql

-- and now we can populate the export logs
-- The reset method will both populate the export log table and
-- set the exported_on field to null
SELECT reset_export_logs();
