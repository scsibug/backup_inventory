delete from inventory_items; delete from inventory_runs;  delete from hashes; delete from file_references; delete from inventory_roots;
ALTER SEQUENCE file_references_id_seq RESTART WITH 1;
ALTER SEQUENCE hashes_id_seq RESTART WITH 1;
ALTER SEQUENCE inventory_items_id_seq RESTART WITH 1;
ALTER SEQUENCE inventory_roots_id_seq  RESTART WITH 1;
ALTER SEQUENCE inventory_runs_id_seq RESTART WITH 1;
