-- Schema for backup_inventory

-- A description of a root path that we inventory.
create table inventory_roots (
  id serial primary key,
  uuid text, -- unique identifier for a root (useful for backups that are mounted on different machines from time to time).
  hostname text, -- where the inventory was performed
  name text, -- human readable name for the root
  path text, -- path as mounted on the host
  description text, -- detailed description of this root
  type text, -- master (authoritative/latest), backup (old copy from a master), archive (backup without a master)
  rep_factor smallint DEFAULT 1 -- redundancy factor for this root
);

create table inventory_runs (
  id serial primary key,
  root_path integer REFERENCES inventory_roots(id),
  tstamp TIMESTAMP WITH TIME ZONE,
  duration numeric, -- seconds
  version text
);

-- Hashes table
create table hashes (
  id bigserial primary key,
  hash bytea -- sha-256 hash (use decode(hash,'hex') for input)
);

-- Filenames (relative)
create table file_references (
  id bigserial primary key,
  rel_path text -- relative path
);

-- Instances of files seen during an inventory run
create table inventory_items (
  id bigserial primary key,
  inventory_run integer REFERENCES inventory_runs(id),
  hash bigint REFERENCES hashes(id),
  file bigint REFERENCES file_references(id),
  modified TIMESTAMP WITH TIME ZONE,
  filesize bigint
);

CREATE UNIQUE INDEX hash_idx ON hashes (hash);
CREATE UNIQUE INDEX file_idx ON file_references (rel_path);
CREATE INDEX inventory_items_hash ON inventory_items (hash);
CREATE INDEX inventory_items_file ON inventory_items (file);

-- View with only the latest and recent inventory runs.
-- roots that haven't been updated in 60 days drop off.
CREATE VIEW latest_inventory_runs AS 
  SELECT * FROM inventory_runs r1 WHERE r1.tstamp >now()-interval '60 days'
    AND r1.tstamp=(SELECT max(r2.tstamp) FROM inventory_runs r2 where r1.root_path=r2.root_path);
-- Most recent inventory run, regardless of age
CREATE VIEW all_latest_inventory_runs AS 
  SELECT * FROM inventory_runs r1 WHERE
    r1.tstamp=(SELECT max(r2.tstamp) FROM inventory_runs r2 where r1.root_path=r2.root_path);

-- Sum all file sizes
--select sum(filesize) from (select items.id, root.path, files.rel_path, items.modified, items.filesize as filesize, root.rep_factor  from latest_inventory_runs runs, inventory_items items, file_references files, inventory_roots root where items.inventory_run = runs.id and items.file=files.id and runs.root_path=root.id ) fs ;

-- Find hash mismatches
create temporary view inventory_items_w_root AS SELECT i.id,r.root_path, i.inventory_run, i.hash, i.file,i.modified,i.filesize FROM inventory_items i INNER JOIN inventory_runs r ON i.inventory_run=r.id;
select * from inventory_items_w_root i1 where i1.id in (select id from inventory_items_w_root i2 where i1.root_path=i2.root_path and i1.modified=i2.modified and i1.hash!=i2.hash);


create temporary view latest_inventory_items_w_root AS SELECT i.id,r.root_path, i.inventory_run, i.hash, i.file,i.modified,i.filesize FROM inventory_items i INNER JOIN all_latest_inventory_runs r ON i.inventory_run=r.id;
create temporary view inventory_items_w_root AS SELECT i.id,r.root_path, i.inventory_run, i.hash, i.file,i.modified,i.filesize FROM inventory_items i INNER JOIN inventory_runs r ON i.inventory_run=r.id;
select * from latest_inventory_items_w_root i1 where i1.id in (select id from inventory_items_w_root i2 where i1.root_path=i2.root_path and i1.modified=i2.modified and i1.hash!=i2.hash);




