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
    AND r1.tstamp=(SELECT max(r2.tstamp) FROM inventory_runs r2 where r1.id=r2.id);

CREATE VIEW latest_inventory AS
  SELECT * FROM latest_inventory_runs runs, inventory_items items, file_references files
    WHERE items.inventory_run = runs.id AND items.file=files.id 


select items.id, root.path, files.rel_path, items.modified, items.filesize, root.rep_factor  from latest_inventory_runs runs, inventory_items items, file_references files, inventory_roots root where items.inventory_run = runs.id and items.file=files.id and runs.root_path=root.id limit 10;

select items.id, root.path, files.rel_path, items.modified, items.filesize as filesize, root.rep_factor  from latest_inventory_runs runs, inventory_items items, file_references files, inventory_roots root where items.inventory_run = runs.id and items.file=files.id and runs.root_path=root.id order by filesize desc;

-- Sum all file sizes
select sum(filesize) from (select items.id, root.path, files.rel_path, items.modified, items.filesize as filesize, root.rep_factor  from latest_inventory_runs runs, inventory_items items, file_references files, inventory_roots root where items.inventory_run = runs.id and items.file=files.id and runs.root_path=root.id ) fs ;
