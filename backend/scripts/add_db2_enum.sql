-- Add 'db2' to the databasetype enum (PostgreSQL).
-- Run this if the enum was created before db2 was added (e.g. connect to cdctest and run):
-- psql -h <host> -U <user> -d cdctest -f add_db2_enum.sql
ALTER TYPE databasetype ADD VALUE IF NOT EXISTS 'db2';
