-- docker-compose runs all 4 shards as separate DATABASES on one Postgres container,
-- to keep the demo to a single DB service. In production each shard is its own
-- Postgres instance (ideally on separate hosts) - only SHARD_URLS changes.
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;
CREATE DATABASE shard_2;
CREATE DATABASE shard_3;
