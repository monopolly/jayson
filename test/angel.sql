--drop table angel;
create table if not exists angel (
	id          bigint primary key,
	role        bigint,
	key         uuid,
	created     timestamptz,
	smalls      smallint,
	music       bigint[] default '{}',
	links       jsonb default '{}'::jsonb,
	stats       jsonb default '{}'::jsonb,
	unique (role, key)
);
