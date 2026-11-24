--drop table angel;
create table if not exists angel (
	id          bigint primary key,
	role        bigint,
	key         uuid,
	created     timestamptz,
	ip          inet,
	smalls      smallint,
	name        text,
	about       text[] default '{}',
	ipes        inet[] default '{}',
	music       bigint[] default '{}',
	links       jsonb default '{}'::jsonb,
	stats       jsonb default '{}'::jsonb,
	unique (role, key)
);
