create table if not exists events (
	id          UInt64,
	country     LowCardinality(String),
	ip          IPv4,
	path        String,
	tags        Array(String)
)
engine = ReplacingMergeTree(id)
primary key (id)
order by (id, country)
partition by intDiv(id,1000000);
