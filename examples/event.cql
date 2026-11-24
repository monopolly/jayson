create table if not exists events (
	id          UInt64,
	country     LowCardinality(String),
	ip          IPv4,
	path        String,
	tags        Array(String)
)
engine = MergeTree()
order by (id)
primary key (id);
