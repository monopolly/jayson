create table if not exists hits (
	id          Int64,
	iuser       Int64,
	country     LowCardinality(String),
	ip          IPv4,
	ip6         IPv6,
	path        String,
	ok          Bool,
	views       Int64,
	created     DateTime64(3),
	tags        Array(String),
	codes       Array(Int64),
	sizes       Array(UInt64),
	attrs       Map(String, String),
	counts      Map(String, Int64),
	ifrom       String
)
engine = ReplacingMergeTree(views)
primary key (id)
order by (id, country)
partition by toYYYYMM(created)
ttl created + interval 30 day;
