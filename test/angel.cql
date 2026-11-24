create table if not exists angel (
	id          Int64,
	role        Int64,
	key         UUID,
	created     DateTime64(3),
	ip          IPv4,
	smalls      UInt8,
	name        String,
	about       Array(String),
	ipes        Array(IPv4),
	music       Array(Int64),
	links       Map(String, String),
	stats       Map(String, Int64)
)
engine = ReplacingMergeTree(role)
order by tuple()
partition by toYYYYMM(created);
