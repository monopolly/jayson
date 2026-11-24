create table if not exists clicks (
	id       Int64,
	link     String,
	tags     Array(String)
)
engine = MergeTree()
order by (id)
primary key (id);
