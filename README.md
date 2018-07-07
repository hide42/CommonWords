# CommonWordsAudioList
CREATE KEYSPACE test
WITH REPLICATION = { 
'class' : 'SimpleStrategy', 
'replication_factor' : 1 
};


CREATE TABLE test.test (
id bigint PRIMARY KEY,
data blob
);


kafka/bin/kafka-topics.sh —create \
—zookeeper localhost:2181 \
—replication-factor 1 —partitions 1 \
—topic test
