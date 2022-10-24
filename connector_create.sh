curl --location --request POST 'localhost:8083/connectors' \
	--header 'Content-Type: application/json' \
	--data-raw '{
		"name":"hdfs-sink","config":
			{
			"connector.class":"io.confluent.connect.hdfs.HdfsSinkConnector",
			"tasks.max":3,
			"topics":"hashtags-avro",
			"hdfs.url":"hdfs://namenode:9000",
			"flush.size":30,
			"logs.dir":"/logs/",
			"topics.dir":"/",
			"format.class":"io.confluent.connect.hdfs.avro.AvroFormat",
			"partitioner.class":"io.confluent.connect.storage.partitioner.FieldPartitioner",
			"partition.field.name":"date"
			}
		}'

