package twitter;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

// import io.confluent.kafka.serializers.KafkaAvroDeserializer;


// Maven package : mvn -U clean package shade:shade

public class App
{
    public static void main( String[] args ) throws Exception
    {
        // Maven package : mvn -U clean package shade:shade
        // sudo update-alternatives --config java

        TopologyBuilder builder = new TopologyBuilder();

        // ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
        // Kafka Spout
        // KafkaSpoutConfig<String, String> ksc_twitter = KafkaSpoutConfig.builder("localhost:9092", "hashtags-avro")
        KafkaSpoutConfig<String, String> ksc_twitter = KafkaSpoutConfig.builder(
                        "172.31.2.27:9092, 172.31.2.27:9093, 172.31.2.27:9094",
                        "hashtags-avro")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "storm")
                .setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
                .setProp("schema.registry.url", "http://172.31.13.207:8081")
                .build();

        builder.setSpout("kafka-spout", new KafkaSpout<>(ksc_twitter), 1);

        builder.setBolt("mongodb-bolt", new MongoDbBolt(), 1).shuffleGrouping("kafka-spout");
        // ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

        StormTopology topology = builder.createTopology();
        Config config = new Config();
        config.setMessageTimeoutSecs(3);
        String topologyName = "twitter";

        if (args.length > 0 && args[0].equals("remote")) {
            StormSubmitter.submitTopology(topologyName, config, topology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, topology);
        }


        // https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html
//        Logger LOG = LoggerFactory.getLogger(App_v2.class);
//
//        Properties props = new Properties();
//
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "storm");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
//        props.put("schema.registry.url", "http://localhost:8081");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        String topic = "hashtags-avro";
//        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
//        consumer.subscribe(Arrays.asList(topic));
//
//        try {
//            while (true) {
//                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
//                for (ConsumerRecord<String, GenericRecord> record : records) {
//                    LOG.info("offset = {}, key = {}, value = {} \n", record.offset(), record.key(), record.value());
//                    JSONObject json = (JSONObject) new JSONParser().parse(String.valueOf(record.value()));
//                    String unique_id = (String) json.get("unique_id");
//                    LOG.info("unique_id       = {}" , unique_id);
//                }
//            }
//        } finally {
//            consumer.close();
//        }


    }
}
