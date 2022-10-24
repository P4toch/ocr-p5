package twitter;


import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.apache.avro.generic.GenericRecord;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;


public class MongoDbBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDbBolt_v3.class);
    private static OutputCollector outputCollector;
    private static MongoDatabase mongoDB;
    
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;

        // Creating Credentials
        MongoCredential credential;
        credential = MongoCredential.createCredential("storm", "twitter", "storm-pwd".toCharArray());

        // MongoClient mongoClient = new MongoClient("localhost", 27017);
        // MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoClient mongoClient = new MongoClient(new ServerAddress("172.31.2.199", 27017),
                Arrays.asList(credential));

        mongoDB = mongoClient.getDatabase("twitter");

        boolean databaseExists = mongoClient.listDatabaseNames().into(new ArrayList<String>()).contains("twitter");
        LOG.info("databaseExists = {}", databaseExists);

        boolean collectionExists = mongoClient.getDatabase("twitter").listCollectionNames()
                .into(new ArrayList<String>()).contains("hashtags_storm");
        LOG.info("collectionExists = {}", collectionExists);

    }

    @Override
    public void execute(Tuple input) {

        LOG.info("---------------------------------------- MongoDbBolt ----------------------------------------");


        try {
            GenericRecord record = (GenericRecord) input.getValue(4);

            JSONObject json = (JSONObject) new JSONParser().parse(String.valueOf(record));

            String unique_id = (String) json.get("unique_id");
            String hashtag = (String) json.get("hashtag");
            String date = (String) json.get("date");
            String hour = (String) json.get("hour");
            // DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            // Date datetime = formatter.parse((String) json.get("datetime"));
            Long timestamp = (Long) json.get("timestamp");

            LOG.info("json            = {}", json.toJSONString());
            LOG.info("kafka topic     = {}" , input.getValue(0));
            LOG.info("kafka partition = {}" , input.getValue(1));
            LOG.info("kafka offset    = {}" , input.getValue(2));
            LOG.info("unique_id       = {}" , unique_id);
            LOG.info("hashtag         = #{}", hashtag);
            LOG.info("timestamp       = {}" , timestamp);
            LOG.info("date            = {}" , date);
            LOG.info("hour            = {}" , hour);

            Document doc = new Document();
            doc.append("_id", new ObjectId());
            doc.append("unique_id", unique_id);
            doc.append("timestamp", timestamp);
            doc.append("hashtag", hashtag);
            doc.append("date", date);
            doc.append("hour", hour);

            try {
                mongoDB.getCollection("twitter.hashtags_storm").insertOne(doc);
            } catch (MongoException except) {
                except.printStackTrace();
            }

            Values values = new Values(json.toJSONString(), unique_id);
            outputCollector.emit(input, values);
            outputCollector.ack(input);

            // Utils.sleep(150);

        } catch (ParseException e) {
            e.printStackTrace();
            outputCollector.fail(input);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tuple", "unique_id"));
    }

}
