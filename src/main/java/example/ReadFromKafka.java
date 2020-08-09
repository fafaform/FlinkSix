package example;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.events.Event;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.EventObject;
import java.util.Properties;


public class ReadFromKafka {
    public static String PERSON = "PERSON";
    public static String SPEECH = "SPEECH";
    //// VARIABLES
    public static String KAFKA_CONSUMER_TOPIC = "speech_in";
    public static String KAFKA_PRODUCER_TOPIC = "speech_out";
    //// TEST IN CLUSTER
    public static String BOOTSTRAP_SERVER = "172.30.74.84:9092,172.30.74.85:9092,172.30.74.86:9092";
//    public static String BOOTSTRAP_SERVER = "poc01.kbtg:9092,poc02.kbtg:9092,poc03.kbtg:9092";
    //// TEST IN MY LOCAL
//    public static String BOOTSTRAP_SERVER = "localhost:9092";

    public static Logger LOG = LoggerFactory.getLogger(ReadFromKafka.class);
    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);
        properties.setProperty("log.message.timestamp.type", "LogAppendTime");

        //// READ FROM EARLIEST HERE
//        properties.setProperty("auto.offset.reset", "earliest");

        //// END VARIABLES
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ////////////////////////////////////////////////////////////////
        //// RECEIVE JSON
        FlinkKafkaConsumer<ObjectNode> JsonSource = new FlinkKafkaConsumer(KAFKA_CONSUMER_TOPIC, new JSONKeyValueDeserializationSchema(false), properties);
//        JsonSource.assignTimestampsAndWatermarks(
//                WatermarkStrategy
//                        .<ObjectNode>forBoundedOutOfOrderness(Duration.of(1, MINUTES))
//        );
        JsonSource.assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<ObjectNode>() {
                    private final long maxOutOfOrderness = 60000; // 60 seconds
                    private long currentMaxTimestamp;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(ObjectNode jsonNodes, long l) {
                        long timestamp = l;
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        return timestamp;
                    }
                }
        );
        DataStream<Tuple2<String,String>> messageStream = env.addSource(JsonSource).flatMap(new FlatMapFunction<ObjectNode, Tuple2<String,String>>() {
            @Override
            public void flatMap(ObjectNode s, Collector<Tuple2<String, String>> collector) throws Exception {
                collector.collect(new Tuple2<String, String>(
                        s.get("value").get(PERSON).asText(),
                        s.get("value").get(SPEECH).asText()
                ));
            }
        }).rebalance();
        DataStream<Tuple2<String,String>> resultStream = messageStream.keyBy(0).process(new CountWithTimeoutFunction());
        resultStream.rebalance();

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer(KAFKA_PRODUCER_TOPIC, new ProducerStringSerializationSchema(KAFKA_PRODUCER_TOPIC), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        resultStream.process(new ProcessFunction<Tuple2<String, String>, String>() {
            @Override
            public void processElement(Tuple2<String, String> stringLongLongTuple3, Context context, Collector<String> collector) throws Exception {
                collector.collect("{\"PERSON\":\"" + stringLongLongTuple3.f0 + "\""
                        + ",\"SPEECH\":\"" + stringLongLongTuple3.f1 + "\""
                        +"}");
            }
        }).addSink(myProducer);

        env.execute("Flink Six");
    }
}
