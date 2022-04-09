import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.time.LocalDateTime;

public class FlinkConsumer {

    public static void main(String[] args) throws Exception {
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        // Build Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("100ktopic")
                .setGroupId("my-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        // Create DataStream of Strings that will be delivered from Kafka and add timestamp using
        // map-Function
        DataStream<String> messageStream = env.fromSource(
                source,
                WatermarkStrategy.forMonotonousTimestamps(),
                "Kafka Source")
                .map((MapFunction<String, String>) value -> LocalDateTime.now() + ": " + value);

        // Create the FileSink which will write the output to a file
        FileSink<String> sink = FileSink.forRowFormat(
                        new Path("/Users/gledon/Downloads/kafka_2.12-3.1.0/output"),
                        new SimpleStringEncoder<String>()
                )
                .withBucketAssigner(new BasePathBucketAssigner<>())
                .build();

        messageStream.sinkTo(sink);
        env.execute("Consumer");
    }
}
