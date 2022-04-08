import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class FlinkProducer {
    public static void main(String[] args) throws Exception {
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Build DataStreamSource which will read the input from the desired file
        DataStreamSource<String> source =
                env.fromSource(
                        FileSource.forRecordStreamFormat(
                                new TextLineFormat(),
                                new Path("file:///Users/gledon/Downloads/datadriverTestData/100kFiles.txt")
                        ).monitorContinuously(Duration.ofMillis(5)).build(),
                        WatermarkStrategy.noWatermarks(),
                        "FileSource"
                );

        // Create KafkaSink which will publish the read lines to Kafka
        KafkaSink<String> sink = KafkaSink.<String>builder().setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("100ktopic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        source.sinkTo(sink);
        env.execute("Producer");
    }
}