package be.vdab.vdp

import be.vdab.vdp.InosEventProcessing.InosEventProcessor
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

import javax.annotation.PostConstruct
import java.time.Duration

@SpringBootApplication
class VDPEventProcessor {

    @Lazy Properties properties = {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "inos-processor");
        //props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 'kafka01-mlb1abt.ops.vdab.be:9092,kafka02-mlb1abt.ops.vdab.be:9092,kafka03-mlb1abt.ops.vdab.be:9092')
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 'localhost:9092')
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props
    }()
    static void main(String[] args) {
        SpringApplication application = new SpringApplication(VDPEventProcessor.class);

        application.run(args);
    }

    @PostConstruct
    def StartStreamProcessing(){

        StreamsBuilder builder = new StreamsBuilder();


        InosEventProcessor processor = new InosEventProcessor()
        processor.Setup(builder)

        Topology topo = builder.build();
        KafkaStreams streams =new KafkaStreams(topo,properties);

        streams.start()

        Runtime.getRuntime().addShutdownHook({
            println("closing stream")
            streams.close(Duration.ofSeconds(10))
        })
    }


}
