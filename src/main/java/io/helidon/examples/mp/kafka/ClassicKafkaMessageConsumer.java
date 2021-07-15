package io.helidon.examples.mp.kafka;

import io.helidon.examples.mp.spi.Startup;
import org.oracle.okafka.clients.consumer.ConsumerRecord;
import org.oracle.okafka.clients.consumer.ConsumerRecords;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

@ApplicationScoped
@Startup
public class ClassicKafkaMessageConsumer {

    @Inject
    @ConfigProperty(name = "kafka.broker")
    String broker;
    @Inject
    @ConfigProperty(name = "kafka.topic")
    String topic;
    @Inject
    @ConfigProperty(name = "kafka.group")
    String group;
    @Inject
    @ConfigProperty(name = "security.protocol")
    String protocol;
    @Inject
    @ConfigProperty(name = "oracle.service.name")
    String serviceName;
    @Inject
    @ConfigProperty(name = "oracle.user.name")
    String oraUser;
    @Inject
    @ConfigProperty(name = "oracle.password")
    String oraPwd;
    //@Inject
    //@ConfigProperty(name = "oracle.instance.name")
    //String instanceName;
    @Inject
    @ConfigProperty(name = "oracle.net.tns_admin")
    String tnsAdmin;

    //KafkaConsumer<String, String> consumer = null;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public ClassicKafkaMessageConsumer() {
    }

    @PostConstruct
    private void init() {
        // Create a consumer
        // Configure the consumer
        Properties props = new Properties();
        props.put("oracle.service.name", serviceName);
        props.put("oracle.user.name", oraUser);
        props.put("oracle.password", oraPwd);
        //props.put("oracle.instance.name", instanceName);
        props.put("oracle.net.tns_admin", tnsAdmin); //eg: "/user/home" if ojdbc.properies file is in home  
        props.put("security.protocol", protocol);
        props.put("bootstrap.servers", broker);
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");
        // Set how to serialize key/value pairs
        props.put("key.deserializer", "org.oracle.okafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.oracle.okafka.common.serialization.StringDeserializer");
        props.put("max.poll.records", 100);
        System.out.println("now connecting to Oracle DB AQ/TEQ");
        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        startKafkaMessageConsumer(props);
        //listenTopic(props);
    }

    private void startKafkaMessageConsumer(Properties props) {
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.execute(() -> {
            logger.info(String.format("starting expensive task thread %s", Thread.currentThread().getName()));
            listenTopic(props);
        });
    }

    public void listenTopic(Properties props) {
        ConsumerRecords<String, String> records = null;

        while (true) {
           KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
           consumer.subscribe(Arrays.asList(topic));
            try {
                records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    logger.warning(String.format("Received oKafka message: topic = %s, partition= %s ,key= %s, value = %s\n",
                            record.topic(), record.partition(), record.key(), record.value()));
                }

                //consumer.commitSync();
            } catch (Exception ex) {
                logger.log(java.util.logging.Level.FINE, "could not poll from oKafka", ex);
                logger.log(java.util.logging.Level.INFO, "could not poll from oKafka");
            } finally {
                consumer.close();
            }
        }
    }

    @PreDestroy
    public void cleanUp() {
        //consumer.close();
    }

    public void dummy() {

    }
}
