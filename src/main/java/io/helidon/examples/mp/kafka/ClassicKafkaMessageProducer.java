package io.helidon.examples.mp.kafka;

import org.oracle.okafka.clients.producer.KafkaProducer;
import org.oracle.okafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

@ApplicationScoped
public class ClassicKafkaMessageProducer {

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
    //@Inject
    //@ConfigProperty(name = "oracle.instance.name")
    //String instanceName;
    @Inject
    @ConfigProperty(name = "oracle.net.tns_admin")
    String tnsAdmin;
    @Inject
    @ConfigProperty(name = "oracle.user.name")
    String user;
    @Inject
    @ConfigProperty(name = "oracle.password")
    String pwd;

    KafkaProducer<String, String> producer;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public ClassicKafkaMessageProducer() {
    }

    @PostConstruct
    private void init() {
        // Set properties used to configure the producer
        Properties props = new Properties();
        props.put("oracle.user.name", user);
        props.put("oracle.password", pwd);
        props.put("oracle.service.name", serviceName);
        //props.put("oracle.instance.name", instanceName);
        props.put("oracle.net.tns_admin", tnsAdmin); //eg: "/user/home" if ojdbc.properies file is in home  
        props.put("security.protocol", protocol);
        props.put("bootstrap.servers", broker);
        //props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");
        props.put("linger.ms", 1000);
        // Set how to serialize key/value pairs
        props.setProperty("key.serializer", "org.oracle.okafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.oracle.okafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    public void publishMessage(String message) throws IOException {

        // Send the message to the topic
        try {
            producer.send(new ProducerRecord<String, String>(topic, 0, "HELIDON", message));
            logger.warning("Sent new oKafka message: "+message);
            //producer.commitTransaction();
            //producer.flush();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new IOException(ex.toString());
        }
    }

    @PreDestroy
    public void cleanUp() {
        producer.close();
    }
}
