/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.helidon.examples.mp.kafka;

/*
** OKafka Java Client version 0.8.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
 */
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.oracle.okafka.clients.consumer.ConsumerRecord;
import org.oracle.okafka.clients.consumer.ConsumerRecords;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

public class SampleConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();

        /*
                props.put("oracle.instance.name", "DB21c"); //name of the oracle databse instance
		props.put("oracle.service.name", "pdb21c.sub02161014000.publicvcn.oraclevcn.com");	//name of the service running on the instance    
		props.put("oracle.net.tns_admin", "c:/temp"); //eg: "/user/home" if ojdbc.properies file is in home  
	    
		props.put("bootstrap.servers", "152.70.55.224:1521"); //ip address or host name where instance running : port where instance listener running
         */
        props.put("oracle.instance.name", "orcl"); //name of the oracle databse instance
        props.put("oracle.service.name", "mypdb.de.oracle.com");	//name of the service running on the instance    
        props.put("oracle.net.tns_admin", "c:/temp"); //eg: "/user/home" if ojdbc.properies file is in home  

        props.put("bootstrap.servers", "127.0.0.1:1521"); //ip address or host name where instance running : port where instance listener running
        props.put("oracle.user.name", "teqdemo");
        props.put("oracle.password", "MyPwd3210");

        props.put("batch.size", 200);
        props.put("linger.ms", 100);
        props.put("buffer.memory", 335544);

        String topic = "TEQ";

        props.put("group.id", "consumers");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10000");

        props.put("key.deserializer",
                "org.oracle.okafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.oracle.okafka.common.serialization.StringDeserializer");
        props.put("max.poll.records", 100);

        KafkaConsumer<String, String> consumer = null;

        consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));

        ConsumerRecords<String, String> records = null;

        try {

            records = consumer.poll(Duration.ofMillis(15000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic = , partition=  ,key= , value = \n"
                        + record.topic() + "  " + record.partition() + "  " + record.key() + "  " + record.value());
                System.out.println(".......");
            }

            consumer.commitSync();

        } catch (Exception ex) {
            ex.printStackTrace();

        } finally {
            consumer.close();
        }

    }

}
