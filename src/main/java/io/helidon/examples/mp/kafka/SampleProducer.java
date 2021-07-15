/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.helidon.examples.mp.kafka;

/*/*
** OKafka Java Client version 0.8.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/


import java.util.Properties;

import org.oracle.okafka.clients.producer.KafkaProducer;
import org.oracle.okafka.clients.producer.ProducerRecord;


public class SampleProducer {
	
	public static void main(String[] args) {			
			
		
		String topic = "TEQ" ;
		
        KafkaProducer<String,String> prod = null;		
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
		props.put("batch.size", 200);
		props.put("linger.ms", 100);
		props.put("buffer.memory", 335544);
		props.put("key.serializer", "org.oracle.okafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.oracle.okafka.common.serialization.StringSerializer");	
		props.put("oracle.user.name","teqdemo");
                props.put("oracle.password","BrunhildeZ32##");
		System.out.println("Creating producer now 1 2 3..");	
		  
		prod=new KafkaProducer<String, String>(props);

		System.out.println("Producer created.");
		
		 try {
			 int i;	
			 for(i = 0; i < 10; i++)				 
			     prod.send(new ProducerRecord<String, String>(topic ,0, i+"000","This is new message"+i));
 
		     System.out.println("Sent "+ i + "messages");	 
                     //Thread.currentThread().sleep(10000);
		 } catch(Exception ex) {
			  
			 System.out.println("Failed to send messages:");
			 ex.printStackTrace();
		 }
		 finally {
			 prod.close();
		 }

		
	}
}
