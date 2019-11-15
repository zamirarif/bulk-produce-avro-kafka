package com.multiple.schema.producer.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.schema.account.Account;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;

@SpringBootApplication
@RestController
public class BulkProduceAvroKafka {

	final static Logger logger = Logger.getLogger(BulkProduceAvroKafka.class);
	@Value("${bootstrap.url}")
	String bootstrap;
	@Value("${registry.url}")
	String registry;
	public static void main(String[] args) {
		SpringApplication.run(BulkProduceAvroKafka.class, args);
			
	}
	//   /movie?recordKey=my-key&genre=Action&movieName=Gone in Sixty Seconnds
	@RequestMapping("/produce")
	public String movie(@RequestParam("number") String number)
	{
		String ret = null;
		//=topic;
		try
		{
			ret += "<br>Using Bootstrap : " + bootstrap;
			ret += "<br>Using Bootstrap : " + registry;
			
			Properties properties = new Properties();
			// Kafka Properties
			properties.setProperty("bootstrap.servers", bootstrap);
			properties.setProperty("acks", "all");
			properties.setProperty("retries", "10");
			// Avro properties
			properties.setProperty("key.serializer", StringSerializer.class.getName());
			properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
			properties.setProperty("schema.registry.url", registry);
			properties.setProperty("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());
			
			Producer<String,Account> producer = new KafkaProducer<String,Account>(properties);
			for(int i = 0; i<=Integer.parseInt(number);i++){
				System.out.println("Record Produced : "+populateAccount().toString());
				ProducerRecord<String, Account> producerRecord = new ProducerRecord<String, Account>("test-topic","key"+i, populateAccount());
				producer.send(producerRecord);
				//ret +=producerRecord;
				
				
			}
			producer.close();
			
		}
		catch(Exception ex){ ret+="<br>"+ex.getMessage();}
		
		return ret;
	}
	
	private Account populateAccount(){
		 
		Account account = Account.newBuilder()
				.setAccount("1234567890")
				.setName("test name")
				.setAddress("123 Main Street")
				.setCity("Nottingham")
				.setCountry("England")
				.setPostcode("NG22 8BG")
				.build();
			return account;
    }
}
