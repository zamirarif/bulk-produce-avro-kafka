# bulk-produce-avro-kafka

To bulk test kafka 

hit to execute : 
http://localhost:8080/produce?number=10

number = number of record you want to produce 

Schema Registry URL for AVRO message :

http://localhost:8081/subjects/test-topic-com.schema.account.Account/versions/latest

mvn clean compile
