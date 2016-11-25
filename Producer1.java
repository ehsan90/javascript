package org.di.datavault.test.producer;


import java.util.Properties;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.AvroSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer1 {

    Random rnd = new Random();

    public static void main(String[] args) throws Exception {

        Producer1 producer1 = new Producer1();
        producer1.produce();

    }

    private void produce() {

        Namer namer = new Namer();
       Colorer color = new Colorer();

        Properties props = gerProps();

        Producer<String, GenericRecord> producer = new KafkaProducer(props);

        AvroSchema avroSchema = new AvroSchema();

        Schema schema = avroSchema.getSchema1();


        for(int i= 0; i<1000000; i++)
        {
            GenericRecord avroRecord = new GenericData.Record(schema);

            avroRecord.put("name", namer.getName(2)+"[" + i + "]");
            avroRecord.put("favorite_number", getNumber());
            avroRecord.put("favorite_color", color.getColor());

            System.out.println(avroRecord.toString());

            ProducerRecord<String, GenericRecord> record = new ProducerRecord<String, GenericRecord>("test3", avroRecord);

            producer.send(record);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }


        System.out.println("Message sent successfully");
        producer.close();
    }



    private Properties gerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        return props;
    }

    private String getColor() {

        return "";
    }

    private int getNumber()
    {

        return rnd.nextInt(100);
    }

}
