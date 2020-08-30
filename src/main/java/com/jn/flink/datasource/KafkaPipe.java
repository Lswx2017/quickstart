package com.jn.flink.datasource;


import com.jn.flink.enums.Ecommenum;
import com.jn.flink.pojo.KafkaMsg1;
import com.jn.flink.util.Rand;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.producer.*;


import java.io.*;
import java.util.Properties;


public class KafkaPipe {

    public static void main(String[] args) {
        //WriteKafka("192.168.31.232:19092", "UserBehaviors", 100000);
        WriteKafka2("192.168.31.232:19092", "UserBehaviors");
    }


    public static void WriteKafka(String server, String topic, Integer amount) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", server);
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int i = amount;
        Long userId;
        Long itemId;
        Integer categoryId;
        String behavior;
        Long timestamp = 0L;

        while(i != 0) {
            userId = Rand.randLongRange(100000L, 999999L);
            itemId = Rand.randLongRange(234550L, 234567L);
            categoryId = Rand.randIntRange(1234567, 5678912);
            behavior = Rand.randomEnum(Ecommenum.class, 1, 4).getStatus();

            if(i == amount) timestamp = Rand.randLongRange(1511658000L, 1511690400L);
            else
                timestamp = timestamp + 1L;
            KafkaMsg1 msg = new KafkaMsg1(userId, itemId, categoryId, behavior, timestamp);

            ProducerRecord<String, String> record = new ProducerRecord<String , String>(topic, msg.toString());
            producer.send(record);

            i--;
        }

        producer.close();

    }

    public static void WriteKafka2(String server, String topic) {



        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.31.232:19092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 定义一个kafka producer
        KafkaProducer producer = new KafkaProducer<String, String>(properties);
        File file = new File("E:\\codebase\\cepdemo\\src\\main\\resources\\UserBehavior.csv");
        FileReader fr = null;
        BufferedReader br = new BufferedReader(fr);
        String line = "";
        try {
            fr = new FileReader(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        while(true) {
            try {
                if (!((line = br.readLine()) != null)) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            ProducerRecord record = new ProducerRecord<String, String>(topic, line);
            producer.send(record);
        }

        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        producer.close();

    }

}
