package com.data.niu.source;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.data.niu.entity.ClickEntity;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import static java.lang.Thread.sleep;
import com.data.niu.util.Property;

/**
 * 往kafka里写点击流Json数据
 * Created by dataniu on 2019/10/3.
 */

public class ClickDataSource {

    public static final String topic = "click_log";

    public static void writeToKafka() throws InterruptedException {

        Properties props = Property.getKafkaProperties("test");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(props);

        String[] visitor_list  = new String[] {"A1","A2","A3"};
        int[] user_list= new int[]{1,2,3};
        String[] click_type_list =  new String[] {"view","view","view","addToCart","buy"};
        int[] product_list = new int[]{100,102,103,104,105,106,107,108,109};
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式

        for (int i = 1; i <= 10; i++) {
            String vistor_id=visitor_list[(int) (Math.random() * visitor_list.length)];
            int user_id=user_list[(int) (Math.random() * user_list.length)];
            String click_type=click_type_list[(int) (Math.random() * click_type_list.length)];
            int product_id=product_list[(int) (Math.random() * product_list.length)];
            String time = df.format(new Date());// new Date()为获取当前系统时间
            sleep(2000);

            ClickEntity clickEntity = new ClickEntity(vistor_id,user_id,time,click_type,product_id);
            String jsonClickEntity = JSON.toJSONString(clickEntity);
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, jsonClickEntity);
            producer.send(record);
            System.out.println("发送数据: " + jsonClickEntity);
        }
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }

}
