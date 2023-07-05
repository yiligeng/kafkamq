package com.maishibing.dml;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @Project ：kafka_mq
 * @File ：KafkaTopicDML.java
 * @Author ：gyl
 * @Date ：2023/7/3 5:28 PM
 */
//必须将node02:9092,node03:9092,node04:9092加入到hosts文件中
public class KafkaTopicDML {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("hello world");
        Properties props=new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "node02:9092,node03:9092,node04:9092");
        KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) KafkaAdminClient.create(props);

        //创建topic信息
        CreateTopicsResult createTopicsResult= kafkaAdminClient.createTopics(Arrays.asList(new NewTopic("topic02",3,(short)3)));
        //同步等待创建完成
        createTopicsResult.all().get();





        //删除topic
        DeleteTopicsResult deleteTopicsResult = kafkaAdminClient.deleteTopics(Arrays.asList("topic02"));
        deleteTopicsResult.all().get();

        ListTopicsResult listTopicsResult = kafkaAdminClient.listTopics();
        Set<String> names=listTopicsResult.names().get();
        for (String name:names){
            System.out.println(name);
        }


        // 查看Topic详细信息
        DescribeTopicsResult dtr=kafkaAdminClient.describeTopics(Arrays.asList("topic01"));
        Map<String,TopicDescription> map=dtr.all().get();
        for (Map.Entry<String,TopicDescription> entry:map.entrySet()){
            System.out.println(entry.getKey()+"\t"+entry.getValue());
        }




        kafkaAdminClient.close();
    }
}
