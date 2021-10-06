package test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Instantion {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        int partitions = 1;
        short replicationFactor = 1;
        List<NewTopic> topics = new ArrayList<NewTopic>();

        String uri = "mongodb://137.121.170.248:31182";
        MongoClient mongoClient = MongoClients.create(uri);
        MongoDatabase database = mongoClient.getDatabase("test");
        try {
            Scanner sc = new Scanner(new File("areas1.txt"));
            String line;
            while(sc.hasNextLine()) {
                line = sc.nextLine();
               database.createCollection(line);
                topics.add(new NewTopic(line, partitions, replicationFactor));
            }
            mongoClient.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Properties properties = new Properties();
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "137.121.170.248:31090,137.121.170.248:31091,137.121.170.248:31092"
        );

        try (Admin admin = Admin.create(properties)) {
            CreateTopicsResult result = admin.createTopics(topics);

            KafkaFuture<Void> future = result.values().get(topics.get(topics.size()-1));
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
