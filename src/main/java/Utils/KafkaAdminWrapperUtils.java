package Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import KafkaDataModel.TopicInfo;

public class KafkaAdminWrapperUtils {

	public static AdminClient admin;
	public static KafkaConsumer<String, String> tConsumer;
	private Set<String> kTopics = null;
	private TopicInfo to =null;
	private List<TopicPartition> tp=null;
	private  KafkaConsumer<String, String> consumer = null;
	public KafkaAdminWrapperUtils(){
		to = new TopicInfo();
		tp=  new ArrayList<TopicPartition>();
	}
	
	private Map<String, Object> CreateAndSetProperties() {
		Map<String, Object> properties = new HashMap<String, Object>();
	    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.25:9092");
		properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000000000);
		return properties;
	}
	
	public void CreateAdminClient() {
		System.out.println("Created Started");
		admin = AdminClient.create(CreateAndSetProperties());
		System.out.println("Created Completed");
	}
	
	public void CreateConsumerClient() {
	     Properties props = new Properties();
	     props.setProperty("bootstrap.servers", "192.168.0.25:9092");
	     //props.setProperty("group.id", "test");
	     props.setProperty("enable.auto.commit", "false");
	     props.setProperty("auto.commit.interval.ms", "1000");
	     props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     consumer = new KafkaConsumer<String, String>(props);
		 consumer.assign(tp);
     	 ListIterator<TopicPartition> tpIter = tp.listIterator();
		 while(tpIter.hasNext()) {
	         TopicPartition tpInfo = tpIter.next();
	         Long actualEndOffset = consumer.endOffsets(tp).get(tpInfo);
		     System.out.println("Topic:" + tpInfo.topic() + " Partiton :" +
			  tpInfo.partition()  + "OffSet : " + actualEndOffset); 
		 }
	}
	
	public void CloseConsumerClient() {
		consumer.close();
	}
	
	public void DisplayTopics() {
		 Iterator<String> iter = kTopics.iterator();
		 while(iter.hasNext()) {
			 System.out.println(iter.next());
		 }
	}
	
	public void DescribeTopics() throws InterruptedException, ExecutionException {
		 Iterator<String> iter = kTopics.iterator();
		 while(iter.hasNext()) {
			 DescribeTopicInformation(iter.next());
		 }
	}
	
	public void DisplayTopicDetails() {
		to.DumpPartitionTopicOffSetDetails();
	}
	
	public void DescribeTopicInformation(String topic) throws InterruptedException, ExecutionException {
		
		//DescribeTopicsResult result = admin.describeTopics(Arrays.asList(topic));
		DescribeTopicsResult result = admin.describeTopics(kTopics);
		Map<String, KafkaFuture<TopicDescription>>  values = result.values();
		KafkaFuture<TopicDescription> topicDescription = values.get(topic);
		//int partitions = topicDescription.get().partitions().size();
		List<TopicPartitionInfo> tpo  = topicDescription.get().partitions();
		
		System.out.println(tpo.get(0));
		ListIterator<TopicPartitionInfo> topIter  = tpo.listIterator();
		to.setTopicName(topic);
		to.setPartionCount(topicDescription.get().partitions().size());
		while (topIter.hasNext()) {
			TopicPartitionInfo tInfo = topIter.next();
			to.setTopicDetails(tInfo.partition(), -1);
			TopicPartition part = new TopicPartition(topic, tInfo.partition());
			tp.add(part);
		}
	}
	
	public void ListKafkaTopics() throws InterruptedException, ExecutionException {
		 ListTopicsOptions options = new ListTopicsOptions();
		 options.listInternal(false);
		 ListTopicsResult topics = admin.listTopics(options);
		 
		 kTopics = topics.names().get();
	}
	
}
