package KafkaDataModel;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class TopicInfo {
	private String topicName;
	private Integer partionCount;
	private Map<String,Map<Integer,Integer>> topicData = null;
	
	public TopicInfo(){
		topicName = new String();
		partionCount = new Integer(0);
		topicData = new HashMap<String,Map<Integer,Integer>>();
		
	}
	
	public Integer getPartionCount() {
		return partionCount;
	}
	public void setPartionCount(Integer partionCount) {
		this.partionCount = partionCount;
	}
	
	public String getTopicName() {
		return topicName;
	}
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	
	public void setTopicDetails(Integer partion,Integer offSet) {
		
		if(topicData.containsKey(topicName)) {
			Map<Integer,Integer> details = topicData.get(topicName);
			if(!details.containsKey(partion)) {
				details.put(partion, offSet);
			}
			topicData.put(topicName, details);
		} else {
			Map<Integer,Integer> entry = new HashMap<Integer,Integer>();
			entry.put(partion, offSet);
			topicData.put(topicName, entry);
			//topicData.put(topicName, new HashMap<Integer,Integer>(partion,offSet));
		}
	}
	public Map<String,Map<Integer,Integer>> getTopicPartitonOffSetDetails() {
	     return topicData;
	}

	public void DumpPartitionTopicOffSetDetails() {
		
		Iterator<Map.Entry<String, Map<Integer,Integer>>> itr = topicData.entrySet().iterator();  
		while(itr.hasNext()) {
			Map.Entry<String, Map<Integer,Integer>> currEntry = itr.next();
			System.out.println("Topic Name:" + currEntry.getKey()+ "                     Partiton Count:" + partionCount);
			System.out.println("-------------------------------------------------------------------");
			Iterator<Entry<Integer, Integer>> partIter = currEntry.getValue().entrySet().iterator();
			while(partIter.hasNext()) {
				Map.Entry<Integer, Integer> curr = partIter.next();
				System.out.println("Current Partiton:"  + curr.getKey()+ "               With Offset:" + curr.getValue());
			}
			System.out.println("-------------------------------------------------------------------");
		}
	}
}
