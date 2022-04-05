import java.util.concurrent.ExecutionException;

//import Utils.KafkaAdminWrapperUtils;
import Utils.KafkaAdminWrapperUtilsM;

public class KafkaMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// TODO Auto-generated method stub
		KafkaAdminWrapperUtilsM ktu = new KafkaAdminWrapperUtilsM();
		ktu.CreateAdminClient();
		ktu.ListKafkaTopics();
		//ktu.DisplayTopics();
		//ktu.DescribeTopicInformation("testing_2");
		ktu.DescribeTopics();
		ktu.DisplayTopicDetails();
		ktu.CreateConsumerClient();
		ktu.DisplayTopicDetails();
		ktu.readMessages();
		ktu.CloseConsumerClient();

	}

}
