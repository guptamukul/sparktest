package sparktest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SparkTest {

	public static void main(String[] args) {

		if (args.length < 5) {
			System.err.println("Usage: SparkTest <kafkabroker> <sparkmaster> <topics> <consumerGroup> <Duration>");
			System.exit(1);
		}

		String kafkaBroker = args[0];
		String sparkMaster = args[1];
		String topics = args[2];
		String consumerGroupID = args[3];
		String durationSec = args[4];

		int duration = 0;

		try {
			duration = Integer.parseInt(durationSec);
		} catch (Exception e) {
			System.err.println("Illegal duration");
			System.exit(1);
		}

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));

		SparkConf conf = null;

		conf = new SparkConf().setMaster(sparkMaster).setAppName("DirectStreamDemo");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(duration));

		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", kafkaBroker);
		kafkaParams.put("group.id", consumerGroupID);

		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		JavaDStream<String> processed = messages.map(new Function<Tuple2<String, String>, String>() {

			@Override
			public String call(Tuple2<String, String> arg0) throws Exception {

				Thread.sleep(7000);
				return arg0._2;
			}
		});

		processed.print(90);

		try {
			jssc.start();
			jssc.awaitTermination();
		} catch (Exception e) {

		} finally {
			jssc.close();
		}
	}
}
