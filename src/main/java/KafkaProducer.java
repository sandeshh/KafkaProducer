import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * Simple Producer
 */
public class KafkaProducer {

    public static void main(String[] args) throws
            IOException, InterruptedException {

        if ( args.length != 3 ) {
            System.out.println("topic path_to_file brokerslist");
            return ;
        }

        final String topic = args[0];
        String path = args[1] ;
        String brokersList = args[2];

        Properties prop = new Properties();
        prop.put("metadata.broker.list", brokersList );
        prop.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig producerConfig = new ProducerConfig(prop);
        final Producer<String, String> producer = new Producer<String, String>(
                producerConfig);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                producer.close();
                System.out.println("done ...");
            }
        });

        final EventGenerator eventGenerator = new EventGenerator();
        eventGenerator.init(path);

        ExecutorService threadPool = Executors.newFixedThreadPool(10);

        for ( int i = 0 ; i < 10 ; ++i ) {

            threadPool.submit(new Runnable() {
                public void run() {
                    while ( true ) {
                        String ads = eventGenerator.generateElement() ;

                        producer.send( new KeyedMessage<String, String>(topic, ads ) ) ;
                        // System.out.println(ads);
                    }
                }
            });
        }

        threadPool.shutdown();
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
}
