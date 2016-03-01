import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;


/**
 * Simple Producer
 */
public class SyslogProducer {

    public static void main(String[] args) throws
            IOException, InterruptedException {

        if ( args.length != 6 ) {
            System.out.println("topic path_to_file redis numberOfCampaigns adsPerCampaigns brokerslist");
            return ;
        }

        String topic = args[0];
        String path = args[1] ;
        String redis = args[2] ;
        Integer numberOfCampaigns = Integer.parseInt(args[3]);
        Integer adsPerCampaign = Integer.parseInt(args[4]);

        String brokersList = args[5];

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

        Campaign campaign = new Campaign();
        campaign.Create(numberOfCampaigns, adsPerCampaign, path);

        // Redis Update
        RedisHelper redisHelper = new RedisHelper();
        redisHelper.init(redis);
        redisHelper.fillDB(path);

        final EventGenerator eventGenerator = new EventGenerator();
        eventGenerator.init(path);

        while ( true ) {
            String ads = eventGenerator.generateElement() ;

            producer.send( new KeyedMessage<String, String>(topic, ads ) ) ;
            // System.out.println(ads);
        }
    }
}
