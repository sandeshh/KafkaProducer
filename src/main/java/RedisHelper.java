import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by sandesh on 2/24/16.
 */
public class RedisHelper {

    private String host ;
    private Jedis jedis ;

    public void init( String host ) {
        this.host = host ;
        jedis = new Jedis(host) ;
    }

    public void clear( Integer db ) {
        jedis.flushDB() ;
    }

    public void fillDB( String fileName ) throws IOException {

        Path filePath = new Path(fileName);
        Configuration configuration = new Configuration();
        FileSystem fs;
        fs = FileSystem.newInstance(filePath.toUri(), configuration);
        FSDataInputStream inputStream = fs.open(filePath);
        BufferedReader bufferedReader;

        try {

            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = bufferedReader.readLine()) != null) {

                String mapping[] = line.split("\\s+");

                if ( mapping.length != 2 ) continue ;

                jedis.sadd("campaigns", mapping[0]);
                jedis.set(mapping[1], mapping[1]) ;
            }
        } catch (Exception e) {

        }
    }
}
