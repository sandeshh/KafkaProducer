import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by sandesh on 2/24/16.
 */
public class Campaign {

    /**
     * Generate a random list of ads and campaigns
     */

    public void Create( int numCampaigns, int numAdsPerCampaign, String outputFile) throws IOException {

        File f = new File(outputFile);
        if(f.exists() && !f.isDirectory()) {
            return ;
        }

        FileWriter fileWriter = new FileWriter(outputFile) ;

        for (int i = 0; i < numCampaigns; i++) {
            String campaign = UUID.randomUUID().toString();

            for (int j = 0; j < numAdsPerCampaign; j++) {

                fileWriter.write(campaign + " " + UUID.randomUUID().toString() + "\n");
            }
        }

        fileWriter.flush();
        fileWriter.close();
    }
}
