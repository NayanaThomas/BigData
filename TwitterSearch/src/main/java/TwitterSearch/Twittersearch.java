package TwitterSearch;

import org.apache.hadoop.fs.Path;
import twitter4j.Query;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.QueryResult;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.RateLimitStatus;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;

public class Twittersearch {
    public static void main(String[] args) throws Exception {
        String destination = args[0];
        String topic = "papermate";
//        String topic = args[1];
//        String consumerKey = args[2];
//        String consumerSecret = args[3];
//        String accessToken = args[4];
//        String accessTokenSecret = args[5];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(destination), conf);
        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("SV7BRw3ilz5eSULTyJf1LWsuy")
                .setOAuthConsumerSecret("P4L2sZluIGmLEaqIx4P578OVnXGraVYfk55V1ZkDgrFavc2gAf")
                .setOAuthAccessToken("1093319920207708166-gTkrJ4p2inAeZB6KaZ87Jx1eziU6R8")
                .setOAuthAccessTokenSecret("MZ2BIr6apVPWj9D99gy876tcAbb0HOmee0ESkCdXwoCaI");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        ArrayList<String> sinceDates = new ArrayList<String>();
        sinceDates.add("2019-02-07");
        sinceDates.add("2019-02-06");
        sinceDates.add("2019-02-05");
        sinceDates.add("2019-02-04");
        sinceDates.add("2019-02-03");
        sinceDates.add("2019-02-02");
        ArrayList<String> untilDates = new ArrayList<String>();
        untilDates.add("2019-02-08");
        untilDates.add("2019-02-07");
        untilDates.add("2019-02-06");
        untilDates.add("2019-02-05");
        untilDates.add("2019-02-04");
        untilDates.add("2019-02-03");


        for(int count = 1; count<=6;count++) {
            String fileName = "TwitterText"+count+".txt";
            String outputPath = destination+"/"+fileName;
            Path destPath = new Path(outputPath);
            try {
                if (fs.exists(destPath)) {
                    fs.delete(destPath, true);
                }
            }
            catch (Exception e){
                System.out.println(e);
            }
            //FSDataOutputStream fin = fs.create(destPath);
            FileOutputStream fos = new FileOutputStream(fileName, true);
            //OutputStream out = fs.create(new Path(outputPath));

            String sinceDate = sinceDates.get(count-1);
            String untilDate = untilDates.get(count-1);

            Query query = new Query(topic);
            query.setSince(sinceDate);
            query.setUntil(untilDate);
            QueryResult result = twitter.search(query);

            do {
                List<Status> tweets = result.getTweets();
                for (Status tweet: tweets) {
                    System.out.println(tweet.getCreatedAt());
                    System.out.println("@" + tweet.getUser().getScreenName() + ":" + tweet.getText());
                    //fin.writeUTF(tweet.getText());
                    byte[] mybytes = tweet.getText().getBytes();
                    fos.write(mybytes);
                }
                query = result.nextQuery();
                if (query != null) {
                    result = twitter.search(query);
                    RateLimitStatus rateLimitStatus = result.getRateLimitStatus();
                    handleRateLimit(rateLimitStatus);
                }
            } while (query != null);
            fos.close();
            //fin.close();
        }
    }

    public static void handleRateLimit(RateLimitStatus rateLimitStatus) {
        //throws NPE here sometimes so I guess it is because rateLimitStatus can be null and add this condition
        if (rateLimitStatus != null) {
            int remaining = rateLimitStatus.getRemaining();
            int resetTime = rateLimitStatus.getSecondsUntilReset();
            int sleep = 0;
            if (remaining == 0) {
                sleep = resetTime + 1; //adding 1 more seconds
            } else {
                sleep = (resetTime / remaining) + 1; //adding 1 more seconds
            }

            try {
                Thread.sleep(sleep * 1000 > 0 ? sleep * 1000 : 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}