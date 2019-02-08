package TwitterSearch;

import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import twitter4j.Query;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.QueryResult;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSearchAPIconsumer {
    public static void main(String[] args) throws Exception{
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("SV7BRw3ilz5eSULTyJf1LWsuy")
                .setOAuthConsumerSecret("P4L2sZluIGmLEaqIx4P578OVnXGraVYfk55V1ZkDgrFavc2gAf")
                .setOAuthAccessToken("1093319920207708166-gTkrJ4p2inAeZB6KaZ87Jx1eziU6R8")
                .setOAuthAccessTokenSecret("MZ2BIr6apVPWj9D99gy876tcAbb0HOmee0ESkCdXwoCaI");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        String topic = "modi";
        Query query = new Query(topic);
        Date date = new Date();
        String sinceDate= new SimpleDateFormat("yyyymmdd").format(date);
        System.out.println(sinceDate);

        SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-mm-dd");
        String untilDate = "2019-02-07";
        try {
            Date newdate = dateformat.parse(untilDate);
            System.out.println(new SimpleDateFormat("yyyymmdd").format(newdate));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        query.setSince(untilDate);

        //query.setUntil(untilDate);
        QueryResult result = twitter.search(query);
        List<Status> tweets = result.getTweets();
            for (Status tweet : tweets) {
                System.out.println("@" + tweet.getUser().getScreenName() + ":" + tweet.getText());
            }
//        do{
//            List<Status> tweets = result.getTweets();
//            for (Status tweet : tweets) {
//                System.out.println("@" + tweet.getUser().getScreenName() + ":" + tweet.getText());
//            }
//            query = result.nextQuery();
//            if(query!=null)
//                result = twitter.search(query);
//        }while(query!=null);
    }
}
