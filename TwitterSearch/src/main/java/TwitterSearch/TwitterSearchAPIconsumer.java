package TwitterSearch;

import twitter4j.TwitterException;
import twitter4j.Query;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.QueryResult;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.Calendar;


public class TwitterSearchAPIconsumer {
    public static void main(String[] args) throws Exception{
        String fileName="TwitterText.txt";
        FileOutputStream fos=new FileOutputStream(fileName,true);
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("SV7BRw3ilz5eSULTyJf1LWsuy")
                .setOAuthConsumerSecret("P4L2sZluIGmLEaqIx4P578OVnXGraVYfk55V1ZkDgrFavc2gAf")
                .setOAuthAccessToken("1093319920207708166-gTkrJ4p2inAeZB6KaZ87Jx1eziU6R8")
                .setOAuthAccessTokenSecret("MZ2BIr6apVPWj9D99gy876tcAbb0HOmee0ESkCdXwoCaI");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        String topic = "modi";
        int count = 5000;
        long sinceId = 0;
        Query query = new Query(topic);
        query.setCount(count);
        getTweets(query, twitter, fos);

        do{
            Query querySince = new Query(topic);
            querySince.setCount(count);
            querySince.setSinceId(sinceId);
            getTweets(querySince, twitter, fos);
        }while(checkIfSinceTweetsAreAvaliable(twitter, topic, count, sinceId));
    }

    private static boolean checkIfSinceTweetsAreAvaliable(Twitter twitter, String topic, int count, long sinceId) {
        Query query = new Query(topic);
        query.setCount(count);
        query.setSinceId(sinceId);

        try {
            QueryResult result = twitter.search(query);
            if (result.getTweets() == null || result.getTweets().isEmpty()) {
                return false;
            }
        } catch (TwitterException te) {
            System.out.println("Couldn't connect: " + te);
            System.exit(-1);
        } catch (Exception e) {
            System.out.println("Something went wrong: " + e);
            System.exit(-1);
        }
        return true;
    }

    private static void getTweets(Query query, Twitter twitter, FileOutputStream fos) throws TwitterException{
        boolean getTweets=true;
        long maxId = 0;
        long whileCount=0;
        long sinceId;
        int numberOfTweets = 0;
        while (getTweets){
            try {
                QueryResult result = twitter.search(query);
                if(result.getTweets()==null || result.getTweets().isEmpty()){
                    getTweets=false;
                }else{
                    System.out.println("***********"+result.getTweets().size());
                    int forCount=0;
                    for (Status status: result.getTweets()) {
                        if(whileCount == 0 && forCount == 0){
                            sinceId = status.getId();//Store sinceId in database
                            System.out.println("*************sinceId= "+sinceId);
                        }
                        System.out.println("Id= "+status.getId());
                        System.out.println(status.getCreatedAt());
                        System.out.println("@" + status.getUser().getScreenName() + " : "+status.getUser().getName()+"--------"+status.getText());
                        Date d = status.getCreatedAt();
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(d);
                        int year = cal.get(Calendar.YEAR);
                        int month = cal.get(Calendar.MONTH);
                        int day = cal.get(Calendar.DAY_OF_MONTH);
                        byte[] mybytes = status.getText().getBytes();

                        fos.write(mybytes);
                        if(forCount == result.getTweets().size()-1){
                            maxId = status.getId();
                            System.out.println("******maxId= "+maxId);
                        }
                        System.out.println("");
                        forCount++;
                    }
                    numberOfTweets=numberOfTweets+result.getTweets().size();
                    query.setMaxId(maxId-1);
                }
            }catch (TwitterException te) {
                System.out.println("Couldn't connect: " + te);
                System.exit(-1);
            }catch (Exception e) {
                System.out.println("Something went wrong: " + e);
                System.exit(-1);
            }
            whileCount++;
        }
        System.out.println("Total tweets count======="+numberOfTweets);
    }
}

