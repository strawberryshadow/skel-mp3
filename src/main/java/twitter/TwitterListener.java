package twitter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.github.redouane59.twitter.TwitterClient;
import io.github.redouane59.twitter.dto.endpoints.AdditionalParameters;
import io.github.redouane59.twitter.dto.tweet.TweetList;
import io.github.redouane59.twitter.dto.tweet.TweetV2;
import io.github.redouane59.twitter.dto.user.User;

import java.io.File;
import java.time.LocalDateTime;

// TODO: write a description for this class
// TODO: complete all methods, irrespective of whether there is an explicit TODO or not
// TODO: write clear specs
// TODO: State the rep invariant and abstraction function
// TODO: what is the thread safety argument?
public class TwitterListener {

    // create a new instance of TwitterListener
    // the credentialsFile is a JSON file that
    // contains the API access keys
    // consider placing this file in the
    // 'secret' directory but the constructor
    // should work with any path
    public TwitterListener(File credentialsFile) {

    }

    // add a subscription for all tweets made by a specific
    // Twitter user
    public boolean addSubscription(String twitterUserName) {
        return false;
    }

    // add a subscription for all tweets made by a specific
    // Twitter user that also match a given pattern
    // for simplicity, a match is an exact match of strings but
    // ignoring case
    public boolean addSubscription(String twitterUserName, String pattern) {
        return false;
    }

    // cancel a previous subscription
    // will also cancel subscriptions to specific patterns
    // from the twitter user
    public boolean cancelSubscription(String twitterUserName) {
        return false;
    }

    // cancel a specific user-pattern subscription
    public boolean cancelSubscription(String twitterUserName, String pattern) {
        return false;
    }

    // get all subscribed tweets since the last tweet or
    // set of tweets was obtained
    public TweetList getRecentTweets() {
        return new TweetList();
    }

    // get all the tweets made by a user
    // within a time range
    public TweetList getTweetsByUser(String twitterUserName,
                                     LocalDateTime startTime,
                                     LocalDateTime endTime) {
        return new TweetList();
    }

}
