package pheme;

import timedelayqueue.PubSubMessage;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// TODO: write a description for this class
// TODO: complete all methods, irrespective of whether there is an explicit TODO or not
// TODO: write clear specs
// TODO: State the rep invariant and abstraction function
// TODO: what is the thread safety argument?
public class PhemeService {

    private PhemeService() {

    }

    public PhemeService getInstance() {
        return new PhemeService();
    }

    public PhemeService getInstance(String configDirName) {
        return new PhemeService();
    }

    public void saveState(String configDirName) {

    }

    public PhemeService addTwitterListener(File credentials) {
        return this;
    }

    public boolean addUser(String userName, String hashPassword) {
        return false;
    }

    public boolean removeUser(String userName, String hashPassword) {
        return false;
    }

    public boolean cancelSubscription(String userName, String hashPassword,
                                      String twitterUserName) {
        return false;
    }

    public boolean cancelSubscription(String userName, String hashPassword, String twitterUserName,
                                      String pattern) {
        return false;
    }

    public boolean addSubscription(String userName, String hashPassword, String twitterUserName) {
        return false;
    }

    public boolean addSubscription(String userName, String hashPassword, String twitterUserName,
                                   String pattern) {
        return false;
    }

    public boolean sendMessage(String userName, String hashPassword, PubSubMessage msg) {
        return false;
    }

    public List<Boolean> isDelivered(UUID msgID, List<UUID> userList) {
        return new ArrayList<>();
    }

    public boolean isDelivered(UUID msgID, UUID user) {
        return false;
    }

    public boolean isUser(String userName) {
        return false;
    }

    public PubSubMessage getNext(String userName, String hashPassword) {
        return PubSubMessage.NO_MSG;
    }

    public List<PubSubMessage> getAllRecent(String userName, String hashPassword) {
        return new ArrayList<PubSubMessage>();
    }
}
