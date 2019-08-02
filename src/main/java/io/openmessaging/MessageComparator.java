package io.openmessaging;

import java.util.Comparator;

public class MessageComparator implements Comparator<Message> {
    @Override
    public int compare(Message o1, Message o2) {
        if (o1.getT() < o2.getT()) {
            return -1;
        } else if (o1.getT() > o2.getT()) {
            return 1;
        } else {
            return 0;
        }
    }
}
