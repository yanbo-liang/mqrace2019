package io.openmessaging;

import java.nio.ByteBuffer;

public class ConcurrentPutAndCollect {
    private static ThreadLocal<LocalInfo> local = new ThreadLocal<>();

    private static class LocalInfo {
        ByteBuffer byteBuffer = ByteBuffer.allocate(500000 * Constants.Message_Size);
    }

    static void put(Message message) {
        LocalInfo localInfo = local.get();
        if (localInfo == null) {
            localInfo = new LocalInfo();
            local.set(localInfo);
        }
        ByteBuffer byteBuffer = localInfo.byteBuffer;
        if (!byteBuffer.hasRemaining()){
            byteBuffer.clear();
        }
        byteBuffer.putLong(message.getT());
        byteBuffer.putLong(message.getA());
        byteBuffer.put(message.getBody());

    }
}
