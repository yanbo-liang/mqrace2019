package io.openmessaging;


import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultMessageStoreImpl extends MessageStore {
    private volatile long[] messageBuffer = new long[Constants.Message_Buffer_Size];
    private volatile int messageBufferStart;

    private AtomicInteger messageCount = new AtomicInteger(0);

    private int batchSize = Constants.Message_Batch_Size;


    private AtomicInteger concurrentPut = new AtomicInteger(0);

    AtomicBoolean init = new AtomicBoolean(false);

    private volatile boolean readyForRead = false;
    static long s = System.currentTimeMillis();

    static long initStart;

    public DefaultMessageStoreImpl() {
        initStart = System.currentTimeMillis();
    }

    //    private void messageToBuffer(int count, Message message) {
//        int startIndex = count * Constants.Message_Size;
//        messageBuffer.putLong(startIndex, message.getT());
//        messageBuffer.putLong(startIndex + 8, message.getA());
//        for (int i = 0; i < messageSize - 16; i++) {
//            messageBuffer.put(startIndex + 16 + i, message.getBody()[i]);
//        }
//    }
    private void toLong(int count, Message message, long[] data) {
        int startIndex = count * Constants.Message_Long_size;
        data[startIndex] = message.getT();
        data[startIndex + 1] = message.getA();
//        LongArrayUtils.byteArrayToLongArray(data, startIndex + 2, message.getBody());

    }


    static FileChannel channel;
    static volatile MappedByteBuffer mappedByteBuffer;
    static volatile long totalbytewritten = 0;

    static {
        try {
            channel = FileChannel.open(Paths.get(Constants.Message_Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE,StandardOpenOption.READ);
            mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, totalbytewritten, Constants.Message_Batch_Size * Constants.Message_Size);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void messageToBuffer(int count, Message message) {
        int startIndex = count * Constants.Message_Size;
        mappedByteBuffer.putLong(startIndex, message.getT());
        mappedByteBuffer.putLong(startIndex + 8, message.getA());
        for (int i = 0; i < Constants.Message_Size - 16; i++) {
            mappedByteBuffer.put(startIndex + 16 + i, message.getBody()[i]);
        }
    }

    @Override
    void put(Message message) {
        try {

            concurrentPut.incrementAndGet();
            int count = messageCount.getAndIncrement();
            if (count < batchSize - 1) {
                messageToBuffer(count, message);
                concurrentPut.decrementAndGet();

            } else if (count == batchSize - 1) {
                messageToBuffer(count, message);
                concurrentPut.decrementAndGet();

                while (concurrentPut.get() != 0) {
                }
                totalbytewritten += mappedByteBuffer.limit();
                mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, totalbytewritten, Constants.Message_Batch_Size * Constants.Message_Size);


                System.out.println(System.currentTimeMillis() - s);
                s = System.currentTimeMillis();
                messageCount.getAndUpdate(x -> 0);
                synchronized (this) {
                    this.notifyAll();
                }
            } else if (count > batchSize - 1) {
                synchronized (this) {
                    concurrentPut.decrementAndGet();
                    this.wait();
                }

                concurrentPut.incrementAndGet();
                count = messageCount.getAndIncrement();
                messageToBuffer(count, message);
                concurrentPut.decrementAndGet();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        System.out.println(System.currentTimeMillis() - initStart);
        System.exit(1);
        try {
            System.out.println("g " + aMin + " " + aMax + " " + tMin + " " + tMax);
            if (!init.get()) {
                if (init.compareAndSet(false, true)) {
                    MessageWriter.write(MessageWriterTask.createEndTask(messageBuffer, messageCount.get()));

                    readyForRead = true;
                    synchronized (this) {
                        this.notifyAll();
                    }
                }
            }
            if (!readyForRead) {
                synchronized (this) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            long start = System.currentTimeMillis();
            ByteBuffer buffer = DirectBufferManager.borrowBuffer();
            MessageReader.read(buffer, tMin, tMax);
            buffer.flip();
            List<Message> messageList = new ArrayList<>();
            while (buffer.hasRemaining()) {
                int dataSize = Constants.Message_Size - 16;
                long t = buffer.getLong();
                long a = buffer.getLong();
                if (tMin <= t && t <= tMax && aMin <= a && a <= aMax) {
                    byte[] b = new byte[dataSize];
                    buffer.get(b, 0, dataSize);
                    messageList.add(new Message(a, t, b));
                } else {
                    buffer.position(buffer.position() + dataSize);
                }
            }
            DirectBufferManager.returnBuffer(buffer);
            System.out.println("gt:\t" + (System.currentTimeMillis() - start));
            return messageList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        try {
            System.out.println("a " + aMin + " " + aMax + " " + tMin + " " + tMax);
            long start = System.currentTimeMillis();
            long sum = 0, count = 0;

            ByteBuffer buffer = DirectBufferManager.borrowBuffer();
            MessageReader.fastRead(buffer, tMin, tMax);
            buffer.flip();
            while (buffer.hasRemaining()) {
                long a = buffer.getLong();
                if (aMin <= a && a <= aMax) {
                    sum += a;
                    count += 1;
                }
            }
            DirectBufferManager.returnBuffer(buffer);
            System.out.println("average:" + (System.currentTimeMillis() - start));
            return count == 0 ? 0 : sum / count;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }


//    @Override
//    void put(Message message) {
//        try {
//            concurrentPut.incrementAndGet();
//            int count = messageCount.getAndIncrement();
//            if (count < batchSize - 1) {
//                messageToBuffer(count, message);
//                concurrentPut.decrementAndGet();
//
//            } else if (count == batchSize - 1) {
//                messageToBuffer(count, message);
//                concurrentPut.decrementAndGet();
//
//                while (concurrentPut.get() != 0) {
//                }
//
////                MessageWriter.write(new MessageWriterTask(messageBuffer,Constants.Message_Batch_Size));
////                messageBuffer=new long[Constants.Message_Buffer_Size];
//
//                System.out.println(System.currentTimeMillis()-s);
//                s=System.currentTimeMillis();
//                messageCount.getAndUpdate(x -> 0);
//                synchronized (this) {
//                    this.notifyAll();
//                }
//            } else if (count > batchSize - 1) {
//                synchronized (this) {
//                    concurrentPut.decrementAndGet();
//                    this.wait();
//                }
//
//                concurrentPut.incrementAndGet();
//                count = messageCount.getAndIncrement();
//                messageToBuffer(count, message);
//                concurrentPut.decrementAndGet();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

//    private void messageToBuffer(int count, Message message) {
//        int startIndex = messageBufferStart + count * Constants.Message_Long_size;
//        messageBuffer[startIndex] = message.getT();
//        messageBuffer[startIndex + 1] = message.getA();
////        messageBuffer[startIndex+2]=ByteBuffer.wrap(message.getBody()).getLong();
////        LongArrayUtils.byteArrayToLongArray(messageBuffer, startIndex + 2, message.getBody());
////
////        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
////        LongArrayUtils.longArraytoByteBuffer(messageBuffer, startIndex + 2, byteBuffer);
////        if (Arrays.equals(message.getBody(),byteBuffer.array())){
////        }else {
////            System.out.println(message.getT());
////            System.out.println(Arrays.toString(message.getBody()));
////            System.out.println(Arrays.toString(byteBuffer.array()));
////            System.out.println(messageBuffer[startIndex+2]);
////
////
////                System.exit(-1);
////        }
//    }

}