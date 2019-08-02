package io.openmessaging;


import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {

        List<ByteBuffer> a = new ArrayList<>();
        while(true) {
            try {
                Path path = Paths.get("/Users/yanbo.liang/test/666669-800002");

                AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
                ByteBuffer buffer = ByteBuffer.allocateDirect(100 * 1024 * 1024);
                fileChannel.read(buffer, 0, null, new CompletionHandler<Integer, ByteBuffer>() {

                    @Override
                    public void completed(Integer result, ByteBuffer attachment) {
                        System.out.println("bytes written: " + result);
                        a.add(buffer);
                        ((DirectBuffer)buffer).cleaner().clean();
                    }

                    @Override
                    public void failed(Throwable e, ByteBuffer attachment) {
                        System.out.println("Write failed");
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


//        long start = System.currentTimeMillis();
//        List<Message> list = new LinkedList<>();
//        for (int i = 0; i < 10000000; i++) {
//            byte[] data = new byte[34];
//            list.add(new Message(1, 1, data));
//        }
//        System.out.println(System.currentTimeMillis() - start);
//        start = System.currentTimeMillis();
//        Collections.sort(list, new Comparator<Message>() {
//            @Override
//            public int compare(Message o1, Message o2) {
//                return 0;
//            }
//        });
//        System.out.println(System.currentTimeMillis() - start);
//
//        try {
//            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
//            theUnsafe.setAccessible(true);
//            Unsafe unsafe = (Unsafe) theUnsafe.get(null);
//            unsafe.allocateMemory(1);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }
}
