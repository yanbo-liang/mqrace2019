package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class MessageReader {
    private AsynchronousFileChannel fileChannel;

    public MessageReader() {
        try {
            Path path = Paths.get(Constants.Path);
            fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class Callback implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer attachment) {
            synchronized (attachment) {

                System.out.println("byte read " + result);
                attachment.notify();
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer attachment) {
            exc.printStackTrace();
        }
    }

    public ByteBuffer read(long tMin, long tMax) {
        long s = System.currentTimeMillis();
        long start = MessageIndex.readStartInclusive(tMin);
        long end = MessageIndex.readEndExclusive(tMax);
        System.out.println("index:" + (System.currentTimeMillis() - s));

        ByteBuffer buffer = DirectBufferManager.borrowBuffer();
        buffer.limit((int) (end - start));
        long r = System.currentTimeMillis();

        synchronized (buffer) {
            fileChannel.read(buffer, start, buffer, new Callback());
            try {
                buffer.wait();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("read:" + (System.currentTimeMillis() - r));

        return buffer;
    }
}

//    static SortedMap<Long, Path> lowerMap = new TreeMap<>();
//    static SortedMap<Long, Path> upperMap = new TreeMap<>();
//
//    public static List<ByteBuffer> read(long tMin, long tMax) {
//        int lower = -1;
//        int upper = -1;
//        ArrayList<Map.Entry<Long, Path>> lowerList = new ArrayList<>(lowerMap.entrySet());
//        ArrayList<Map.Entry<Long, Path>> upperList = new ArrayList<>(upperMap.entrySet());
//        if (lowerList.get(0).getKey() > tMin) {
//            lower = 0;
//        } else if (upperList.get(upperList.size() - 1).getKey() < tMin){
//            return new ArrayList<>();
//
//        }else {
//            for (int i = 0; i < lowerList.size(); i++) {
//                if (lowerList.get(i).getKey() <= tMin) {
//                    lower = i;
//                }
//            }
//        }
//
//
//        if (upperList.get(upperList.size() - 1).getKey() < tMax) {
//            upper = upperList.size() - 1;
//        } else if (lowerList.get(0).getKey() > tMax) {
//            return new ArrayList<>();
//        } else {
//            for (int i = upperList.size() - 1; i >= 0; i--) {
//                if (upperList.get(i).getKey() >= tMax) {
//                    upper = i;
//                }
//            }
//        }
//        List<AsynchronousFileChannel> channels = new ArrayList<>();
//        List<ByteBuffer> buffers = new ArrayList<>();
//        List<Future<Integer>> futures = new ArrayList<>();
//        if (lower <= upper) {
//            System.out.println(tMin + " " + tMax + " " + lower + " " + upper);
//            for (int i = lower; i <= upper; i++) {
//                Path path = lowerList.get(i).getValue();
//
//
//
//                try {
//                    AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
//                    ByteBuffer buffer = ByteBuffer.allocateDirect((int)fileChannel.size());
//                    buffers.add(buffer);
//                    Future<Integer> readFuture = fileChannel.read(buffer, 0);
//                    futures.add(readFuture);
//                    channels.add(fileChannel);
//
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//
//            }
//        }
//        while (true) {
//            if (check(futures)) {
//                break;
//
//            }
//        }
//        for (AsynchronousFileChannel channel : channels) {
//            try {
//                channel.close();
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        return buffers;
//    }
//
//    public static <T> boolean check(List<Future<T>> futures) {
//        for (Future future : futures) {
//            if (!future.isDone()) {
//                return false;
//            }
//        }
//        return true;
//    }
