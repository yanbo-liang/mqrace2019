//package io.openmessaging;
//
//public class LinearExampleChecker {
//    static int partCount = 0;
//
//    static void check(byte[] messageBuffer) {
//        for (int i = 0; i < Constants.Message_Batch_Size; i++) {
//            long t = LongArrayUtils.getLong(messageBuffer, i * Constants.Message_Size);
//            if (t != i + partCount) {
//                System.out.println("fucked " + i + " " + t);
//                System.exit(1);
//            }
//        }
//        partCount += Constants.Message_Batch_Size;
//    }
//}
