package io.openmessaging;

public class Constants {

//    public static int Message_Size = 24;
//    public static String Body_Path = "/Users/yanbo.liang/test/message";
//    public static String A_Path = "/Users/yanbo.liang/test/header";

    public static int Message_Size = 50;
    public static String Body_Path = "/alidata1/race2019/data/message";
    public static String A_Path = "/alidata1/race2019/data/header";

    public static int Thread_Count = 12;
    public static int Body_Size = Message_Size - 16;
    public static int Batch_Size = 5000000;

    public static long Direct_Memory_Size = 2L * 1024 * 1024 * 1024;
    public static long Compressed_Buffer_Size = 500 * 1024 * 1024;
    public static long Write_Body_Buffer_Size = Body_Size * Batch_Size;
    public static long Write_A_Buffer_Size = 8 * Batch_Size;
}
