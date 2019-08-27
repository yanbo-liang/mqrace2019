package io.openmessaging;

public class Constants {

    public static int Message_Size = 24;
    public static int Thread_Count = 12;
    static String Message_Path = "/Users/yanbo.liang/test/message";
    static String A_Path = "/Users/yanbo.liang/test/header";
    static String Path = "/Users/yanbo.liang/test/";


//    public static int Message_Size = 50;
//public static int Thread_Count = 12;
//    static int Message_Long_size = 7;
//    static String Message_Path = "/alidata1/race2019/data/message";
//    static String A_Path = "/alidata1/race2019/data/header";
//    static String Path = "/alidata1/race2019/data/";

    public static int Message_Batch_Size = 5000000;

    public static int Message_Buffer_Size = Message_Batch_Size * Message_Size;

    static long Direct_Memory_Size = 2L * 1024 * 1024 * 1024;
    static long Compressed_Buffer_Size = 500 * 1024 * 1024;
    static long Message_Write_Buffer_Size = 50 * Message_Batch_Size;
    static long Header_Write_Buffer_Size = 8 * Message_Batch_Size;

    static long Direct_Read_Buffer_Size = 20L * 1024 * 1024;
}
