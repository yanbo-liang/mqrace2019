package io.openmessaging;

public class Constants {
//    static int Message_Size = 24;
    static int Message_Size = 50;

    static int Message_Batch_Size = 5000000;
    static int Index_Skip_Size = Message_Size * 1000;

//    static String Path = "/Users/yanbo.liang/test/hive";
        static String Path = "/alidata1/race2019/data/hive";

    static long Direct_Memory_Size = 2L * 1024 * 1024 * 1024;
    static long Direct_Write_Buffer_Size = 256 * 1024 * 1024;
    static long Direct_Read_Buffer_Size = 40L * 1024 * 1024;


}
