package io.openmessaging;

public class Constants {

    static int Message_Size = 24;
    static String Path = "/Users/yanbo.liang/test/message";
    static String Header_Path = "/Users/yanbo.liang/test/message_header";

//    static int Message_Size = 50;
//    static String Path = "/alidata1/race2019/data/message";
//    static String Header_Path = "/alidata1/race2019/data/message_header";

    static int Message_Batch_Size = 5000000;

    static long Direct_Memory_Size = 2L * 1024 * 1024 * 1024;
    static long Direct_Write_Buffer_Size = 256 * 1024 * 1024;

    static long Direct_Read_Buffer_Size = 40L * 1024 * 1024;

}
