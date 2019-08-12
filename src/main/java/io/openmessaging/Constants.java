package io.openmessaging;

public class Constants {
//        static int Message_Size = 24;
    static int Message_Size = 50;

    static int Message_Batch_Size = 5000000;
    static int Index_Skip_Size = Message_Size * 1000;

//    static String Messages = "/Users/yanbo.liang/test/messages";
//    static String Messages_Without_Data = "/Users/yanbo.liang/test/messagesWithoutData";

    static String Messages = "/alidata1/race2019/data/messages";
    static String Messages_Without_Data = "/alidata1/race2019/data/messagesWithoutData";

    static long Direct_Memory_Size = 2L * 1024 * 1024 * 1024;
    static long Direct_Write_Buffer_Size = 256 * 1024 * 1024;
    static long Direct_Read_Buffer_Size = 40L * 1024 * 1024;


}
