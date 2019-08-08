package io.openmessaging;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class LogAnalyzer {
    public static void main(String[] args) {
        try {
            Path path = Paths.get("/Users/yanbo.liang/tester_log.log");
            List<String> collect = Files.readAllLines(path).stream().filter(x -> x.startsWith("/alidata1"))
                    .map(x -> x.substring(24))
                    .collect(Collectors.toList());
            int[][] indexes = new int[collect.size()][2];
            for (int i = 0; i < indexes.length; i++) {
                int[] range = indexes[i];
                String s = collect.get(i);
                String[] split = s.split("-");
                range[0] = Integer.valueOf(split[0]);
                range[1] = Integer.valueOf(split[1]);
            }
            for (int i = 1; i < indexes.length; i++) {
                if (indexes[i-1][1]<indexes[i][0]){

                }else{
                    System.out.println(i);
                }
            }
            for (int i = 1; i < indexes.length; i++) {
                if (indexes[i-1][0]<indexes[i][0]){

                }else{
                    System.out.println(i);
                }
            }
            for (int i = 1; i < indexes.length; i++) {
                if (indexes[i-1][1]<indexes[i][1]){

                }else{
                    System.out.println(i);
                }
            }
            for (int i = 0; i < indexes.length; i++) {
                if (indexes[i][0]<indexes[i][1]){

                }else{
                    System.out.println(i);
                }
            }
//            Files.write(Path, collect, StandardOpenOption.
//                    CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
