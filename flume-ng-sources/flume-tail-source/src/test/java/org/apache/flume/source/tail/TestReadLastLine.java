package org.apache.flume.source.tail;

import java.io.*;

/**
 * Created by ybaniu on 12/16/14.
 */
public class TestReadLastLine {
    public static void main(String[] args) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("/tmp/test.log")));
        String strLine = null;
        String tmp = null;
        while ((tmp = bufferedReader.readLine()) != null) {
            strLine = tmp;
        }
        System.out.println(strLine);
    }
}
