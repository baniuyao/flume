package org.apache.flume.source.tail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by ybaniu on 12/1/14.
 */
public class TestTail {
    public static void main(String[] args) throws IOException {
        FileReader fr = new FileReader("/tmp/test.log");
        BufferedReader br = new BufferedReader(fr);
        String currentLine = null;
        while (true) {
            if ((currentLine = br.readLine()) != null) {
                System.out.println(currentLine);
            }
        }
    }
}
