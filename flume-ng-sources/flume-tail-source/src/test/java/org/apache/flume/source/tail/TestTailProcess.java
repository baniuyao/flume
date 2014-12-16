package org.apache.flume.source.tail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ybaniu on 12/16/14.
 */
public class TestTailProcess {
    private static final Logger log =
            LoggerFactory.getLogger(TestTailProcess.class);
    public static void main(String[] args) throws IOException {
        TailProcess tailProcess = new TailProcess("/tmp/test.log");
        String line = null;
        while (true) {
            line = tailProcess.tailOneLine();
            if (line != null) {
                System.out.println(line);
                tailProcess.commit();
            }
        }
    }
}
