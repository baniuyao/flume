package org.apache.flume.source.tail;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

import java.io.*;
import java.util.concurrent.TimeUnit;

/** Make workbench of open file handler, write offset and close it.
 *  Result:
 *  org.apache.flume.source.tail.TestWriteFile:
 requests:
 count = 2112754
 mean rate = 61924.45 writes/s
 1-minute rate = 57390.89 writes/s
 5-minute rate = 54840.56 writes/s
 15-minute rate = 54323.34 writes/s
 * Created by ybaniu on 12/1/14.
 */
public class TestWriteFile {
    public static final Meter meter = Metrics.newMeter(TestWriteFile.class, "requests", "writes", TimeUnit.SECONDS);
    public static void main(String[] args) throws IOException, InterruptedException {
        Writer writer = null;
//        ConsoleReporter.enable(1, TimeUnit.SECONDS);
        while (true) {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/tmp/test_write.log", true)));
            writer.write("hello");
            writer.close();
            Thread.sleep(5000L);
//            meter.mark();
        }
//        writer.close();
    }
}
