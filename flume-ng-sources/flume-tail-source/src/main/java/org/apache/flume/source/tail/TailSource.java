package org.apache.flume.source.tail;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created by ybaniu on 12/1/14.
 */
public class TailSource extends AbstractSource implements Configurable, PollableSource {
    private String toTailFileName;
    private Integer batchSize;
    private FileReader fileReader;
    private BufferedReader bufferedReader;

    private static final Logger log = LoggerFactory.getLogger(TailSource.class);
    @Override
    public Status process() throws EventDeliveryException {
        return null;
    }

    @Override
    public void configure(Context context) {
        toTailFileName = context.getString(TailSourceConstants.FILE_NAME);
        batchSize = context.getInteger(TailSourceConstants.BATCH_SIZE, TailSourceConstants.DEFAULT_BATCH_SIZE);
    }

    @Override
    public synchronized void start() {
        try {
            fileReader = new FileReader(toTailFileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        bufferedReader = new BufferedReader(fileReader);
        super.start();
    }
}
