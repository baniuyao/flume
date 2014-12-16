package org.apache.flume.source.tail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by ybaniu on 12/16/14.
 */
public class TestTailOffset {
    private static final Logger logger =
            LoggerFactory.getLogger(TestTailOffset.class);
    public static void main(String[] args) throws IOException {
        TailOffsetKeeper tailOffsetKeeper = new TailOffsetKeeper("/tmp/offset.log");
        logger.debug(String.valueOf(tailOffsetKeeper.getLatestOffset()));
    }
}
