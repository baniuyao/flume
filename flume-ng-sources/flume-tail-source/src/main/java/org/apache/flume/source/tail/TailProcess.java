package org.apache.flume.source.tail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by ybaniu on 12/16/14.
 */
public class TailProcess {
    private static final Logger LOG = LoggerFactory.getLogger(TailProcess.class);
    private File file;
    private FileReader fileReader;
    private BufferedReader bufferedReader;
    private TailOffsetKeeper tailOffsetKeeper = null;
    private Integer currentLineLength;

    public TailProcess(String fileName) throws IOException {
        this.file = new File(fileName);
        this.fileReader = new FileReader(file);
        this.bufferedReader = new BufferedReader(fileReader);
        tailOffsetKeeper = new TailOffsetKeeper(fileName + ".offset");
        bufferedReader.skip(tailOffsetKeeper.getLatestOffset());
    }

    public void commit() throws IOException {
        tailOffsetKeeper.updateOffset(currentLineLength);
    }
    public String tailOneLine() throws IOException {
        checkFileRotate();
        String currentLine = bufferedReader.readLine();
        if (currentLine != null) {
//            tailOffsetKeeper.updateOffset(currentLine.length());
            currentLineLength = currentLine.length();
        }
        return currentLine;
    }


    private void checkFileRotate() {
        if (!file.exists()) {
            LOG.info("file does not exist");
            LOG.debug("begin loop");
            while (true) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    // TODO: how to handle this exception?
                    e.printStackTrace();
                }
                LOG.debug("wait");
                if (file.exists()) {
                    LOG.debug("file appear");
                    try {
                        LOG.debug("refresh fileReader");
                        fileReader = new FileReader(file);
                        tailOffsetKeeper.rotate();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    LOG.debug("refresh bufferedReader");
                    bufferedReader = new BufferedReader(fileReader);
                    break;
                }
            }
        }
    }
}
