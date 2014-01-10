package org.apache.flume.channel;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SpoolLog {
  private static Logger LOGGER = LoggerFactory.getLogger(SpoolLog.class);
  private static String CKPT_DATA_FILENAME= "checkpoint.data";
  private String currLogFileName;
  private String fullPath;
  private int currLogOffset; // next offset for reading.

  public SpoolLog(String checkpointDir)  {
    // create the checkpoint dir if necessary.
    File dir = new File(checkpointDir);
    if (!dir.exists()) {
      try{
        dir.mkdirs();
      } catch (SecurityException e) {
        LOGGER.error("Can't create spool checkpoint dir: " + checkpointDir, e);
      }
    }

    // create the checkpoint data file if necessary.
    fullPath = checkpointDir + "/" + SpoolLog.CKPT_DATA_FILENAME;
    File f = new File(fullPath);
    if (!f.exists()) {
      try {
        f.createNewFile();
      } catch (IOException e) {
        LOGGER.error("Can't create spool checkpoint file: " + fullPath, e);
      }
    }
  }

  public void commit(String logFileName, int logOffset) {
    if (currLogFileName == null || !currLogFileName.equals(logFileName)) {
      LOGGER.info("reseting logOffset");
      currLogOffset = logOffset;
    } else {
      currLogOffset += logOffset;
    }
    currLogFileName = logFileName;

    LOGGER.info("currLogFileName: " + currLogFileName);
    LOGGER.info("\t currLogOffset: " + currLogOffset);

    StringBuffer sb = new StringBuffer();
    sb.append("logFilename:");
    sb.append(currLogFileName);
    sb.append("|offset:");
    sb.append(currLogOffset);

    try {
      //FileWriter fileWriter = new FileWriter(fullPath, false [> don't append <]);
      LOGGER.info("fullPath: " + fullPath);//xxx
      FileWriter logWriter = new FileWriter(fullPath, false /* don't append */);
      //this.logWriter = new PrintWriter(fileWriter, true [> autoFlush <]);
      LOGGER.info("writing to the log file"); //xxx
      logWriter.write(sb.toString());
      LOGGER.info("about to close"); //xxx
      logWriter.close();
      LOGGER.info("close"); //xxx
    } catch (IOException e) {
      LOGGER.error("Can't commit checkpoint: " + fullPath, e);
    }
  }

  public void replay() {

  }
}

