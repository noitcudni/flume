package org.apache.flume.channel;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SpoolLog {
  private static Logger LOGGER = LoggerFactory.getLogger(SpoolLog.class);
  private static String CKPT_DATA_FILENAME = "checkpoint.data";
  private static String CURR_LOG_FILENAME_KEY = "currLogFileName";
  private static String CURR_LOG_OFFSET = "currLogOffset";
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

    Properties prop = new Properties();
    prop.setProperty(CURR_LOG_FILENAME_KEY, currLogFileName);
    prop.setProperty(CURR_LOG_OFFSET, Integer.toString(currLogOffset));

    try {
      prop.store(new FileOutputStream(fullPath, false), null);
    } catch (IOException e) {
      LOGGER.error("Can't commit checkpoint: " + fullPath, e);
    }
  }

  public void replay() {
    // Read spoollog file if exists.
    // Do a line count on the data log file.
    // Check the line count against spoollog's offset.
    //
    // If the data log file is marked COMPLETED and it's really not COMPLETED, remove the COMPLETED suffix.
    // Reset SpoolLog state

    try {
      FileReader logReader = new FileReader(fullPath);
      BufferedReader br = new BufferedReader(logReader);
      String line = br.readLine();

      // empty spool log file. Nothing to replay
      if (line == null) return;


      logReader.read();
      logReader.close();
    } catch (FileNotFoundException e) {
      LOGGER.warn("log file is missing: " + fullPath, e);
    } catch (IOException e) {
      LOGGER.error("Failed to close logReader", e);
    }

  }
}

