package org.apache.flume.channel;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Properties;

import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SpoolLog {
  private static Logger LOGGER = LoggerFactory.getLogger(SpoolLog.class);
  private static String CKPT_DATA_FILENAME = "checkpoint.data";
  private static String CURR_LOG_FILENAME_KEY = "currLogFileName";
  private static String CURR_LOG_OFFSET = "currLogOffset";
  private String completedSuffix;
  private String currLogFileName;
  private String fullPath;
  private int currLogOffset; // next offset for reading.

  public SpoolLog(String checkpointDir, String completedSuffix)  {
    this.completedSuffix = completedSuffix;

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

  private int countLines(String logFilename) throws IOException, FileNotFoundException{
    int r = 0;
    LineNumberReader reader = null;
    reader = new LineNumberReader(new FileReader(logFilename));
    while ((reader.readLine()) != null);
    r = reader.getLineNumber();
    reader.close();
    return r;
  }

  public void replay(){
    // Read spoollog file if exists.
    // Do a line count on the data log file.
    // Check the line count against spoollog's offset.
    //
    // If the data log file is marked COMPLETED and it's really not COMPLETED, remove the COMPLETED suffix.
    // Reset SpoolLog state

    String logFilename;
    int offset;
    try {
      Properties prop = new Properties();
      FileInputStream is = new FileInputStream(fullPath);
      prop.load(is);

      logFilename = prop.getProperty(CURR_LOG_FILENAME_KEY);
      String offsetStr = prop.getProperty(CURR_LOG_OFFSET);

      // is the spool log's format correct? skip replay if there's anything missing.
      if (logFilename == null || offsetStr == null) {
        return;
      }
      offset = Integer.parseInt(offsetStr);
    } catch (FileNotFoundException e) {
      LOGGER.warn("Spool checkpoint file is missing: " + fullPath, e);
      return;
    } catch (IOException e) {
      LOGGER.error("Failed to close logReader", e);
      return;
    }

    // count the number of lines in the data file.
    // Might need to read the original filename and the one with the COMPLETE suffix
    int totalLineCnt = 0;
    boolean tryCompleteSuffix = false;
    try {
      totalLineCnt = this.countLines(logFilename);
    } catch (FileNotFoundException e) {
      LOGGER.info("Data file is missing: " + logFilename + ". Try reading the suffix version.");
      tryCompleteSuffix = true;
    } catch (IOException e) {
      LOGGER.error("Failed to wc -l on : " + logFilename);
      return;
    }

    if (tryCompleteSuffix) {
      try {
        logFilename = logFilename + completedSuffix;
        totalLineCnt = this.countLines(logFilename);
      } catch (FileNotFoundException e) {
        LOGGER.error("Data file (COMPLETE) is missing: " + logFilename + ". Try reading the suffix version.", e);
        return;
      } catch (IOException e) {
        LOGGER.error("Failed to wc -l on : " + logFilename);
        return;
      }

    }

    LOGGER.info("offset from checkpoint: " + offset); //xxx
    LOGGER.info("totalLineCnt: " + totalLineCnt); //xxx
  }
}

