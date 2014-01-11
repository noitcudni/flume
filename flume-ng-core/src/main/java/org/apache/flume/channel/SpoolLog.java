package org.apache.flume.channel;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SpoolLog {
  private static Logger LOGGER = LoggerFactory.getLogger(SpoolLog.class);
  private static String CKPT_DATA_FILENAME = "checkpoint.data";
  private static String CURR_LOG_FILENAME_KEY = "currLogFileName";
  private static String CURR_LOG_OFFSET = "currLogOffset";
  private String completedSuffix;
  private String fullPath; //checkpoint's fullpath
  private String currLogFileName;
  private int currLogOffset; // next offset for reading.
  private int startPlaybackOffset;

  public SpoolLog(String checkpointDir, String completedSuffix)  {
    this.completedSuffix = completedSuffix;
    this.currLogOffset = 0;
    this.startPlaybackOffset = 0;

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
    String completedLogFilename;
    int offset;
    try {
      Properties prop = new Properties();
      FileInputStream is = new FileInputStream(fullPath);
      prop.load(is);

      logFilename = prop.getProperty(CURR_LOG_FILENAME_KEY);
      completedLogFilename = logFilename + completedSuffix;

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
        completedLogFilename = logFilename + completedSuffix;
        totalLineCnt = this.countLines(completedLogFilename);
      } catch (FileNotFoundException e) {
        LOGGER.error("Data file (COMPLETE) is missing: " + completedLogFilename + ". Try reading the suffix version.", e);
        return;
      } catch (IOException e) {
        LOGGER.error("Failed to wc -l on : " + completedLogFilename);
        return;
      }
    }

    LOGGER.info("offset from checkpoint: " + offset);
    LOGGER.info("totalLineCnt: " + totalLineCnt);

    // This particular data log file hasn't been completely consumed yet.
    // Remove the complete suffix if applicable.
    // Reset SpoolLog's internal state: currLogFileName; currLogOffset.
    //
    if (offset != totalLineCnt) {
      if (tryCompleteSuffix) {
        new File(completedLogFilename).renameTo(new File(logFilename));
      }
      this.currLogFileName = logFilename;
      this.currLogOffset = 0;
      this.startPlaybackOffset = offset;
    }
  }


  /*
   * Returns true to indicate that doPut in PersistentPoolChannle should proceed as normal.
   */
  public boolean putCheck(String logFileName) {
    boolean r = true;
    if (this.startPlaybackOffset > 0) { // possibly still doing replay
      if(!this.currLogFileName.equals(logFileName)) {
        this.startPlaybackOffset = 0;
      } else {
        // still replay
        if (this.currLogOffset < this.startPlaybackOffset) {
          r = false;
        }
      }
    }
    return r;
  }

  public void advanceOffsetDuringReplay() {
    this.currLogOffset++;
  }
}

