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
  private String currDataFilename;
  private int currDataOffset; //inclusive. Start counting from 1.
  private int startPlaybackOffset;

  public SpoolLog(String checkpointDir, String completedSuffix)  {
    this.completedSuffix = completedSuffix;
    this.currDataOffset = 0;
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

  public void commit(String dataFilename, int dataOffset) {
    if (currDataFilename == null || !currDataFilename.equals(dataFilename)) {
      LOGGER.info("reseting logOffset");
      currDataOffset = dataOffset;
    } else {
      currDataOffset += dataOffset;
    }
    currDataFilename = dataFilename;

    LOGGER.info("currLogFileName: " + currDataFilename);
    LOGGER.info("\t currLogOffset: " + currDataOffset);

    Properties prop = new Properties();
    prop.setProperty(CURR_LOG_FILENAME_KEY, currDataFilename);
    prop.setProperty(CURR_LOG_OFFSET, Integer.toString(currDataOffset));

    try {
      prop.store(new FileOutputStream(fullPath, false), null);
    } catch (IOException e) {
      LOGGER.error("Can't commit checkpoint: " + fullPath, e);
    }
  }

  private int countLines(String dataFilename) throws IOException, FileNotFoundException{
    int r = 0;
    LineNumberReader reader = null;
    reader = new LineNumberReader(new FileReader(dataFilename));
    while ((reader.readLine()) != null);
    r = reader.getLineNumber();
    reader.close();
    return r;
  }

  public void restoreFromCheckPoint() {
    // Read spoollog file if exists.
    // Do a line count on the data log file.
    // Check the line count against spoollog's offset.
    //
    // If the data log file is marked COMPLETED and it's really not COMPLETED, remove the COMPLETED suffix.
    // Reset SpoolLog state

    String dataFilename;
    String completedDatafilename;
    int offset;
    try {
      Properties prop = new Properties();
      FileInputStream is = new FileInputStream(fullPath);
      prop.load(is);

      dataFilename = prop.getProperty(CURR_LOG_FILENAME_KEY);
      completedDatafilename = dataFilename + completedSuffix;

      String offsetStr = prop.getProperty(CURR_LOG_OFFSET);

      // is the spool log's format correct? skip replay if there's anything missing.
      if (dataFilename == null || offsetStr == null) {
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
      totalLineCnt = this.countLines(dataFilename);
    } catch (FileNotFoundException e) {
      LOGGER.info("Data file is missing: " + dataFilename + ". Try reading the suffix version.");
      tryCompleteSuffix = true;
    } catch (IOException e) {
      LOGGER.error("Failed to wc -l on : " + dataFilename);
      return;
    }

    if (tryCompleteSuffix) {
      try {
        completedDatafilename = dataFilename + completedSuffix;
        totalLineCnt = this.countLines(completedDatafilename);
      } catch (FileNotFoundException e) {
        LOGGER.error("Data file (COMPLETE) is missing: " + completedDatafilename + ". Try reading the suffix version.", e);
        return;
      } catch (IOException e) {
        LOGGER.error("Failed to wc -l on : " + completedDatafilename);
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
        new File(completedDatafilename).renameTo(new File(dataFilename));
      }
      this.currDataFilename = dataFilename;
      this.currDataOffset = 0;
      this.startPlaybackOffset = offset;
    }
  }


  /*
   * Returns true to indicate that doPut in PersistentPoolChannle should proceed as normal.
   */
  public boolean putCheck(String dataFilename) {
    boolean r = true;
    if (this.startPlaybackOffset > 0) { // possibly still doing replay
      if(!this.currDataFilename.equals(dataFilename)) {
        this.startPlaybackOffset = 0;
      } else {
        // still replay
        if (this.currDataOffset < this.startPlaybackOffset) {
          r = false;
        }
      }
    }
    return r;
  }

  public void replay() {
    this.currDataOffset++;
  }
}

