/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.annotations.Recyclable;
import org.apache.flume.instrumentation.ChannelCounter;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * PersistentSpoolChannel is the recommended channel to use when speeds which
 * writing to disk is impractical is required or durability of data is not
 * required.
 * </p>
 * <p>
 * Additionally, PersistentSpoolChannel should be used when a channel is required for
 * unit testing purposes.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Recyclable
public class PersistentSpoolChannel extends BasicChannelSemantics {
  private static final String CHECKPOINT_DIR = "checkpointDir";

  private static Logger LOGGER = LoggerFactory.getLogger(PersistentSpoolChannel.class);
  private static final Integer defaultCapacity = 100;
  private static final Integer defaultTransCapacity = 100;
  private static final double byteCapacitySlotSize = 100;
  private static final Long defaultByteCapacity = (long)(Runtime.getRuntime().maxMemory() * .80);
  private static final Integer defaultByteCapacityBufferPercentage = 20;

  private static final Integer defaultKeepAlive = 3;
  private SpoolLog log;
  private String checkpointDir;

  private class PersistentSpoolTransaction extends BasicTransactionSemantics {
    private LinkedBlockingDeque<Event> takeList;
    private LinkedBlockingDeque<Event> putList;
    private final ChannelCounter channelCounter;
    private int putByteCounter = 0;
    private int takeByteCounter = 0;

    public PersistentSpoolTransaction(int transCapacity, ChannelCounter counter) {
      putList = new LinkedBlockingDeque<Event>(transCapacity);
      takeList = new LinkedBlockingDeque<Event>(transCapacity);

      channelCounter = counter;
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
      if( !log.putCheck(event.getHeaders().get(fileHeaderKey)) ) {
        // still replaying.
        log.advanceOffsetDuringReplay();
        return;
      }

      channelCounter.incrementEventPutAttemptCount();
      int eventByteSize = (int)Math.ceil(estimateEventSize(event)/byteCapacitySlotSize);

      if (bytesRemaining.tryAcquire(eventByteSize, keepAlive, TimeUnit.SECONDS)) {
        if(!putList.offer(event)) {
          throw new ChannelException("Put queue for PersistentSpoolTransaction of capacity " +
              putList.size() + " full, consider committing more frequently, " +
              "increasing capacity or increasing thread count");
        }
      } else {
        throw new ChannelException("Put queue for PersistentSpoolTransaction of byteCapacity " +
            (lastByteCapacity * (int)byteCapacitySlotSize) + " bytes cannot add an " +
            " event of size " + estimateEventSize(event) + " bytes because " +
             (bytesRemaining.availablePermits() * (int)byteCapacitySlotSize) + " bytes are already used." +
            " Try consider comitting more frequently, increasing byteCapacity or increasing thread count");
      }
      putByteCounter += eventByteSize;
    }

    @Override
    protected Event doTake() throws InterruptedException {
      channelCounter.incrementEventTakeAttemptCount();
      if(takeList.remainingCapacity() == 0) {
        throw new ChannelException("Take list for PersistentSpoolTransaction, capacity " +
            takeList.size() + " full, consider committing more frequently, " +
            "increasing capacity, or increasing thread count");
      }
      if(!queueStored.tryAcquire(keepAlive, TimeUnit.SECONDS)) {
        return null;
      }
      Event event;
      synchronized(queueLock) {
        event = queue.poll();
      }
      Preconditions.checkNotNull(event, "Queue.poll returned NULL despite semaphore " +
          "signalling existence of entry");
      takeList.put(event);

      int eventByteSize = (int)Math.ceil(estimateEventSize(event)/byteCapacitySlotSize);
      takeByteCounter += eventByteSize;

      return event;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      int remainingChange = takeList.size() - putList.size();
      if(remainingChange < 0) {
        if(!queueRemaining.tryAcquire(-remainingChange, keepAlive, TimeUnit.SECONDS)) {
          throw new ChannelException("Space for commit to queue couldn't be acquired" +
              " Sinks are likely not keeping up with sources, or the buffer size is too tight");
        }
      }
      int puts = putList.size();
      int takes = takeList.size();
      synchronized(queueLock) {
        if(puts > 0 ) {
          while(!putList.isEmpty()) {
            if(!queue.offer(putList.removeFirst())) {
              throw new RuntimeException("Queue add failed, this shouldn't be able to happen");
            }
          }
        }
        if (takes > 0) {
          // just take a peek to see what file we are dealing with
          Event e = takeList.getFirst();
          LOGGER.debug("filename: " +
              e.getHeaders().get(fileHeaderKey));
          LOGGER.debug("\tsize: " + takes);
          String logFilename =
            e.getHeaders().get(fileHeaderKey);
          log.commit(logFilename, takes);
        }

        putList.clear();
        takeList.clear();
      }
      bytesRemaining.release(takeByteCounter);
      takeByteCounter = 0;
      putByteCounter = 0;

      queueStored.release(puts);
      if(remainingChange > 0) {
        queueRemaining.release(remainingChange);
      }
      if (puts > 0) {
        channelCounter.addToEventPutSuccessCount(puts);
      }
      if (takes > 0) {
        channelCounter.addToEventTakeSuccessCount(takes);
      }

      channelCounter.setChannelSize(queue.size());
    }

    @Override
    protected void doRollback() {
      int takes = takeList.size();
      synchronized(queueLock) {
        Preconditions.checkState(queue.remainingCapacity() >= takeList.size(), "Not enough space in PersistentSpool channel " +
            "queue to rollback takes. This should never happen, please report");
        while(!takeList.isEmpty()) {
          queue.addFirst(takeList.removeLast());
        }
        putList.clear();
      }
      bytesRemaining.release(putByteCounter);
      putByteCounter = 0;
      takeByteCounter = 0;

      queueStored.release(takes);
      channelCounter.setChannelSize(queue.size());
    }

  }

  // lock to guard queue, mainly needed to keep it locked down during resizes
  // it should never be held through a blocking operation
  private Object queueLock = new Object();

  @GuardedBy(value = "queueLock")
  private LinkedBlockingDeque<Event> queue;

  // invariant that tracks the amount of space remaining in the queue(with all uncommitted takeLists deducted)
  // we maintain the remaining permits = queue.remaining - takeList.size()
  // this allows local threads waiting for space in the queue to commit without denying access to the
  // shared lock to threads that would make more space on the queue
  private Semaphore queueRemaining;
  // used to make "reservations" to grab data from the queue.
  // by using this we can block for a while to get data without locking all other threads out
  // like we would if we tried to use a blocking call on queue
  private Semaphore queueStored;
  // maximum items in a transaction queue
  private volatile Integer transCapacity;
  private volatile int keepAlive;
  private volatile int byteCapacity;
  private volatile int lastByteCapacity;
  private volatile int byteCapacityBufferPercentage;
  private Semaphore bytesRemaining;
  private ChannelCounter channelCounter;
  private String completedSuffix;
  private String fileHeaderKey;

  public PersistentSpoolChannel() {
    super();
  }

  /**
   * Read parameters from context
   * <li>capacity = type long that defines the total number of events allowed at one time in the queue.
   * <li>transactionCapacity = type long that defines the total number of events allowed in one transaction.
   * <li>byteCapacity = type long that defines the max number of bytes used for events in the queue.
   * <li>byteCapacityBufferPercentage = type int that defines the percent of buffer between byteCapacity and the estimated event size.
   * <li>keep-alive = type int that defines the number of second to wait for a queue permit
   */
  @Override
  public void configure(Context context) {
    completedSuffix = context.getString(
        SpoolDirectorySourceConfigurationConstants.SPOOLED_FILE_SUFFIX,
        SpoolDirectorySourceConfigurationConstants.DEFAULT_SPOOLED_FILE_SUFFIX);

    fileHeaderKey = context.getString(
        SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER_KEY,
        SpoolDirectorySourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY);

    String homePath = System.getProperty("user.home").replace('\\', '/');

    Integer capacity = null;
    try {
      capacity = context.getInteger("capacity", defaultCapacity);
    } catch(NumberFormatException e) {
      capacity = defaultCapacity;
      LOGGER.warn("Invalid capacity specified, initializing channel to "
          + "default capacity of {}", defaultCapacity);
    }

    if (capacity <= 0) {
      capacity = defaultCapacity;
      LOGGER.warn("Invalid capacity specified, initializing channel to "
          + "default capacity of {}", defaultCapacity);
    }
    try {
      transCapacity = context.getInteger("transactionCapacity", defaultTransCapacity);
    } catch(NumberFormatException e) {
      transCapacity = defaultTransCapacity;
      LOGGER.warn("Invalid transation capacity specified, initializing channel"
          + " to default capacity of {}", defaultTransCapacity);
    }

    if (transCapacity <= 0) {
      transCapacity = defaultTransCapacity;
      LOGGER.warn("Invalid transation capacity specified, initializing channel"
          + " to default capacity of {}", defaultTransCapacity);
    }
    Preconditions.checkState(transCapacity <= capacity,
        "Transaction Capacity of PersistentSpool Channel cannot be higher than " +
            "the capacity.");

    try {
      byteCapacityBufferPercentage = context.getInteger("byteCapacityBufferPercentage", defaultByteCapacityBufferPercentage);
    } catch(NumberFormatException e) {
      byteCapacityBufferPercentage = defaultByteCapacityBufferPercentage;
    }

    try {
      byteCapacity = (int)((context.getLong("byteCapacity", defaultByteCapacity).longValue() * (1 - byteCapacityBufferPercentage * .01 )) /byteCapacitySlotSize);
      if (byteCapacity < 1) {
        byteCapacity = Integer.MAX_VALUE;
      }
    } catch(NumberFormatException e) {
      byteCapacity = (int)((defaultByteCapacity * (1 - byteCapacityBufferPercentage * .01 )) /byteCapacitySlotSize);
    }

    try {
      keepAlive = context.getInteger("keep-alive", defaultKeepAlive);
    } catch(NumberFormatException e) {
      keepAlive = defaultKeepAlive;
    }

    if(queue != null) {
      try {
        resizeQueue(capacity);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      synchronized(queueLock) {
        queue = new LinkedBlockingDeque<Event>(capacity);
        queueRemaining = new Semaphore(capacity);
        queueStored = new Semaphore(0);
      }
    }

    if (bytesRemaining == null) {
      bytesRemaining = new Semaphore(byteCapacity);
      lastByteCapacity = byteCapacity;
    } else {
      if (byteCapacity > lastByteCapacity) {
        bytesRemaining.release(byteCapacity - lastByteCapacity);
        lastByteCapacity = byteCapacity;
      } else {
        try {
          if(!bytesRemaining.tryAcquire(lastByteCapacity - byteCapacity, keepAlive, TimeUnit.SECONDS)) {
            LOGGER.warn("Couldn't acquire permits to downsize the byte capacity, resizing has been aborted");
          } else {
            lastByteCapacity = byteCapacity;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    if (channelCounter == null) {
      channelCounter = new ChannelCounter(getName());
    }

    checkpointDir = context.getString(CHECKPOINT_DIR,
        homePath + "/.flume/persistent-spool-channel/checkpoint");
  }

  private void resizeQueue(int capacity) throws InterruptedException {
    int oldCapacity;
    synchronized(queueLock) {
      oldCapacity = queue.size() + queue.remainingCapacity();
    }

    if(oldCapacity == capacity) {
      return;
    } else if (oldCapacity > capacity) {
      if(!queueRemaining.tryAcquire(oldCapacity - capacity, keepAlive, TimeUnit.SECONDS)) {
        LOGGER.warn("Couldn't acquire permits to downsize the queue, resizing has been aborted");
      } else {
        synchronized(queueLock) {
          LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
          newQueue.addAll(queue);
          queue = newQueue;
        }
      }
    } else {
      synchronized(queueLock) {
        LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
        newQueue.addAll(queue);
        queue = newQueue;
      }
      queueRemaining.release(capacity - oldCapacity);
    }
  }

  @Override
  public synchronized void start() {
    channelCounter.start();
    channelCounter.setChannelSize(queue.size());
    channelCounter.setChannelCapacity(Long.valueOf(
            queue.size() + queue.remainingCapacity()));

    log = new SpoolLog(checkpointDir, completedSuffix);
    log.replay();
    super.start();
  }

  @Override
  public synchronized void stop() {
    channelCounter.setChannelSize(queue.size());
    channelCounter.stop();
    super.stop();
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new PersistentSpoolTransaction(transCapacity, channelCounter);
  }

  private long estimateEventSize(Event event)
  {
    byte[] body = event.getBody();
    if(body != null && body.length != 0) {
      return body.length;
    }
    //Each event occupies at least 1 slot, so return 1.
    return 1;
  }
}
