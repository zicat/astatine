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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package name.zicat.astatine.connector.doris.table;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import name.zicat.astatine.connector.base.DefaultRetryPolicy;
import name.zicat.astatine.connector.base.RetryPolicy;
import name.zicat.astatine.connector.doris.model.GroupCommitMode;
import name.zicat.astatine.connector.doris.model.RespContent;
import org.apache.flink.util.IOUtils;

/** DorisStreamLoadContext. */
public class DorisStreamLoadContext implements Closeable {

  private static final String DATE_FORMAT = "yyyyMMdd_HHmmss";

  private final DorisHealthChecker dorisHealthChecker;
  private final ExecutorService executorService;
  private final GroupCommitMode groupCommitMode;
  private final DorisStreamLoad streamLoad;
  private final BlockingQueue<Future<RespContent>> tasks;
  private final ByteBuffer byteBuffer;
  private final transient Object lock = new Object();
  private final long timeout;
  private final RetryPolicy retryPolicy;
  private final Map<String, String> headers;
  private final FlushService flushService;

  private volatile Exception loadException;

  public DorisStreamLoadContext(
      DorisHealthChecker dorisHealthChecker,
      int bufferFlushMaxBytes,
      int threads,
      GroupCommitMode groupCommitMode,
      Map<String, String> headers,
      long connectionTimeout,
      long socketTimeout,
      int maxRetry,
      long retryInterval,
      long flushInterval) {
    this.dorisHealthChecker = dorisHealthChecker;
    this.groupCommitMode = groupCommitMode;
    this.headers = headers;
    this.tasks = new ArrayBlockingQueue<>(threads * 2);
    this.timeout = socketTimeout * maxRetry;
    this.byteBuffer = ByteBuffer.allocate(bufferFlushMaxBytes);
    this.executorService = Executors.newFixedThreadPool(threads);
    this.retryPolicy = new DefaultRetryPolicy(maxRetry, retryInterval);
    this.streamLoad = new DorisStreamLoad(connectionTimeout, socketTimeout, threads);
    this.flushService = new FlushService(flushInterval);
  }

  /**
   * invoke data.
   *
   * @param data data
   */
  public void invoke(byte[] data) {
    if (data.length > byteBuffer.capacity()) {
      writeAsync(flipData());
      writeAsync(data, label());
    } else {
      byte[] flipData = null;
      synchronized (lock) {
        if (byteBuffer.remaining() < data.length) {
          flipData = flipData();
        }
        byteBuffer.put(data);
      }
      writeAsync(flipData);
    }
  }

  /** flush. */
  public byte[] flipData() {
    synchronized (lock) {
      byteBuffer.flip();
      if (byteBuffer.remaining() == 0) {
        byteBuffer.clear();
        return null;
      }
      final byte[] data = new byte[byteBuffer.remaining()];
      byteBuffer.get(data);
      byteBuffer.clear();
      return data;
    }
  }

  public void writeAsync(byte[] flipData) {
    if (flipData == null) {
      return;
    }
    writeAsync(flipData, label());
  }

  /**
   * write.
   *
   * @param data data
   * @param label label
   */
  private void writeAsync(byte[] data, String label) {
    final var task = submitStreamLoadTask(data, label);
    while (!tasks.offer(task)) {
      tasksSync();
    }
  }

  /**
   * submit data.
   *
   * @param data data
   * @param label label
   * @return future
   */
  private Future<RespContent> submitStreamLoadTask(byte[] data, String label) {
    final var headers = new HashMap<>(this.headers);
    groupCommitMode.head(label, headers::put);
    return executorService.submit(
        () -> {
          final var ref = new AtomicReference<RespContent>();
          retryPolicy.execute(
              i -> {
                if (i != 0) {
                  dorisHealthChecker.refresh(true);
                }
                final var url = dorisHealthChecker.loadUrlStr();
                ref.set(streamLoad.streamLoad(url, data, headers));
              });
          return ref.get();
        });
  }

  /** task sync. */
  public void tasksSync() {
    Future<RespContent> task;
    while ((task = tasks.poll()) != null) {
      try {
        task.get(timeout, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * create label.
   *
   * @return string
   */
  public static String label() {
    final var sdf = new SimpleDateFormat(DATE_FORMAT);
    final var formatDate = sdf.format(new Date());
    return String.format(
        "flink_connector_%s_%s", formatDate, UUID.randomUUID().toString().replaceAll("-", ""));
  }

  public void flush() {
    writeAsync(flipData());
    tasksSync();
  }

  @Override
  public void close() {
    try {
      if (executorService != null) {
        executorService.shutdown();
      }
    } finally {
      IOUtils.closeQuietly(flushService);
      IOUtils.closeQuietly(streamLoad);
    }
  }

  /**
   * check state.
   *
   * @throws Exception Exception
   */
  public void checkState() throws Exception {
    if (loadException == null) {
      return;
    }
    final var e = loadException;
    loadException = null;
    throw e;
  }

  /** FlushService. */
  private class FlushService implements Closeable {

    private final ScheduledExecutorService flushService;

    public FlushService(long flushInterval) {
      if (flushInterval > 0) {
        flushService = Executors.newSingleThreadScheduledExecutor();
        flushService.scheduleWithFixedDelay(
            () -> {
              try {
                flush();
              } catch (Exception e) {
                loadException = e;
              }
            },
            flushInterval,
            flushInterval,
            TimeUnit.MILLISECONDS);
      } else {
        this.flushService = null;
      }
    }

    @Override
    public void close() {
      if (flushService != null) {
        flushService.shutdown();
      }
    }
  }
}
