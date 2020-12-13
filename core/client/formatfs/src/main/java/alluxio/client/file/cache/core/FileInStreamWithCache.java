/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.core;

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.Metric.ClientCacheStatistics;
import alluxio.client.file.cache.Metric.HitRatioMetric;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.PreconditionMessage;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static alluxio.client.file.cache.core.ClientCacheContext.*;

public class FileInStreamWithCache extends FileInStream {
  protected final ClientCacheContext mCacheContext;
  public CacheManager mCachePolicy;
  long mPosition;
  public final long mLength;
  final long mFileId;
  LockManager mLockManager;

  public FileInStreamWithCache(InStreamOptions opt, ClientCacheContext context, URIStatus status) {
    super(status, opt, FileSystemContext.get());
    mCacheContext = context;
    mCachePolicy = mCacheContext.getCacheManager();
    mPosition = 0;
    mLength = status.getLength();
    mFileId = status.getFileId();
    fileId = mFileId;
    mLockManager = mCacheContext.getLockManager();
  }

  public long getPos() {
    return mPosition;
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    return read0(pos, b, off, len);
  }

  public int innerRead(byte[] b, int off, int len) throws IOException {
    long st = System.currentTimeMillis();
    int res = super.read(b, off, len);
    if (res > 0) mPosition += res;
    ClientCacheStatistics.INSTANCE.readUFSTime += (System.currentTimeMillis() - st);
    return res;
  }

  public int innerPositionRead(long pos, byte[] b, int off, int len) throws IOException {
    long st = System.currentTimeMillis();
    long  b1 = System.currentTimeMillis();
    int res = super.positionedRead(pos, b, off, len);
    missSize += res;
    missTime ++;
    testRead.actualRead += System.currentTimeMillis() - b1;
    ClientCacheStatistics.INSTANCE.readUFSTime += (System.currentTimeMillis() - st);
    return res;
  }

  protected int read0(long pos, byte[] b, int off, int len) throws IOException {
    boolean isPosition = false;
    if (pos != mPosition) {
      isPosition = true;
    }
    long length = mLength;
    if (pos < 0 || pos >= length) {
      return -1;
    }
    int res;

    if (len == 0) {
      return 0;
    } else if (pos == mLength) { // at end of file
      return -1;
    }
    if (mLockManager.evictCheck()) {
      try {
        if (mCachePolicy.isFixedLength()) {
          // take a note of real pos and len, to calculate hit size properly
          FixLengthReadNote.takeNote(pos, len);
        }
        LockTask task = ClientCacheContext.INSTANCE.getLockTask(mFileId);
        ClientCacheContext.readTask = task;

        CacheUnit unit;
        long beginForFixLength = pos;
        long endForFixLength = pos + len;
        long st = System.currentTimeMillis();
        if (mCachePolicy.isFixedLength()) {
          beginForFixLength = pos / CACHE_SIZE * CACHE_SIZE;
          long endInd = (pos + len)/ CACHE_SIZE;
          if ((pos + len) % CACHE_SIZE != 0) {
            endInd ++;
          }
          endForFixLength = endInd * CACHE_SIZE;
          unit = mCacheContext.getCache(mFileId, mLength, beginForFixLength, Math.min(endForFixLength,
                  mLength), task);
        } else {
          unit = mCacheContext.getCache(mFileId, mLength, pos, Math.min(pos + len,
                  mLength), task);
        }
        ClientCacheStatistics.INSTANCE.getCacheTime += (System.currentTimeMillis() - st);
        unit.setLockTask(task);
        if (unit.isFinish()) {
          ClientCacheContext.allHitTime ++;
          long st1 = System.currentTimeMillis();
          Preconditions.checkArgument(pos >= unit.getBegin());
          int remaining = mCachePolicy.read((CacheInternalUnit) unit, b, off, pos, len);
          if (!isPosition) {
            mPosition += remaining;
          }
          ClientCacheStatistics.INSTANCE.readUnitTime += (System.currentTimeMillis() - st1);
          return remaining;
        } else {
          long st1 = System.currentTimeMillis();
          TempCacheUnit tmpUnit = (TempCacheUnit) unit;
          tmpUnit.setInStream(this);
          if (mCachePolicy.isFixedLength()) {
            byte[]cachedBuf = new byte[(int)(endForFixLength-beginForFixLength)];
            long preAccessSize = HitRatioMetric.INSTANCE.accessSize;
            res = mCachePolicy.read(tmpUnit, cachedBuf, off, (int)(endForFixLength-beginForFixLength), beginForFixLength, mCacheContext.isAllowCache());

            if (res != -1) {
//              for(int i = 0; i < len; i ++) {
//                b[i] = cachedBuf[(int) (pos - beginForFixLength) + i];
//              }
//              b = Arrays.copyOfRange(cachedBuf, (int) (pos - beginForFixLength), len);
              System.arraycopy(cachedBuf, (int) (pos - beginForFixLength), b, 0, len);
              res = len;
              HitRatioMetric.INSTANCE.accessSize = preAccessSize + res;
            }
          } else {
            res = mCachePolicy.read(tmpUnit, b, off, len, pos, mCacheContext.isAllowCache());
          }
          if (res != len) {
            // the end of file
            tmpUnit.resetEnd((int) mLength);
          }
          ClientCacheStatistics.INSTANCE.readTmpUnitTime += (System.currentTimeMillis() - st1);
        }
      } finally {
        FixLengthReadNote.discardNote();
        mLockManager.evictReadUnlock();
      }
    } else {
      if (isPosition) {
        res = innerPositionRead(pos, b, off, len);
      } else {
        res = innerRead(b, off, len);
      }
    }
    return res;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length, PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    return read0(mPosition, b, off, len);
  }

  /**
   * sequential reading from file, no data coincidence in  reading process;
   * cached data after reading first time;
   * used by mt sequential reading.
   */
  public int sequentialReading(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length, PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (len == 0) {
      return 0;
    } else if (mPosition == mLength) { // at end of file
      return -1;
    }
    long pos = mPosition;
    LockTask task = new LockTask(mLockManager, mFileId);
    CacheUnit unit = mCacheContext.getCache(mFileId, mLength, pos, Math.min(pos + len, pos +
      (int) (mLength - mPosition)), task);
    if (unit.isFinish()) {
      int remaining = ((CacheInternalUnit) unit).positionedRead(b, off, pos, len);
      if (remaining > 0) mPosition += remaining;
      return remaining;
      //return -1;
    } else {
      int res = innerRead(b, off, len);
      if (res > 0) {
        TempCacheUnit tempUnit = (TempCacheUnit) unit;
        if (res != len) {
          tempUnit.resetEnd(mLength);
          mCacheContext.REVERSE = false;
        }
        tempUnit.addCache(b, off, res);
        mCacheContext.addCache(tempUnit);
      } else {
        mCacheContext.REVERSE = false;
      }
      return res;
    }
  }

  @Override
  public long skip(long n) throws IOException {
    long skipLength = super.skip(n);
    if (skipLength > 0) {
      mPosition += skipLength;
    }
    return skipLength;
  }
}

