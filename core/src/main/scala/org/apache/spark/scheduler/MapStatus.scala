/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
 */
private[spark] sealed trait MapStatus {
  /** Location where this task was run. */
  def location: BlockManagerId

  /**
   * Estimated size for the reduce block, in bytes.
   *
   * If a block is non-empty, then this method MUST return a non-zero size.  This invariant is
   * necessary for correctness, since block fetchers are allowed to skip zero-size blocks.
   */
  def getSizeForBlock(reduceId: Int): Long

  def getRecordForBlock(reduceId: Int): Long
}


private[spark] object MapStatus {

  // we use Array[Long]() as uncompressedRecords's default value,
  // main consideration is ser/deser do not accept null.
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long],
    uncompressedRecords: Array[Long] = Array[Long]()): MapStatus = {
    val verbose = Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_STATISTICS_VERBOSE))
      .getOrElse(config.SHUFFLE_STATISTICS_VERBOSE.defaultValue.get)
    val threshold = Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_HIGHLY_COMPRESSED_MAP_STATUS_THRESHOLD))
      .getOrElse(config.SHUFFLE_HIGHLY_COMPRESSED_MAP_STATUS_THRESHOLD.defaultValue.get)

    val newRecords = if (verbose) {
      uncompressedRecords
    } else {
      Array[Long]()
    }
    if (uncompressedSizes.length > threshold) {
      HighlyCompressedMapStatus(loc, uncompressedSizes, newRecords)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes, newRecords)
    }
  }

  private[this] val LOG_BASE = 1.1

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }
}


/**
 * A [[MapStatus]] implementation that tracks the size of each block. Size for each block is
 * represented using a single byte.
 *
 * @param loc location where the task is being executed.
 * @param compressedSizes size of the blocks, indexed by reduce partition id.
 */
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte],
    private[this] var compressedRecords: Array[Byte])
  extends MapStatus with Externalizable {

  protected def this() = this(null, null.asInstanceOf[Array[Byte]],
    null.asInstanceOf[Array[Byte]])  // For deserialization only

  def this(loc: BlockManagerId, uncompressedSizes: Array[Long],
    uncompressedRecords: Array[Long] = Array[Long]()) {
    this(loc, uncompressedSizes.map(MapStatus.compressSize),
      uncompressedRecords.map(MapStatus.compressSize))
  }

  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    MapStatus.decompressSize(compressedSizes(reduceId))
  }

  override def getRecordForBlock(reduceId: Int): Long = {
    if (compressedRecords.nonEmpty) {
      MapStatus.decompressSize(compressedRecords(reduceId))
    } else {
      -1
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
    out.writeInt(compressedRecords.length)
    out.write(compressedRecords)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val len = in.readInt()
    compressedSizes = new Array[Byte](len)
    in.readFully(compressedSizes)
    val recordsLen = in.readInt()
    compressedRecords = new Array[Byte](recordsLen)
    in.readFully(compressedRecords)
  }
}

/**
 * A [[MapStatus]] implementation that stores the accurate size of huge blocks, which are larger
 * than spark.shuffle.accurateBlockThreshold. It stores the average size of other non-empty blocks,
 * plus a bitmap for tracking which blocks are empty.
 *
 * @param loc location where the task is being executed
 * @param numNonEmptyBlocks the number of non-empty blocks
 * @param emptyBlocks a bitmap tracking which blocks are empty
 * @param avgSmallSize average size of the non-empty and non-huge blocks
 * @param hugeBlockSizes sizes of huge blocks by their reduceId.
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    private[this] var numNonEmptyBlocks: Int,
    private[this] var emptyBlocks: RoaringBitmap,
    private[this] var tinySizeBlocks: RoaringBitmap,
    private[this] var tinyRecordBlocks: RoaringBitmap,
    private[this] var avgSmallSize: Long,
    private[this] var avgTinySize: Long,
    private var hugeBlockSizes: Map[Int, Byte],
    private[this] var avgSmallRecord: Long,
    private[this] var avgTinyRecord: Long,
    private var hugeBlockRecords: Map[Int, Byte])
  extends MapStatus with Externalizable {

  // loc could be null when the default constructor is called during deserialization
  require(loc == null || avgTinySize > 0 || avgSmallSize > 0 || hugeBlockSizes.size > 0 ||
    numNonEmptyBlocks == 0, "Average size can only be zero for map stages that produced no output")

  // For deserialization only
  protected def this() = this(null, -1, null, null, null, -1, -1, null, -1, -1, null)

  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    assert(hugeBlockSizes != null)
    if (emptyBlocks.contains(reduceId)) {
      0
    } else if (tinySizeBlocks.contains(reduceId)) {
      avgTinySize
    } else {
      hugeBlockSizes.get(reduceId) match {
        case Some(size) => MapStatus.decompressSize(size)
        case None => avgSmallSize
      }
    }
  }

  override def getRecordForBlock(reduceId: Int): Long = {
    assert(hugeBlockSizes != null)
    if (avgSmallRecord != -1) {
      if (emptyBlocks.contains(reduceId)) {
        0
      } else if (tinyRecordBlocks.contains(reduceId)) {
        avgTinyRecord
      } else {
        hugeBlockRecords.get(reduceId) match {
          case Some(record) => MapStatus.decompressSize(record)
          case None => avgSmallRecord
        }
      }
    } else {
      -1
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    emptyBlocks.writeExternal(out)
    tinySizeBlocks.writeExternal(out)
    tinyRecordBlocks.writeExternal(out)
    out.writeLong(avgSmallSize)
    out.writeLong(avgTinySize)
    out.writeInt(hugeBlockSizes.size)
    hugeBlockSizes.foreach { kv =>
      out.writeInt(kv._1)
      out.writeByte(kv._2)
    }
    out.writeLong(avgSmallRecord)
    out.writeLong(avgTinyRecord)
    out.writeInt(hugeBlockRecords.size)
    hugeBlockRecords.foreach { kv =>
      out.writeInt(kv._1)
      out.writeByte(kv._2)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    emptyBlocks = new RoaringBitmap()
    emptyBlocks.readExternal(in)
    tinySizeBlocks = new RoaringBitmap()
    tinySizeBlocks.readExternal(in)
    tinyRecordBlocks = new RoaringBitmap()
    tinyRecordBlocks.readExternal(in)
    avgSmallSize = in.readLong()
    avgTinySize = in.readLong()
    val count = in.readInt()
    val hugeBlockSizesArray = mutable.ArrayBuffer[Tuple2[Int, Byte]]()
    (0 until count).foreach { _ =>
      val block = in.readInt()
      val size = in.readByte()
      hugeBlockSizesArray += Tuple2(block, size)
    }
    hugeBlockSizes = hugeBlockSizesArray.toMap
    avgSmallRecord = in.readLong()
    avgTinyRecord = in.readLong()
    val recordCount = in.readInt()
    val hugeBlockRecordsArray = mutable.ArrayBuffer[Tuple2[Int, Byte]]()
    (0 until recordCount).foreach { _ =>
      val block = in.readInt()
      val record = in.readByte()
      hugeBlockRecordsArray += Tuple2(block, record)
    }
    hugeBlockRecords = hugeBlockRecordsArray.toMap
  }
}

private[spark] object HighlyCompressedMapStatus {
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long],
    uncompressedRecords: Array[Long] = Array[Long]()): HighlyCompressedMapStatus = {
    // We must keep track of which blocks are empty so that we don't report a zero-sized
    // block as being non-empty (or vice-versa) when using the average block size.
    var i = 0
    var numNonEmptyBlocks: Int = 0
    var sizeSmallBlocks: Long = 0
    var numSmallBlocks: Long = 0
    var sizeTinyBlocks: Long = 0
    var numTinyBlocks: Long = 0
    // From a compression standpoint, it shouldn't matter whether we track empty or non-empty
    // blocks. From a performance standpoint, we benefit from tracking empty blocks because
    // we expect that there will be far fewer of them, so we will perform fewer bitmap insertions.
    val emptyBlocks = new RoaringBitmap()
    var tinySizeBlocks = new RoaringBitmap()
    var tinyRecordBlocks = new RoaringBitmap()
    val totalNumBlocks = uncompressedSizes.length
    val threshold = Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_ACCURATE_BLOCK_SIZE_THRESHOLD))
      .getOrElse(config.SHUFFLE_ACCURATE_BLOCK_SIZE_THRESHOLD.defaultValue.get)
    val smallTinyRatio = Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_ACCURATE_BLOCK_SMALL_TINY_RATIO))
      .getOrElse(config.SHUFFLE_ACCURATE_BLOCK_SMALL_TINY_RATIO.defaultValue.get)
    val smallThreshold = Math.max(1, threshold / smallTinyRatio)
    val hugeBlockSizesArray = ArrayBuffer[Tuple2[Int, Byte]]()
    while (i < totalNumBlocks) {
      val size = uncompressedSizes(i)
      if (size > 0) {
        numNonEmptyBlocks += 1
        // Huge blocks are not included in the calculation for average size, thus size for smaller
        // blocks is more accurate.
        if (size < threshold) {
          if (size < smallThreshold) {
            sizeTinyBlocks += size
            numTinyBlocks += 1
            tinySizeBlocks.add(i)
          } else {
            sizeSmallBlocks += size
            numSmallBlocks += 1
          }
        } else {
          hugeBlockSizesArray += Tuple2(i, MapStatus.compressSize(uncompressedSizes(i)))
        }
      } else {
        emptyBlocks.add(i)
      }
      i += 1
    }

    val avgTinySize = if (numTinyBlocks > 0) {
      sizeTinyBlocks / numTinyBlocks
    } else {
      0
    }
    val avgSmallSize = if (numSmallBlocks > 0) {
      sizeSmallBlocks / numSmallBlocks
    } else {
      0
    }

    var recordSmallBlocks: Long = 0
    numSmallBlocks = 0
    var recordTinyBlocks: Long = 0
    numTinyBlocks = 0
    var avgSmallRecord: Long = -1
    var avgTinyRecord: Long = -1
    val recordThreshold = Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_ACCURATE_BLOCK_RECORD_THRESHOLD))
      .getOrElse(config.SHUFFLE_ACCURATE_BLOCK_RECORD_THRESHOLD.defaultValue.get)
    val recordSmallThreshold = Math.max(1, recordThreshold / smallTinyRatio)
    val hugeBlockRecordsArray = ArrayBuffer[Tuple2[Int, Byte]]()
    if (uncompressedRecords.nonEmpty) {
      i = 0
      while (i < totalNumBlocks) {
        val record = uncompressedRecords(i)
        if (record > 0) {
          if (record < recordThreshold) {
            if (record < recordSmallThreshold) {
              recordTinyBlocks += record
              numTinyBlocks += 1
              tinyRecordBlocks.add(i)
            } else {
              recordSmallBlocks += record
              numSmallBlocks += 1
            }
          } else {
            hugeBlockRecordsArray += Tuple2(i, MapStatus.compressSize(uncompressedRecords(i)))
          }
        }
        i += 1
      }

      avgTinyRecord = if (numTinyBlocks > 0) {
        recordTinyBlocks / numTinyBlocks
      } else {
        0
      }
      avgSmallRecord = if (numSmallBlocks > 0) {
        recordSmallBlocks / numSmallBlocks
      } else {
        0
      }
    }

    emptyBlocks.trim()
    emptyBlocks.runOptimize()
    tinySizeBlocks.trim()
    tinySizeBlocks.runOptimize()
    tinyRecordBlocks.trim()
    tinyRecordBlocks.runOptimize()
    new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, tinySizeBlocks,
      tinyRecordBlocks, avgSmallSize, avgTinySize, hugeBlockSizesArray.toMap, avgSmallRecord,
      avgTinyRecord, hugeBlockRecordsArray.toMap)
  }
}
