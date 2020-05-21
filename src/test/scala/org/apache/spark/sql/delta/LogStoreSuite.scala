/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.delta

import java.io.{File, IOException}
import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.control.Breaks._
import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, CreateTableRequest, KeySchemaElement, ProvisionedThroughput}
import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import com.google.common.cache.CacheBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.storage._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, RawLocalFileSystem}
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils
import org.scalatest.Ignore


abstract class LogStoreSuiteBase extends QueryTest
  with LogStoreProvider
  with SharedSparkSession {

  def logStoreClassName: String

  protected override def sparkConf = {
    super.sparkConf.set(logStoreClassConfKey, logStoreClassName)
  }

  test("instantiation through SparkConf") {
    assert(spark.sparkContext.getConf.get(logStoreClassConfKey) == logStoreClassName)
    assert(LogStore(spark.sparkContext).getClass.getName == logStoreClassName)
  }

  test("read / write") {
    def assertNoLeakedCrcFiles(dir: File): Unit = {
      // crc file should not be leaked when origin file doesn't exist.
      // The implementation of Hadoop filesystem may filter out checksum file, so
      // listing files from local filesystem.
      val fileNames = dir.listFiles().toSeq.filter(p => p.isFile).map(p => p.getName)
      val crcFiles = fileNames.filter(n => n.startsWith(".") && n.endsWith(".crc"))
      val originFileNamesForExistingCrcFiles = crcFiles.map { name =>
        // remove first "." and last ".crc"
        name.substring(1, name.length - 4)
      }

      // Check all origin files exist for all crc files.
      assert(originFileNamesForExistingCrcFiles.toSet.subsetOf(fileNames.toSet),
        s"Some of origin files for crc files don't exist - crc files: $crcFiles / " +
          s"expected origin files: $originFileNamesForExistingCrcFiles / actual files: $fileNames")
    }

    val tempDir = Utils.createTempDir()
    val store = createLogStore(spark)

    val deltas = Seq(0, 1).map(i => new File(tempDir, i.toString)).map(_.getCanonicalPath)
    store.write(deltas.head, Iterator("zero", "none"))
    store.write(deltas(1), Iterator("one"))

    assert(store.read(deltas.head) == Seq("zero", "none"))
    assert(store.read(deltas(1)) == Seq("one"))

    assertNoLeakedCrcFiles(tempDir)
  }

  test("detects conflict") {
    val tempDir = Utils.createTempDir()
    val store = createLogStore(spark)

    val deltas = Seq(0, 1).map(i => new File(tempDir, i.toString)).map(_.getCanonicalPath)
    store.write(deltas.head, Iterator("zero"))
    store.write(deltas(1), Iterator("one"))

    intercept[java.nio.file.FileAlreadyExistsException] {
      store.write(deltas(1), Iterator("uno"))
    }
  }

  test("listFrom") {
    val tempDir = Utils.createTempDir()
    val store = createLogStore(spark)

    val deltas =
      Seq(0, 1, 2, 3, 4).map(i => new File(tempDir, i.toString)).map(_.toURI).map(new Path(_))
    store.write(deltas(1), Iterator("zero"))
    store.write(deltas(2), Iterator("one"))
    store.write(deltas(3), Iterator("two"))

    assert(
      store.listFrom(deltas.head).map(_.getPath.getName).toArray === Seq(1, 2, 3).map(_.toString))
    assert(
      store.listFrom(deltas(1)).map(_.getPath.getName).toArray === Seq(1, 2, 3).map(_.toString))
    assert(store.listFrom(deltas(2)).map(_.getPath.getName).toArray === Seq(2, 3).map(_.toString))
    assert(store.listFrom(deltas(3)).map(_.getPath.getName).toArray === Seq(3).map(_.toString))
    assert(store.listFrom(deltas(4)).map(_.getPath.getName).toArray === Nil)
  }

  test("simple log store test") {
    val tempDir = Utils.createTempDir()
    val log1 = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
    assert(log1.store.getClass.getName == logStoreClassName)

    val txn = log1.startTransaction()
    val file = AddFile("1", Map.empty, 1, 1, true) :: Nil
    txn.commit(file, ManualUpdate)
    log1.checkpoint()

    DeltaLog.clearCache()
    val log2 = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
    assert(log2.store.getClass.getName == logStoreClassName)

    assert(log2.lastCheckpoint.map(_.version) === Some(0L))
    assert(log2.snapshot.allFiles.count == 1)
  }

  protected def testHadoopConf(expectedErrMsg: String, fsImplConfs: (String, String)*): Unit = {
    test("should pick up fs impl conf from session Hadoop configuration") {
      withTempDir { tempDir =>
        val path = new Path(new URI(s"fake://${tempDir.toURI.getRawPath}/1.json"))

        // Make sure it will fail without FakeFileSystem
        val e = intercept[IOException] {
          createLogStore(spark).listFrom(path)
        }
        assert(e.getMessage.contains(expectedErrMsg))
        withSQLConf(fsImplConfs: _*) {
          createLogStore(spark).listFrom(path)
        }
      }
    }
  }

  /**
   * Whether the log store being tested should use rename to write checkpoint or not. The following
   * test is using this method to verify the behavior of `checkpoint`.
   */
  protected def shouldUseRenameToWriteCheckpoint: Boolean

  test("use isPartialWriteVisible to decide whether use rename") {
    withTempDir { tempDir =>
      import testImplicits._
      Seq(1, 2, 4).toDF().write.format("delta").save(tempDir.getCanonicalPath)
      withSQLConf(
          "fs.file.impl" -> classOf[TrackingRenameFileSystem].getName,
          "fs.file.impl.disable.cache" -> "true") {
        val logStore = DeltaLog.forTable(spark, tempDir.getCanonicalPath)
        TrackingRenameFileSystem.numOfRename = 0
        logStore.checkpoint()
        val expectedNumOfRename = if (shouldUseRenameToWriteCheckpoint) 1 else 0
        assert(TrackingRenameFileSystem.numOfRename === expectedNumOfRename)
      }
    }
  }
}

class AzureLogStoreSuite extends LogStoreSuiteBase {

  override val logStoreClassName: String = classOf[AzureLogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme: fake",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

class HDFSLogStoreSuite extends LogStoreSuiteBase {

  override val logStoreClassName: String = classOf[HDFSLogStore].getName
  // HDFSLogStore is based on FileContext APIs and hence requires AbstractFileSystem-based
  // implementations.
  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme: fake",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  import testImplicits._

  test("writes on systems without AbstractFileSystem implemented") {
    withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
      "fs.fake.impl.disable.cache" -> "true") {
      val tempDir = Utils.createTempDir()
      val path = new Path(new URI(s"fake://${tempDir.toURI.getRawPath}/1.json"))
      val e = intercept[IOException] {
        createLogStore(spark).write(path, Iterator("zero", "none"))
      }
      assert(e.getMessage.contains(
        DeltaErrors.incorrectLogStoreImplementationException(sparkConf, null).getMessage))
    }
  }

  test("reads should work on systems without AbstractFileSystem implemented") {
    withTempDir { tempDir =>
      val writtenFile = new File(tempDir, "1")
      val store = createLogStore(spark)
      store.write(writtenFile.getCanonicalPath, Iterator("zero", "none"))
      withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
        "fs.fake.impl.disable.cache" -> "true") {
        val read = createLogStore(spark).read("fake://" + writtenFile.getCanonicalPath)
        assert(read === ArrayBuffer("zero", "none"))
      }
    }
  }

  test("No AbstractFileSystem - end to end test using data frame") {
    // Writes to the fake file system will fail
    withTempDir { tempDir =>
      val fakeFSLocation = s"fake://${tempDir.getCanonicalFile}"
      withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
        "fs.fake.impl.disable.cache" -> "true") {
        val e = intercept[IOException] {
          Seq(1, 2, 4).toDF().write.format("delta").save(fakeFSLocation)
        }
        assert(e.getMessage.contains(
          DeltaErrors.incorrectLogStoreImplementationException(sparkConf, null).getMessage))
      }
    }
    // Reading files written by other systems will work.
    withTempDir { tempDir =>
      Seq(1, 2, 4).toDF().write.format("delta").save(tempDir.getAbsolutePath)
      withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
        "fs.fake.impl.disable.cache" -> "true") {
        val fakeFSLocation = s"fake://${tempDir.getCanonicalFile}"
        checkAnswer(spark.read.format("delta").load(fakeFSLocation), Seq(1, 2, 4).toDF())
      }
    }
  }

  protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

class LocalLogStoreSuite extends LogStoreSuiteBase {

  override val logStoreClassName: String = classOf[LocalLogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme: fake",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}


abstract class BaseExternalLogStoreSuite extends LogStoreSuiteBase {
  test("fix incomplete transaction test") {
    withTempDir { tempDir =>
      val path = new Path(s"fakeNonConsistent://${tempDir.toURI.getRawPath}")

      withSQLConf(
        "fs.fakeNonConsistent.impl" -> classOf[FakeNonConsistentFileSystem].getName,
        "fs.AbstractFileSystem.fakeNonConsistent.impl" ->
          classOf[FakeNonConsistentAbstractFileSystem].getName
      ) {
        val log = DeltaLog(spark, path)
        assert(log.store.getClass.getName == logStoreClassName)

        // rename temp file to destination should fail to test fix transactions
        FakeNonConsistentFileSystem.disabledRenameOnce = true
        val txn = log.startTransaction()
        val file = AddFile("1", Map.empty, 1, 1, dataChange = true) :: Nil
        txn.commit(file, ManualUpdate)
        log.checkpoint()

        val secondFile = AddFile("2", Map.empty, 1, 1, dataChange = true) :: Nil
        val secondTxn = log.startTransaction()
        secondTxn.commit(secondFile, ManualUpdate)

        log.checkpoint()

        assert(
          createLogStore(spark).listFrom(path + "/00000000000000000000.json").count(_ => true) == 3
        )
        DeltaLog.clearCache()
      }
    }
  }

  test("listFrom exceptions") {
    val store = createLogStore(spark)
    assertThrows[java.io.FileNotFoundException] {
      store.listFrom("/non-existing-path/with-parent")
    }
  }
}

class S3LogStoreSuite extends LogStoreSuiteBase {
  override val logStoreClassName: String = classOf[S3SingleDriverLogStore].getName

  protected def shouldUseRenameToWriteCheckpoint: Boolean = false
}

class TestMemoryLogStoreSuite extends BaseExternalLogStoreSuite {
  override val logStoreClassName: String = classOf[TestMemoryLogStore].getName
  protected def shouldUseRenameToWriteCheckpoint: Boolean = false
}

@Ignore
class DynamoDBLogStoreSuite extends BaseExternalLogStoreSuite with ForAllTestContainer {
  private val dynamoTableName = "delta_log"

  override def logStoreClassName: String = classOf[DynamoDBLogStore].getName
  protected def shouldUseRenameToWriteCheckpoint: Boolean = true

  override val container: GenericContainer = GenericContainer(
    "amazon/dynamodb-local",
    exposedPorts = Seq(8000)
  )

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.delta.DynamoDBLogStore.tableName", dynamoTableName)
      .set("spark.delta.DynamoDBLogStore.host", s"http://localhost:${container.mappedPort(8000)}")
      .set("spark.delta.DynamoDBLogStore.fakeAuth", "true")
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    val client = DynamoDBLogStore.getClient(sparkConf)

    client.createTable(
      new CreateTableRequest(
        List(
          new AttributeDefinition("parentPath", "S"),
          new AttributeDefinition("filename", "S")
        ).asJava,
        dynamoTableName,
        List(
          new KeySchemaElement("parentPath", "HASH"),
          new KeySchemaElement("filename", "RANGE")
        ).asJava,
        new ProvisionedThroughput(5L, 5L)
      )
    )

  }
}


/** A fake file system to test whether session Hadoop configuration will be picked up. */
class FakeFileSystem extends RawLocalFileSystem {
  override def getScheme: String = FakeFileSystem.scheme
  override def getUri: URI = FakeFileSystem.uri
}

object FakeFileSystem {
  val scheme = "fake"
  val uri = URI.create(s"$scheme:///")
}

/**
 * A fake AbstractFileSystem to test whether session Hadoop configuration will be picked up.
 * This is a wrapper around [[FakeFileSystem]].
 */
class FakeAbstractFileSystem(uri: URI, conf: org.apache.hadoop.conf.Configuration)
  extends org.apache.hadoop.fs.DelegateToFileSystem(
    uri,
    new FakeFileSystem,
    conf,
    FakeFileSystem.scheme,
    false) {

  // Implementation copied from RawLocalFs
  import org.apache.hadoop.fs.local.LocalConfigKeys
  import org.apache.hadoop.fs._

  override def getUriDefaultPort(): Int = -1
  override def getServerDefaults(): FsServerDefaults = LocalConfigKeys.getServerDefaults
  override def isValidName(src: String): Boolean = true
}

/**
 * A file system allowing to track how many times `rename` is called.
 * `TrackingRenameFileSystem.numOfRename` should be reset to 0 before starting to trace.
 */
class TrackingRenameFileSystem extends RawLocalFileSystem {
  override def rename(src: Path, dst: Path): Boolean = {
    TrackingRenameFileSystem.numOfRename += 1
    super.rename(src, dst)
  }
}

object TrackingRenameFileSystem {
  @volatile var numOfRename = 0
}

/**
 * A fake file system to test whether session Hadoop configuration will be picked up.
 * Filesystem is inconsistent, files there are in a listing after 1 second.
 * It is possible to disable the rename method by parameter.
 */
class FakeNonConsistentFileSystem extends RawLocalFileSystem {
  override def getScheme: String = FakeNonConsistentFileSystem.scheme

  override def getUri: URI = FakeNonConsistentFileSystem.uri

  override def listStatus(f: Path): Array[FileStatus] = {
    val now = System.currentTimeMillis - 1000
    super.listStatus(f).iterator.filter(file => file.getModificationTime < now).toArray
  }

  override def rename(src: Path, dst: Path): Boolean = {
    if (FakeNonConsistentFileSystem.disabledRename) {
      return false
    }
    if (FakeNonConsistentFileSystem.disabledRenameOnce) {
      FakeNonConsistentFileSystem.disabledRenameOnce = false
      return false
    }
    super.rename(src, dst)
  }
}

object FakeNonConsistentFileSystem {
  val scheme = "fakeNonConsistent"
  private val uri: URI = URI.create(s"$scheme:///")

  var disabledRename = false
  var disabledRenameOnce = false
}


class FakeNonConsistentAbstractFileSystem(uri: URI, conf: org.apache.hadoop.conf.Configuration)
  extends org.apache.hadoop.fs.DelegateToFileSystem(
    uri,
    new FakeNonConsistentFileSystem,
    conf,
    FakeNonConsistentFileSystem.scheme,
    false) {

  // Implementation copied from RawLocalFs
  import org.apache.hadoop.fs.local.LocalConfigKeys
  import org.apache.hadoop.fs._

  override def getUriDefaultPort(): Int = -1
  override def getServerDefaults(): FsServerDefaults = LocalConfigKeys.getServerDefaults
  override def isValidName(src: String): Boolean = true
}

class TestMemoryLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
  extends BaseExternalLogStore(sparkConf, hadoopConf) {

  import TestMemoryLogStore._

  private def releaseLock(logEntryMetadata: LogEntryMetadata) {
    val unlock = pathLock.remove(logEntryMetadata.path)
    unlock.synchronized {
      unlock.notifyAll()
    }
  }

  override protected def cleanCache(p: LogEntryMetadata => Boolean) {
    val keys = writtenPathCache
      .asMap()
      .asScala
      .filter { case (_, entry) => p(entry) }
      .keys
      .asJava

    writtenPathCache.invalidateAll(keys)
  }

  override def invalidateCache(): Unit = {
    writtenPathCache.invalidateAll()
  }

  override protected def listFromCache(
                                        fs: FileSystem,
                                        resolvedPath: Path): Iterator[LogEntryMetadata] = {
    val pathKey = getPathKey(resolvedPath)
    writtenPathCache
      .asMap()
      .asScala
      .iterator
      .filter { case (path, _) =>
        path.getParent == pathKey.getParent() && path.getName >= pathKey.getName
      }
      .map {
        case (_, logEntry) => logEntry
      }
  }

  override protected def writeCache(
                                     fs: FileSystem,
                                     logEntry: LogEntryMetadata,
                                     overwrite: Boolean = false): Unit = {

    logDebug(s"WriteExternalCache: ${logEntry.path} (overwrite=$overwrite)")

    if (!overwrite) {
      writeCacheExclusive(fs, logEntry)
      return
    }
    writtenPathCache.put(logEntry.path, logEntry)
  }

  protected def writeCacheExclusive(
                                     fs: FileSystem,
                                     logEntry: LogEntryMetadata
                                   ): Unit = {
    breakable {
      while (true) {
        val lock = pathLock.putIfAbsent(logEntry.path, new Object)
        if (lock == null) break
        lock.synchronized {
          while (pathLock.get(logEntry.path) == lock) {
            lock.wait()
          }
        }
      }
    }

    if (exists(fs, logEntry.path)) {
      releaseLock(logEntry)
      throw new java.nio.file.FileAlreadyExistsException(
        s"TransactionLog exists ${logEntry.path}"
      )
    }

    writtenPathCache.put(logEntry.path, logEntry)
    releaseLock(logEntry)
  }

}

object TestMemoryLogStore {
  /**
   * A global path lock to ensure that no concurrent writers writing to the same path in the same
   * JVM.
   */
  private val pathLock = new ConcurrentHashMap[Path, AnyRef]()

  /**
   * A global cache that records the metadata of the files recently written.
   * As list-after-write may be inconsistent on S3, we can use the files in the cache
   * to fix the inconsistent file listing.
   */
  private val writtenPathCache =
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(120, TimeUnit.MINUTES)
      .build[Path, LogEntryMetadata]()
}
