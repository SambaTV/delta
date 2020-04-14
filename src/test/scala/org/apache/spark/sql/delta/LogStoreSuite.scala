/*
 * Copyright 2019 Databricks, Inc.
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

import scala.collection.JavaConverters._
import com.amazonaws.services.dynamodbv2.model.{
  AttributeDefinition,
  CreateTableRequest,
  KeySchemaElement,
  ProvisionedThroughput
}
import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.storage._
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

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
}

class AzureLogStoreSuite extends LogStoreSuiteBase {

  override val logStoreClassName: String = classOf[AzureLogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme: fake",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")
}

class HDFSLogStoreSuite extends LogStoreSuiteBase {

  override val logStoreClassName: String = classOf[HDFSLogStore].getName
  // HDFSLogStore is based on FileContext APIs and hence requires AbstractFileSystem-based
  // implementations.
  testHadoopConf(
    expectedErrMsg = "No AbstractFileSystem",
    "fs.AbstractFileSystem.fake.impl" -> classOf[FakeAbstractFileSystem].getName)
}

class LocalLogStoreSuite extends LogStoreSuiteBase {

  override val logStoreClassName: String = classOf[LocalLogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme: fake",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")
}


abstract class BaseExternalLogStoreSuite extends LogStoreSuiteBase {
  test("fix incomplete transaction test") {
    withTempDir { tempDir =>
      val path = new Path(new URI(s"fakeNonConsistent://${tempDir.toURI.getRawPath}"))

      withSQLConf("fs.fakeNonConsistent.impl" -> classOf[FakeNonConsistentFileSystem].getName) {
        val log = DeltaLog(spark, path)
        assert(log.store.getClass.getName == logStoreClassName)

        // rename temp file to destination should fail to test fix transactions
        FakeNonConsistentFileSystem.disabledRename = true
        val txn = log.startTransaction()
        val file = AddFile("1", Map.empty, 1, 1, dataChange = true) :: Nil

        assertThrows[java.nio.file.FileSystemException] {
          txn.commit(file, ManualUpdate)
        }

        FakeNonConsistentFileSystem.disabledRename = false
        assert(
          createLogStore(spark).listFrom(path + "/00000000000000000000.json").count(_ => true) == 1
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
}

class MemoryLogStoreSuite extends BaseExternalLogStoreSuite {
  override val logStoreClassName: String = classOf[MemoryLogStore].getName
}

class DynamoDBLogStoreSuite extends BaseExternalLogStoreSuite with ForAllTestContainer {
  private val dynamoTableName = "delta_log"

  override def logStoreClassName: String = classOf[DynamoDBLogStore].getName

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
    super.rename(src, dst)
  }
}

object FakeNonConsistentFileSystem {
  private val scheme = "fakeNonConsistent"
  private val uri: URI = URI.create(s"$scheme:///")

  var disabledRename = false
}
