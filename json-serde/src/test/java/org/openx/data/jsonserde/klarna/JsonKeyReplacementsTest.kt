package org.openx.data.jsonserde.klarna

import com.klarna.hiverunner.HiveShell
import com.klarna.hiverunner.StandaloneHiveRunner
import com.klarna.hiverunner.annotations.HiveSQL
import org.apache.commons.io.FileUtils
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith


@RunWith(StandaloneHiveRunner::class)
class JsonKeyReplacementsTest {

    @Suppress("unused")
    @field:HiveSQL(files = arrayOf())
    var hiveShell:HiveShell? = null

    @Test
    fun withConfig() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/json_key_replacements.txt"),
            tmpDir.newFile()
        )
        hiveShell!!.execute("""
            DROP TABLE IF EXISTS test_input;
            CREATE EXTERNAL TABLE test_input (
              untouched INT,

              ary ARRAY<STRUCT<
                shoop:STRUCT<
                    foot:STRUCT<
                        testme:INT
                    >
                >
              >>,

              troxy STRING
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              "changekeyto.testme" = "ary.shoop.foot.noot",
              "changekeyto.troxy" = "foxy"
            )
            LOCATION '${tmpDir.root.absolutePath}';
        """)

        var results = hiveShell!!.executeQuery("SELECT * FROM test_input")
        Assert.assertNotNull(results)
        Assert.assertEquals(1, results.size)
        println(results.first())
        val cols = results.first().split("\t")
        Assert.assertEquals("1", cols[0])
        Assert.assertEquals(
            """[{"shoop":{"foot":{"testme":123}}},{"shoop":{"foot":{"testme":456}}}]""",
            cols[1]
        )
        Assert.assertEquals("abc", cols[2])
    }

    @Test
    fun withoutConfig() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/json_key_replacements.txt"),
            tmpDir.newFile()
        )
        hiveShell!!.execute("""
            DROP TABLE IF EXISTS test_input;
            CREATE EXTERNAL TABLE test_input (
              untouched INT,

              ary ARRAY<STRUCT<
                shoop:STRUCT<
                    foot:STRUCT<
                        noot:INT
                    >
                >
              >>,

              foxy STRING
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            LOCATION '${tmpDir.root.absolutePath}';
        """)

        var results = hiveShell!!.executeQuery("SELECT * FROM test_input")
        Assert.assertNotNull(results)
        Assert.assertEquals(1, results.size)
        println(results.first())
        val cols = results.first().split("\t")
        Assert.assertEquals("1", cols[0])
        Assert.assertEquals(
            """[{"shoop":{"foot":{"noot":123}}},{"shoop":{"foot":{"noot":456}}}]""",
            cols[1]
        )
        Assert.assertEquals("abc", cols[2])
    }

    @Test
    fun wildcard() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/json_key_replacements_wildcard.txt"),
            tmpDir.newFile()
        )
        hiveShell!!.execute("""
            DROP TABLE IF EXISTS test_input;
            CREATE EXTERNAL TABLE test_input (
              untouched INT,

              a MAP<STRING, STRUCT<
                a_child:STRUCT<
                    notouch:STRING,
                    testme:INT
                >
              >>
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              "changekeyto.testme" = "*.a_child.noot",
              "prefix.for.a" = "a_"
            )
            LOCATION '${tmpDir.root.absolutePath}';
        """)

        var results = hiveShell!!.executeQuery("SELECT * FROM test_input")
        Assert.assertNotNull(results)
        Assert.assertEquals(1, results.size)
        println(results.first())
        val cols = results.first().split("\t")
        Assert.assertEquals("1", cols[0])
        Assert.assertEquals(
            """{"a_2":{"a_child":{"notouch":"def","testme":456}},"a_1":{"a_child":{"notouch":"abc","testme":123}}}""",
            cols[1]
        )
    }

    @Test
    fun wildcardPrefix() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/json_key_replacements_wildcard.txt"),
            tmpDir.newFile()
        )
        hiveShell!!.execute("""
            DROP TABLE IF EXISTS test_input;
            CREATE EXTERNAL TABLE test_input (
              untouched INT,

              a MAP<STRING, STRUCT<
                a_child:STRUCT<
                    notouch:STRING,
                    testme2:INT
                >
              >>
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              "changekeyto.testme1" = "a*.a_child.noot",
              "changekeyto.testme2" = "a_*.a_child.noot",
              "changekeyto.testme3" = "*.a_child.noot",
              "prefix.for.a" = "a_"
            )
            LOCATION '${tmpDir.root.absolutePath}';
        """)

        // Make sure the longest wildcard match always wins!
        var results = hiveShell!!.executeQuery("SELECT * FROM test_input")
        Assert.assertNotNull(results)
        Assert.assertEquals(1, results.size)
        println(results.first())
        val cols = results.first().split("\t")
        Assert.assertEquals("1", cols[0])
        Assert.assertEquals(
            """{"a_2":{"a_child":{"notouch":"def","testme2":456}},"a_1":{"a_child":{"notouch":"abc","testme2":123}}}""",
            cols[1]
        )
    }

}