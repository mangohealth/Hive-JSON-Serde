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

}