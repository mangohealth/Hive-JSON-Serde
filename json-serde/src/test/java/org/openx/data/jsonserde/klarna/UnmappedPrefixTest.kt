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
class UnmappedPrefixTest {

    @Suppress("unused")
    @field:HiveSQL(files = arrayOf())
    var hiveShell:HiveShell? = null

    @Before
    fun prepare() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/unmapped_prefixes.txt"),
            tmpDir.newFile()
        )
        hiveShell!!.execute("""
            DROP TABLE IF EXISTS test_input;
            CREATE EXTERNAL TABLE test_input (
              listed1 INT,
              listed2 INT,
              a MAP<STRING, INT>,
              b MAP<STRING, FLOAT>,
              c map<string, string>,
              d MAP<STRING, ARRAY<INT>>,
              e MAP<STRING, MAP<STRING, INT>>,
              f MAP<STRING, BOOLEAN>,
              g MAP<STRING, STRING>,
              h MAP<STRING, STRUCT<
                h1:STRING,
                h2:INT
              >>,
              unmapped_cols MAP<STRING, STRING>
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              "unmapped.prefix.a_" = "a",
              "unmapped.prefix.b_" = "b",
              "unmapped.prefix.c_" = "c",
              "unmapped.prefix.d_" = "d",
              "unmapped.prefix.e_" = "e",
              "unmapped.prefix.f_" = "f",
              "unmapped.prefix.g_" = "g",
              "unmapped.prefix.h_" = "h",
              "unmapped.attr.key" = "unmapped_cols"
            )
            LOCATION '${tmpDir.root.absolutePath}';
        """)
    }

    @Test
    fun verifyFullRead() {
        var results = hiveShell!!.executeQuery("SELECT * FROM test_input")
        Assert.assertNotNull(results)
        Assert.assertEquals(1, results.size)
        println(results.first())
        val cols = results.first().split("\t")
        Assert.assertEquals("1", cols[0])
        Assert.assertEquals("2", cols[1])
        Assert.assertEquals("""{"a_456":2,"a_123":1,"a_789":3}""", cols[2])
        Assert.assertEquals("""{"b_123":1.1}""", cols[3])
        Assert.assertEquals("""{"c_123":"hello"}""", cols[4])
        Assert.assertEquals("""{"d_123":[1,2,3]}""", cols[5])
        Assert.assertEquals("""{"e_123":{"b":2,"a":1}}""", cols[6])
        Assert.assertEquals("""{"f_123":true}""", cols[7])
        Assert.assertEquals("""{"g_123":null}""", cols[8])
        Assert.assertEquals("""{"h_123":{"h1":"abc","h2":1}}""", cols[9])
        Assert.assertEquals("""{"bob":"123","nancy":"\"hello\""}""", cols[10])
    }

}