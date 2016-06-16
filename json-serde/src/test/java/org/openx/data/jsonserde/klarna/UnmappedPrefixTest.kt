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
              c map<STRING, STRING>,
              d MAP<STRING, ARRAY<INT>>,
              e MAP<STRING, MAP<STRING, INT>>,
              f MAP<STRING, BOOLEAN>,
              g MAP<STRING, STRING>,
              h MAP<STRING, STRUCT<
                h1:STRING,
                h2:INT
              >>,
              a2 MAP<STRING, STRING>,
              unmapped_cols MAP<STRING, STRING>
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              "prefix.for.a" = "a_",
              "prefix.for.b" = "b_",
              "prefix.for.c" = "c_",
              "prefix.for.d" = "d_",
              "prefix.for.e" = "e_",
              "prefix.for.f" = "f_",
              "prefix.for.g" = "g_",
              "prefix.for.h" = "h_",
              "prefix.for.a2" = "a_",
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
        Assert.assertEquals("regular col 1", "1", cols[0])
        Assert.assertEquals("regular col 2", "2", cols[1])
        Assert.assertEquals("int collecting", """{"a_456":2,"a_123":1,"a_789":3}""", cols[2])
        Assert.assertEquals("float collecting", """{"b_123":1.1}""", cols[3])
        Assert.assertEquals("string collecting", """{"c_123":"hello"}""", cols[4])
        Assert.assertEquals("array<int> collecting", """{"d_123":[1,2,3]}""", cols[5])
        Assert.assertEquals("map<string,int> collecting", """{"e_123":{"b":2,"a":1}}""", cols[6])
        Assert.assertEquals("boolean collecting", """{"f_123":true}""", cols[7])
        Assert.assertEquals("null collecting", """{"g_123":null}""", cols[8])
        Assert.assertEquals("struct collecting", """{"h_123":{"h1":"abc","h2":1}}""", cols[9])
        Assert.assertEquals("mapping the same prefix twice", """{"a_456":"2","a_123":"1","a_789":"3"}""", cols[10])
        Assert.assertEquals("unmapped entries still works", """{"bob":"123","nancy":"\"hello\""}""", cols[11])

    }

}