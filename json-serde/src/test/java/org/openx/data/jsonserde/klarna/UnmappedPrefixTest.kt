package org.openx.data.jsonserde.klarna

import com.klarna.hiverunner.HiveShell
import com.klarna.hiverunner.annotations.HiveSQL
import org.apache.commons.io.FileUtils
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.rules.TemporaryFolder

class UnmappedPrefixTest : TestBase() {

    @field:HiveSQL(files = arrayOf())
    override var hiveShell:HiveShell? = null

    @Test
    fun verifyFullRead() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/unmapped_prefixes.txt"),
            tmpDir.newFile()
        )
        execute("""
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

        val cols = queryForRowJSON("SELECT * FROM test_input")
        assertEquals("column count should match", 12, cols.size)
        assertEquals("regular col 1", 1, cols[0])
        assertEquals("regular col 2", 2, cols[1])
        assertEquals(
            "int collecting",
            mapOf(
                "a_123" to 1,
                "a_456" to 2,
                "a_789" to 3
            ),
            cols[2]
        )
        assertEquals("float collecting", mapOf("b_123" to 1.1), cols[3])
        assertEquals("string collecting", mapOf("c_123" to "hello"), cols[4])
        assertEquals("array<int> collecting", mapOf("d_123" to listOf(1,2,3)), cols[5])
        assertEquals("map<string,int> collecting",
            mapOf("e_123" to mapOf("b" to 2, "a" to 1)),
            cols[6]
        )
        assertEquals("boolean collecting", mapOf("f_123" to true), cols[7])
        assertEquals("null collecting", mapOf("g_123" to null), cols[8])
        assertEquals("struct collecting",
            mapOf("h_123" to mapOf("h1" to "abc", "h2" to 1)),
            cols[9]
        )
        assertEquals("mapping the same prefix twice",
            mapOf("a_456" to "2", "a_123" to "1", "a_789" to "3"),
            cols[10]
        )
        assertEquals("unmapped entries still works",
            mapOf("bob" to "123", "nancy" to "\"hello\""),
            cols[11]
        )
    }

    @Test
    fun verifyRepeatedKeys() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/unmapped_prefix_merge.txt"),
            tmpDir.newFile()
        )
        execute("""
            DROP TABLE IF EXISTS test_input;
            CREATE EXTERNAL TABLE test_input (
              merged MAP<STRING, STRUCT<
                k1:STRING,
                k2:INT
              >>
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              "prefix.for.merged" = "a_,b_,c_"
            )
            LOCATION '${tmpDir.root.absolutePath}';
        """)

        assertEquals(
            mapOf(
                "a_123" to mapOf("k1" to "hola", "k2" to 1),
                "a_456" to mapOf("k1" to "hola", "k2" to 2),
                "b_123" to mapOf("k1" to "hola", "k2" to 3),
                "b_456" to mapOf("k1" to "hola", "k2" to 4),
                "c_123" to mapOf("k1" to "hola", "k2" to 5),
                "c_456" to mapOf("k1" to "hola", "k2" to 6)
            ),
            queryForJSON("SELECT * FROM test_input")
        )
    }

}