package org.openx.data.jsonserde.klarna

import com.klarna.hiverunner.HiveShell
import com.klarna.hiverunner.annotations.HiveSQL
import org.apache.commons.io.FileUtils
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.rules.TemporaryFolder

class UnmappedAttrsTest : TestBase() {

    @field:HiveSQL(files = arrayOf())
    override var hiveShell:HiveShell? = null

    @Test
    fun verifyFullRead() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/unmapped_attrs.txt"),
            tmpDir.newFile()
        )
        execute("""
            DROP TABLE IF EXISTS test_input;
            CREATE EXTERNAL TABLE test_input (
              listed1 INT,
              listed2 INT,
              unmapped_cols MAP<STRING, STRING>
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              "unmapped.attr.key" = "unmapped_cols"
            )
            LOCATION '${tmpDir.root.absolutePath}';
        """)
        val cols = queryForRowJSON("SELECT * FROM test_input")
        assertEquals("Col count matches", 3, cols.size)
        assertEquals("listed1 matches", 1, cols[0])
        assertEquals("listed2 matches", 2, cols[1])
        assertEquals(
            "unmapped_cols matches",
            mapOf(
                "f" to "true",
                "g" to null,
                "d" to "[1,2,3]",
                "e" to "{\"a\":1,\"b\":2}",
                "b" to "1.1",
                "c" to "\"hello\"",
                "a" to "1"
            ).toSortedMap(),
            cols[2]
        )
    }

}