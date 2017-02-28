package org.openx.data.jsonserde.klarna

import com.klarna.hiverunner.HiveShell
import com.klarna.hiverunner.annotations.HiveSQL
import org.junit.Assert.assertEquals
import org.junit.Test

class BasePrefixClashTest : TestBase() {

    @field:HiveSQL(files = arrayOf())
    override var hiveShell:HiveShell? = null

    // TODO Fix this!
    /**
     * The "prefix.for.*" option should *not* execute recursively!  At least, that
     * should not be done in this way for now.  It's too surprising to the end-user.
     */
    @Test
    fun commonColNamesShouldNotCompete() {
        val location = getTestLocation("""
            {
                "thing_123": {"value":111},
                "thing_456": {"value":111},
                "unrelated_789":{"things":999, "thing_123": {"value":111}}
            }
        """)
        execute("""
            CREATE EXTERNAL TABLE test_input (
              things MAP<STRING, STRUCT<value:INT>>,
              unrelated MAP<STRING, STRUCT<things:STRING>>,
              unmapped_cols MAP<STRING, STRING>
            )
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            WITH SERDEPROPERTIES (
              "prefix.for.things" = "thing_",
              "prefix.for.unrelated" = "unrelated_",
              "unmapped.attr.key" = "unmapped_cols"
            )
            LOCATION '${location}'
        """)

        var result = queryOne("SELECT * FROM test_input")
        val cols = result.split("\t")
        assertEquals("things", """{"thing_123":{"value":111},"thing_456":{"value":111}}""", cols[0])
        assertEquals("unrelated", """{"unrelated_789":{"things":"999"}}""", cols[1])
        assertEquals("unmapped_cols", "{}", cols[2])
    }

}