package org.openx.data.jsonserde.klarna

import com.klarna.hiverunner.HiveShell
import com.klarna.hiverunner.annotations.HiveSQL
import org.apache.commons.io.FileUtils
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.rules.TemporaryFolder

class JsonKeyReplacementsTest : TestBase() {

    @field:HiveSQL(files = arrayOf())
    override var hiveShell:HiveShell? = null

    @Test
    fun withConfig() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/json_key_replacements.txt"),
            tmpDir.newFile()
        )
        execute("""
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

        val cols = queryForRowJSON("SELECT * FROM test_input")
        assertEquals("Col count matches", 3, cols.size)
        assertEquals("untouched matches", 1, cols[0])
        assertEquals(
            "ary matches",
            listOf(
                mapOf("shoop" to mapOf("foot" to mapOf("testme" to 123))),
                mapOf("shoop" to mapOf("foot" to mapOf("testme" to 456)))
            ),
            cols[1]
        )
        assertEquals("troxy matches", "abc", cols[2])
    }

    @Test
    fun withoutConfig() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/json_key_replacements.txt"),
            tmpDir.newFile()
        )
        execute("""
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

        val cols = queryForRowJSON("SELECT * FROM test_input")
        assertEquals("Col count matches", 3, cols.size)
        assertEquals("untouched matches", 1, cols[0])
        assertEquals(
            "ary matches",
            listOf(
                mapOf("shoop" to mapOf("foot" to mapOf("noot" to 123))),
                mapOf("shoop" to mapOf("foot" to mapOf("noot" to 456)))
            ),
            cols[1]
        )
        assertEquals("troxy matches", "abc", cols[2])
    }

    @Test
    fun wildcard() {
        val tmpDir = TemporaryFolder()
        tmpDir.create()
        FileUtils.copyInputStreamToFile(
            this.javaClass.getResourceAsStream("/json_key_replacements_wildcard.txt"),
            tmpDir.newFile()
        )
        execute("""
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

        val cols = queryForRowJSON("SELECT * FROM test_input")
        assertEquals("Col count matches", 2, cols.size)
        assertEquals("untouched matches", 1, cols[0])
        assertEquals(
            "a matches",
            mapOf(
                "a_1" to mapOf("a_child" to mapOf("notouch" to "abc", "testme" to 123)),
                "a_2" to mapOf("a_child" to mapOf("notouch" to "def", "testme" to 456))
            ),
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
        execute("""
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
        val cols = queryForRowJSON("SELECT * FROM test_input")
        assertEquals("Col count matches", 2, cols.size)
        assertEquals("untouched matches", 1, cols[0])
        assertEquals(
            "a matches",
            mapOf(
                "a_1" to mapOf("a_child" to mapOf("notouch" to "abc", "testme2" to 123)),
                "a_2" to mapOf("a_child" to mapOf("notouch" to "def", "testme2" to 456))
            ),
            cols[1]
        )
    }

}