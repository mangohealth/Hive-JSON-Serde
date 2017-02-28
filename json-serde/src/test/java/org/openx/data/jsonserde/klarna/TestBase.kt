package org.openx.data.jsonserde.klarna

import com.klarna.hiverunner.HiveShell
import com.klarna.hiverunner.StandaloneHiveRunner
import org.apache.commons.io.FileUtils
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith

@RunWith(StandaloneHiveRunner::class)
abstract class TestBase {

    /**
     * This *must* get declared in the child class for Klarna to find it
     */
    abstract val hiveShell:HiveShell?

    companion object {
        /**
         * This assumes json_line contains a single JSON entry and will create a
         * single object file containing all the new-lines stripped out of the
         * input param.  It will return a path to the base directory containing
         * that object file which can be used as the LOCATION for a CREATE EXTERNAL
         * TABLE statement.
         */
        fun getTestLocation(json_line:String):String {
            val tmpDir = TemporaryFolder()
            tmpDir.create()
            FileUtils.write(
                tmpDir.newFile(),
                json_line.trimIndent().replace(Regex("[\r\n]+"), " ")
            )
            return tmpDir.root.absolutePath
        }
    }

    fun execute(str:String) {
        hiveShell!!.execute(str)
    }

    fun query(queryStr:String):List<String> {
        return hiveShell!!.executeQuery(queryStr)
    }

    fun queryOne(queryStr:String):String {
        val results = query(queryStr)
        assertNotNull("Hive should not provide a null response!", results)
        assertEquals("Expected exactly 1 result!", 1, results.size)
        return results.first()
    }

}
