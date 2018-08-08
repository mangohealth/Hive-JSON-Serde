package org.openx.data.jsonserde.klarna

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.klarna.hiverunner.HiveShell
import com.klarna.hiverunner.StandaloneHiveRunner
import org.junit.Assert
import org.junit.runner.RunWith

@RunWith(StandaloneHiveRunner::class)
abstract class TestBase {

    abstract var hiveShell:HiveShell?

    companion object {
        val JSON_MAPPER = jacksonObjectMapper()

        init {
            JSON_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
            JSON_MAPPER.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        }
    }

    fun execute(str:String) {
        hiveShell!!.execute(str)
    }

    fun query(queryStr:String):List<String> {
        return hiveShell!!.executeQuery(queryStr)
    }

    fun queryOne(queryStr:String):String {
        println(queryStr)
        val results = query(queryStr)
        Assert.assertNotNull("Hive should not provide a null response!", results)
        Assert.assertEquals("Expected exactly 1 result!", 1, results.size)
        return results.first()
    }

    fun queryForRowJSON(queryStr:String):List<Any> {
        return queryOne(queryStr).split("\t").map {
            try {
                JSON_MAPPER.readValue<Any>(it)
            }
            catch(ex:JsonParseException) {
                // It may be the case that not all cols are json, so tread lightly
                it
            }
        }
    }

    inline fun <reified T : Any> queryForJSON(queryStr:String):T {
        return JSON_MAPPER.readValue(queryOne(queryStr))
    }

    /**
     * Parses Hive output as JSON to avoid having test expectation issues w/ Map key sorts!
     */
    inline fun <reified T : Any> queryForManyJSON(queryStr:String):List<T?> {
        val results = query(queryStr)
        return results.map {
            if(it.equals("NULL", ignoreCase = true)) {
                null
            }
            else {
                JSON_MAPPER.readValue<T>(it)
            }
        }
    }

}