/*======================================================================*
 * Copyright (c) 2011, OpenX Technologies, Inc. All rights reserved.    *
 *                                                                      *
 * Licensed under the New BSD License (the "License"); you may not use  *
 * this file except in compliance with the License. Unless required     *
 * by applicable law or agreed to in writing, software distributed      *
 * under the License is distributed on an "AS IS" BASIS, WITHOUT        *
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     *
 * See the License for the specific language governing permissions and  *
 * limitations under the License. See accompanying LICENSE file.        *
 *======================================================================*/


package org.openx.data.jsonserde;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.Text;
import org.openx.data.jsonserde.json.JSONArray;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;
import org.openx.data.jsonserde.json.ReplaceNode;
import org.openx.data.jsonserde.json.JSONOptions;
import org.openx.data.jsonserde.objectinspector.JsonObjectInspectorFactory;
import org.openx.data.jsonserde.objectinspector.JsonStructOIOptions;

import javax.print.attribute.standard.DateTimeAtCompleted;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.openx.data.jsonserde.objectinspector.primitive.JavaStringTimestampObjectInspector;
import org.openx.data.jsonserde.objectinspector.primitive.ParsePrimitiveUtils;

/**
 * Properties:
 * ignore.malformed.json = true/false : malformed json will be ignored
 *         instead of throwing an exception
 * 
 * @author rcongiu
 */
public class JsonSerDe extends AbstractSerDe {

    public static final Log LOG = LogFactory.getLog(JsonSerDe.class);
    List<String> columnNames;
    List<TypeInfo> columnTypes;
    StructTypeInfo rowTypeInfo;
    StructObjectInspector rowObjectInspector;
    boolean[] columnSortOrderIsDesc;
    private SerDeStats stats;
    private boolean lastOperationSerialize;
    long deserializedDataSize;
    long serializedDataSize;

    // if set, will ignore malformed JSON in deserialization
    boolean ignoreMalformedJson = false;
    boolean explicitNull = false;

    // properties used in configuration
    public static final String PROP_IGNORE_MALFORMED_JSON = "ignore.malformed.json";

    // Causes the JSON parser not to fail when duplicated object keys are encountered
    boolean allowDuplicates = false;
    public static final String PROP_ALLOW_DUPLICATE_KEYS = "allow.duplicate.json.keys";

    public static final String PROP_DOTS_IN_KEYS = "dots.in.keys";
    public static final String PROP_CASE_INSENSITIVE ="case.insensitive" ;
    public static final String PROP_EXPLICIT_NULL ="explicit.null" ;

    // Allow table schema to define an extra MAP<STRING, STRING> column to dump all other base level keys into that
    // aren't otherwise defined in the schema
    public static final String PROP_UNMAPPED_ATTR_KEY = "unmapped.attr.key";

    // Allow first level object keys w/ a given prefix to get aggregated into a well formed table column
    public static final String PROP_PREFIX_MAPPING_PREFIX = "prefix.for.";

   JsonStructOIOptions options;

    /**
     * Initializes the SerDe.
     * Gets the list of columns and their types from the table properties.
     * Will use them to look into/create JSON data.
     * 
     * @param conf Hadoop configuration object
     * @param tbl  Table Properties
     * @throws SerDeException 
     */
    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        LOG.debug("Initializing SerDe");
        // Get column names and sort order
        String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        
        LOG.debug("columns " + columnNameProperty + " types " + columnTypeProperty);

        // all table column names
        if (columnNameProperty.length() == 0) {
            columnNames = new ArrayList<String>();
        } else {
            columnNames = Arrays.asList(columnNameProperty.split(","));
        }

        // all column types
        if (columnTypeProperty.length() == 0) {
            columnTypes = new ArrayList<TypeInfo>();
        } else {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }
        assert columnNames.size() == columnTypes.size();

        stats = new SerDeStats();

        // Create row related objects
        rowTypeInfo = (StructTypeInfo) TypeInfoFactory
                .getStructTypeInfo(columnNames, columnTypes);
        
        // build options
        boolean isCaseInsensitive = Boolean.parseBoolean(tbl.getProperty(PROP_CASE_INSENSITIVE, "true"));
        options = new JsonStructOIOptions(getMappings(tbl, isCaseInsensitive), getKeyReplacements(tbl));
        options.setCaseInsensitive(isCaseInsensitive);

        // Get the sort order
        String columnSortOrder = tbl.getProperty(serdeConstants.SERIALIZATION_SORT_ORDER);
        columnSortOrderIsDesc = new boolean[columnNames.size()];
        for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
            columnSortOrderIsDesc[i] = columnSortOrder != null && 
                    columnSortOrder.charAt(i) == '-';
        }

        // dots in key names. Substitute with underscores
        options.setDotsInKeyNames(Boolean.parseBoolean(tbl.getProperty(PROP_DOTS_IN_KEYS,"false")));

        JSONOptions.globalOptions = new JSONOptions().setCaseInsensitive(options.isCaseInsensitive());

        rowObjectInspector = (StructObjectInspector) JsonObjectInspectorFactory
                .getJsonObjectInspectorFromTypeInfo(rowTypeInfo, options);

        // other configuration
        ignoreMalformedJson = Boolean.parseBoolean(tbl
                .getProperty(PROP_IGNORE_MALFORMED_JSON, "false"));

        allowDuplicates = Boolean.parseBoolean(tbl
                .getProperty(PROP_ALLOW_DUPLICATE_KEYS, "false"));

        options.setUnmappedValuesFieldName(tbl.getProperty(PROP_UNMAPPED_ATTR_KEY));

        options.setPrefixMappings(getPrefixMappings(tbl));
        
        explicitNull = Boolean.parseBoolean(tbl
                .getProperty(PROP_EXPLICIT_NULL, "false"));
    }

    /**
     * Deserializes the object. Reads a Writable and uses JSONObject to
     * parse its text
     * 
     * @param w the text to parse
     * @return a JSONObject
     * @throws SerDeException 
     */
    @Override
    public Object deserialize(Writable w) throws SerDeException {
        Text rowText = (Text) w;
        deserializedDataSize = rowText.getBytes().length;
	
        // Try parsing row into JSON object
        Object jObj = null;
        
        try {
            String txt = rowText.toString().trim();
            
            if(txt.startsWith("{")) {
                jObj = new JSONObject(txt, allowDuplicates, options.getJsonKeyReplacements(), "deserialize-base");
            } else if (txt.startsWith("[")){
                jObj = new JSONArray(txt, allowDuplicates, options.getJsonKeyReplacements());
            }
        } catch (JSONException e) {
            // If row is not a JSON object, make the whole row NULL
            onMalformedJson("Row is not a valid JSON Object - JSONException: "
                    + e.getMessage(), rowText.toString().trim());
            try {
                jObj = new JSONObject("{}", allowDuplicates, options.getJsonKeyReplacements(), "malformed");
            } catch (JSONException ex) {
                onMalformedJson("Error parsing empty row. This should never happen.", "");
            }
        }
	
        return jObj;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowObjectInspector;
    }

    /**
     * We serialize to Text 
     * @return
     * 
     * @see org.apache.hadoop.io.Text
     */
    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    /**
     * Hive will call this to serialize an object. Returns a writable object
     * of the same class returned by <a href="#getSerializedClass">getSerializedClass</a>
     * 
     * @param obj The object to serialize
     * @param objInspector The ObjectInspector that knows about the object's structure
     * @return a serialized object in form of a Writable. Must be the 
     *         same type returned by <a href="#getSerializedClass">getSerializedClass</a>
     * @throws SerDeException 
     */
    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {        
        // make sure it is a struct record
        if (objInspector.getCategory() != Category.STRUCT) {
            throw new SerDeException(getClass().toString()
                    + " can only serialize struct types, but we got: "
                    + objInspector.getTypeName());
        }

        JSONObject serializer = 
            serializeStruct( obj, (StructObjectInspector) objInspector, columnNames);
        
        Text t = new Text(serializer.toString());
        
        serializedDataSize = t.getBytes().length;
        return t;
    }

    private String getSerializedFieldName( List<String> columnNames, int pos, StructField sf) {
        String n = columnNames==null? sf.getFieldName(): columnNames.get(pos);
        
        if(options.getMappings().containsKey(n)) {
            return options.getMappings().get(n);
        } else {
            return n;
        }
    }
    
    /**
     * Serializing means getting every field, and setting the appropriate 
     * JSONObject field. Actual serialization is done at the end when
     * the whole JSON object is built
     */
    private JSONObject serializeStruct( Object obj,
            StructObjectInspector soi, List<String> columnNames) {
        // do nothing for null struct
        if (null == obj) {
            return null;
        }

        JSONObject result = new JSONObject("serialize-parent");
        
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        
        for (int i =0; i< fields.size(); i++) {
            StructField sf = fields.get(i);
            Object data = soi.getStructFieldData(obj, sf);

            try {
                if (null != data) {

                        // we want to serialize columns with their proper HIVE name,
                        // not the _col2 kind of name usually generated upstream
                        result.put(
                                getSerializedFieldName(columnNames, i, sf),
                                serializeField(
                                    data,
                                    sf.getFieldObjectInspector()));
                } else if(explicitNull) {
                    result.putNull(getSerializedFieldName(columnNames, i, sf));
                }
            } catch (JSONException ex) {
                LOG.warn("Problem serializing", ex);
                throw new RuntimeException(ex);
            }
        }
        return result;
    }
   
    /**
     * Serializes a field. Since we have nested structures, it may be called
     * recursively for instance when defining a list<struct<>> 
     * 
     * @param obj Object holding the fields' content
     * @param oi  The field's objec inspector
     * @return  the serialized object
     */  
    public Object serializeField(Object obj,
            ObjectInspector oi ){
        if(obj == null) {return null;}
        
        Object result = null;
        switch(oi.getCategory()) {
            case PRIMITIVE:
                PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
                switch(poi.getPrimitiveCategory()) {
                    case VOID:
                        result = null;
                        break;
                    case BOOLEAN:
                        result = ((BooleanObjectInspector)poi).get(obj)?
                                            Boolean.TRUE:
                                            Boolean.FALSE;
                        break;
                    case BYTE:
                        result = ((ByteObjectInspector)poi).get(obj);
                        break;
                    case DOUBLE:
                        result = ((DoubleObjectInspector)poi).get(obj);
                        break;
                    case FLOAT:
                        result = ((FloatObjectInspector)poi).get(obj);
                        break;
                    case INT:
                        result = ((IntObjectInspector)poi).get(obj);
                        break;
                    case LONG:
                        result = ((LongObjectInspector)poi).get(obj);
                        break;
                    case SHORT:
                        result = ((ShortObjectInspector)poi).get(obj);
                        break;
                    case STRING:
                        result = ((StringObjectInspector)poi).getPrimitiveJavaObject(obj);
                        break;
                    case TIMESTAMP:
                        result = ParsePrimitiveUtils.serializeAsUTC(((TimestampObjectInspector)poi).getPrimitiveJavaObject(obj));
                        break;
                    case UNKNOWN:
                        throw new RuntimeException("Unknown primitive");
                    default:
                        break;
                }
                break;
            case MAP:
                result = serializeMap(obj, (MapObjectInspector) oi);
                break;
            case LIST:
                result = serializeList(obj, (ListObjectInspector)oi);
                break;
            case STRUCT:
                result = serializeStruct(obj, (StructObjectInspector)oi, null);
                break;
            case UNION:
                result = serializeUnion(obj, (UnionObjectInspector)oi);
            default:
                break;
        }
        return result;
    }

    /**
     * Serializes a Hive List using a JSONArray 
     * 
     * @param obj the object to serialize
     * @param loi the object's inspector
     * @return 
     */
    private JSONArray serializeList(Object obj, ListObjectInspector loi) {
        // could be an array of whatever!
        // we do it in reverse order since the JSONArray is grown on demand,
        // as higher indexes are added.
        if(obj==null) { return null; }
        
        JSONArray ar = new JSONArray();
        for(int i=loi.getListLength(obj)-1; i>=0; i--) {
            Object element = loi.getListElement(obj, i);
            try {
                ar.put(i, serializeField(element, loi.getListElementObjectInspector() ) );
            } catch (JSONException ex) {
                LOG.warn("Problem serializing array", ex);
                throw new RuntimeException(ex);
            }
        }
        return ar;
    }

    /**
     * Serializes a Union
     */
    private Object serializeUnion(Object obj, UnionObjectInspector oi) {
        if(obj == null) return null;

        return serializeField(obj, oi.getObjectInspectors().get(oi.getTag(obj)));
    }

    /**
     * Serializes a Hive map&lt;&gt; using a JSONObject.
     * 
     * @param obj the object to serialize
     * @param moi the object's inspector
     * @return 
     */
    private JSONObject serializeMap(Object obj, MapObjectInspector moi) {
        if (obj==null) { return null; }
        
        JSONObject jo = new JSONObject("serialize-map");
        Map m = moi.getMap(obj);
        
        for(Object k : m.keySet()) {
            try {
                jo.put(
                        serializeField(k, moi.getMapKeyObjectInspector()).toString(),
                        serializeField(m.get(k), moi.getMapValueObjectInspector()) );
            } catch (JSONException ex) {
                LOG.warn("Problem serializing map");
            }
        }
        return jo;
    }
    
    public void onMalformedJson(String msg, String input) throws SerDeException {
        if(ignoreMalformedJson) {
            System.err.println("Ignoring malformed JSON: " + msg);
            System.err.println("Which had this as input: " + input);
        }  else {
            throw new SerDeException(msg);
        }
    }

    @Override
    public SerDeStats getSerDeStats() {
        if(lastOperationSerialize) {
            stats.setRawDataSize(serializedDataSize);
        } else {
            stats.setRawDataSize(deserializedDataSize);
        }
        return stats;
    }

   
    public static final String PFX = "mapping.";
    /**
     * Builds mappings between hive columns and json attributes
     * 
     * @param tbl
     * @return 
     */
    private Map<String, String> getMappings(Properties tbl, boolean isCaseInsensitive) {
        int n = PFX.length();
        Map<String,String> mps = new HashMap<String,String>();
        
        for(Object o: tbl.keySet()) {
            if( ! (o instanceof String)) { continue ; }
            String s = (String) o;
            
            if(s.startsWith(PFX) ) {
                String fieldTo = tbl.getProperty(s);
                mps.put(s.substring(n), (isCaseInsensitive ? fieldTo.toLowerCase(): fieldTo));
            }
        }
        return mps;
    }

    public static final String CHANGE_KEY_TO_PREFIX = "changekeyto.";

    /**
     * Builds mappings for json attribute names in case we want to refer to them as something else in the schema.
     * This can be very useful for struct elements that use HiveQL keywords.
     *
     * @param tbl
     * @return
     */
    private ReplaceNode getKeyReplacements(Properties tbl) {
        int n = CHANGE_KEY_TO_PREFIX.length();
        ReplaceNode retVal = new ReplaceNode();

        for(Object o: tbl.keySet()) {
            if( ! (o instanceof String)) { continue ; }
            String s = (String) o;

            if(s.startsWith(CHANGE_KEY_TO_PREFIX)) {
                Map<String, ReplaceNode> branch = retVal.children;
                ReplaceNode node = null;
                for(String pathPart : tbl.getProperty(s).toLowerCase().split("\\.")) {
                    node = branch.get(pathPart);
                    if(node == null) {
                        node = new ReplaceNode();
                        branch.put(pathPart, node);
                    }
                    branch = node.children;
                }

                // Node should never be null!
                if(node != null) {
                    node.replaceWith = s.substring(n);
                }
            }
        }

        // For performance reasons, allow a clear signal to say "no swaps ever!"
        if(retVal.children.size() > 0) {
            return retVal;
        }
        return null;
    }

    /**
     * Creates mappings of column name to JSON key prefix to collect into that column.
     * @param tbl
     * @return
     */
    private Map<String, String[]> getPrefixMappings(Properties tbl) {
        int configSuffixLen = PROP_PREFIX_MAPPING_PREFIX.length();
        Map<String, String[]> retVal = new HashMap<String,String[]>();

        for(Object entry: tbl.keySet()) {
            if(entry instanceof String) {
                String configKey = (String) entry;
                if (configKey.startsWith(PROP_PREFIX_MAPPING_PREFIX)) {
                    String fieldName = configKey.substring(configSuffixLen);
                    String prefixesRaw = tbl.getProperty(configKey).toLowerCase();
                    String[] prefixes = prefixesRaw.split(",[ \t]*");
                    retVal.put(fieldName, prefixes);
                }
            }
        }

        return retVal;
    }


    
}
