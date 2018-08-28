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

package org.openx.data.jsonserde.objectinspector;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.openx.data.jsonserde.json.JSONArray;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;

/**
 * @author rcongiu
 */
public class JsonListObjectInspector extends StandardListObjectInspector {

  public static final Log LOG = LogFactory.getLog(JsonListObjectInspector.class);

    JsonListObjectInspector(ObjectInspector listElementObjectInspector) {
        super(listElementObjectInspector);
    }

    @Override
    public List<?> getList(Object data) {
        if (data == null || JSONObject.NULL.equals(data)) {
            return null;
        }
        JSONArray array = getJSONArray(data);
        if(array==null) return null;

        List<Object> al = new ArrayList<Object>(array.length());
        for (int i = 0; i < array.length(); i++) {
            al.add(getListElement(data, i));
        }
        return al;
    }

    @Override
    public Object getListElement(Object data, int index) {
        if (data == null) {
            return null;
        }
        JSONArray array = getJSONArray(data);
        if(array==null) return null;

        try {
            Object obj = array.get(index);
            if (JSONObject.NULL == obj) {
                return null;
            } else {
                return obj;
            }
        } catch (JSONException ex) {
            return null;
        }
    }
    
    @Override
    public int getListLength(Object data) {
        if (data == null) {
            return -1;
        }
        JSONArray array = getJSONArray(data);
        if(array == null) return -1;

        return array.length();
    }

  private JSONArray getJSONArray(Object data) {
    if(!(data instanceof JSONArray)) {
      // Allow empty objects to get translated as empty lists if that's what we expected.
      // Do this because when we fail to parse some JSON, it'll default to {} which will otherwise
      // cause this to break.
      if(data instanceof JSONObject) {
        JSONObject obj = (JSONObject) data;
        if(obj.length() == 0) {
          return JSONArray.EMPTY_ARRAY;
        }
      }

      // Try to make as helpful a warning message as possible so user can debug the problem from logs
      String msg = "Could not cast to JSONArray:  [[" + data.getClass() + "]] => ";
      if(data instanceof JSONObject) {
        msg += "[[parent=" + ((JSONObject) data).getParent() + "]] ";
      }
      msg += data.toString();
      LOG.warn(msg);

      // Otherwise *force* an array here so we don't bring entire pipeline to a halt during normal ops for
      // an unanticipated error.  Retain current data in array to leave a trace of what originated here though!
      JSONArray retVal = new JSONArray();
      retVal.put(data);
      return retVal;
    }

    return (JSONArray) data;
  }

}
