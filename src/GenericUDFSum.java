/**
 ** Licensed to the Apache Software Foundation (ASF) under one
 ** or more contributor license agreements.  See the NOTICE file
 ** distributed with this work for additional information
 ** regarding copyright ownership.  The ASF licenses this file
 ** to you under the Apache License, Version 2.0 (the
 ** "License"); you may not use this file except in compliance
 ** with the License.  You may obtain a copy of the License at
 **
 **      <http://www.apache.org/licenses/LICENSE-2.0>
 *      http://www.apache.org/licenses/LICENSE-2.0
 **
 ** Unless required by applicable law or agreed to in writing, software
 ** distributed under the License is distributed on an "AS IS" BASIS,
 ** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 ** See the License for the specific language governing permissions and
 ** limitations under the License.
 **/

package com.hiveswarm.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;


@Description(name = "sum",
            value = "_FUNC_(hash_key, order_by_col1, order_by_col2 ...) " +
                        "- Returns the summed value of group",
            extended = "Example:\n"
                        + "  > SELECT _FUNC_(HASH(p1, p2), order_by_col1, order_by_col2, ... ) FROM (\n"
                        + "  >         SELECT ~ FROM table DISTRIBUTE BY HASH(p1,p2) SORT BY  p1,p2,order_by_col1, order_by_col2 DESC, ... \n"
                        + "  > );")
      
@UDFType(deterministic = false, stateful = true)
public class GenericUDFSum extends GenericUDF {
    private final LongWritable longResult = new LongWritable();
    private final DoubleWritable doubleResult = new DoubleWritable();
    private ObjectInspector hashOI, valueOI, prevHashStandardOI, resultOI;
    private Object prevHash;
    @Override
        public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
            if (arguments.length != 2) {
                throw new UDFArgumentException("Exactly two argument is expected.");
            }

            for(int i=0;i<arguments.length;i++){
                if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                    throw new UDFArgumentTypeException(i,
                                    "Only primitive type arguments are accepted but "
                                    + arguments[i].getTypeName() + " is passed.");
                }
            }

            String t = arguments[1].getTypeName();
            if (t.equals(Constants.TINYINT_TYPE_NAME)||
                t.equals(Constants.SMALLINT_TYPE_NAME)||
                t.equals(Constants.INT_TYPE_NAME)||
                t.equals(Constants.BIGINT_TYPE_NAME)) {
                    resultOI =
                            PrimitiveObjectInspectorFactory.writableLongObjectInspector;
                } else if (t.equals(Constants.FLOAT_TYPE_NAME)||
                           t.equals(Constants.DOUBLE_TYPE_NAME)||
                           t.equals(Constants.STRING_TYPE_NAME)) {
                   resultOI =
                            PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
                } else{ 
                    throw new UDFArgumentTypeException(1,
                                                        "Only numeric or string type arguments are accepted but"
                                                        + arguments[1].getTypeName() + " is passed.");
                }

                longResult.set(0);
                doubleResult.set(0);
                hashOI = arguments[0];
                valueOI = arguments[1];
                                         
                prevHashStandardOI=ObjectInspectorUtils.getStandardObjectInspector(hashOI,ObjectInspectorCopyOption.JAVA);
                return resultOI;
            }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object hash = arguments[0].get();
        Object value = arguments[1].get();
        if (prevHash==null||ObjectInspectorUtils.compare(prevHash,prevHashStandardOI,hash,hashOI)!=0) {
            longResult.set(0);
            doubleResult.set(0);
        }

        prevHash=ObjectInspectorUtils.copyToStandardObject(hash,hashOI,ObjectInspectorCopyOption.JAVA);
        Converter converter = ObjectInspectorConverters.getConverter(valueOI, resultOI);
        if(resultOI.getTypeName()==Constants.DOUBLE_TYPE_NAME){
            DoubleWritable valueW =
                        (DoubleWritable)converter.convert(value);
            doubleResult.set(doubleResult.get()+valueW.get());
            return doubleResult;
        }
        LongWritable valueW = (LongWritable)converter.convert(value);
        longResult.set(longResult.get()+valueW.get());
        return longResult;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "sum(" + StringUtils.join(children, ',') + ")";
    }
}
