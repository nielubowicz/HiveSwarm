/**
 ** Licensed to the Apache Software Foundation (ASF) under one
 ** or more contributor license agreements.  See the NOTICE file
 ** distributed with this work for additional information
 ** regarding copyright ownership.  The ASF licenses this file
 ** to you under the Apache License, Version 2.0 (the
 ** "License"); you may not use this file except in compliance
 ** with the License.  You may obtain a copy of the License at
 **
 **     http://www.apache.org/licenses/LICENSE-2.0
 **
 ** Unless required by applicable law or agreed to in writing, software
 ** distributed under the License is distributed on an "AS IS" BASIS,
 ** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 ** See the License for the specific language governing permissions and
 ** limitations under the License.
 **/

package com.grooveshark.hive.udtf;

import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class BinaryFlagEvaluationUDTF extends GenericUDTF {
          IntWritable flagValue = new IntWritable(0);
          IntWritable sourceFlag = new IntWritable(0);
          IntWritable value = new IntWritable(0);
          PrimitiveObjectInspector valueOI = null;
          Object forwardObj[] = new Object[1];

          @Override
            public void close() throws HiveException {
            }

        @Override
              public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
                  if (argOIs.length != 1) {
                      throw new UDFArgumentLengthException("Exactly one primitive argument expected");
                  }

                  if (argOIs[0].getCategory() != Category.PRIMITIVE) {
                      throw new UDFArgumentTypeException(0,
                              "Primitive argument was expected but an argument of type " + argOIs[0].getTypeName() 
                              + " was given.");
                  }

                  valueOI=(PrimitiveObjectInspector)argOIs[0];

                  ArrayList<String> fieldNames = new ArrayList<String>();
                  ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
                  fieldNames.add("col1");
                  fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
                  return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
                }

          @Override
                public void process(Object[] args) throws HiveException {
                    // just return if null
                    if (args == null || args[0] == null) {
                        return;
                    }

                    value = (IntWritable)valueOI.getPrimitiveWritableObject(args[0]);
                    sourceFlag.set(value.get());

                    // for source flag 0, return 0
                    if (value.get() == 0) {
                        forwardObj[0]=sourceFlag;
                        this.forward(forwardObj);
                    }

                    // otherwise, run through the powers of 2 for each bit in the flag
                    // and output each matching flag
                    for (int i=0; i<= 16; i++) {
                        double flag=Math.pow(2,i);
                        flagValue.set((int)flag); 
                        if (flagValue.get() == (sourceFlag.get() & flagValue.get())) {
                            forwardObj[0]=flagValue;
                            this.forward(forwardObj);
                        }
                    }
                }

          @Override
            public String toString() {
                return "explode_flag";
            }
}
