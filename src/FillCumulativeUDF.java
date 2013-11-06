/**
 **/

package com.grooveshark.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;


@Description(name = "fill",
            value = "_FUNC_(hash(p1,p2), value) " +
                        "- Returns the value, or if value == 0, the last value, for a column -- by hash(p1,p2...)")
      
@UDFType(deterministic = false, stateful = true)
public class FillCumulativeUDF extends GenericUDF {
    private PrimitiveObjectInspector resultOI;
    private Object lastValue = null, value = null;
    private WritableIntObjectInspector hashOI,prevHashOI;
    private static final IntWritable prevHash = new IntWritable(0);
    private static final IntWritable hash = new IntWritable(0);
    private static final IntWritable zero = new IntWritable(0);


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

        hashOI = (WritableIntObjectInspector) arguments[0];
        prevHashOI=PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        resultOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJavaClass(PrimitiveObjectInspectorUtils.getJavaPrimitiveClassFromObjectInspector(arguments[1])).primitiveCategory);
        return resultOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        hash.set(hashOI.get(arguments[0].get()));
        value = arguments[1].get();

        if (prevHash==null || hash.equals(prevHash) == false) {
            lastValue=resultOI.copyObject(zero);
        }

        prevHash.set(hash.get());

        if (value == null || Integer.valueOf(value.toString()) == 0) {
            value = lastValue; 
        } else {
            lastValue = resultOI.copyObject(value);
        }

        return value;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "fill(" + StringUtils.join(children, ',') + ")";
    }
}

