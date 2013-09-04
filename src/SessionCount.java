package com.grooveshark.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

@UDFType(deterministic = false, stateful = true)

public class SessionCount extends GenericUDF {
  private final static IntWritable counter = new IntWritable();
  private final static LongWritable ts = new LongWritable();
  private final static LongWritable lastTS = new LongWritable();

  private Object[] previousKey;
  private ObjectInspector[] ois;

  @Override
  public Object evaluate(DeferredObject[] currentKey) throws HiveException {
    if (currentKey == null) {
        return 0;
    }

    if (currentKey[currentKey.length - 1].get() == null) {
        return 0;
    }

    ts.set(((LongWritable)(currentKey[currentKey.length - 1]).get()).get()); // last object should be timestamp
    if (!sameAsPreviousKey(currentKey)) {
        lastTS.set(0);
        counter.set(0);
        copyToPreviousKey(currentKey);
    }

    if (ts.get() - lastTS.get() > 30 * 60) {
        counter.set(counter.get() + 1);
    }

    lastTS.set(ts.get());
    return counter;
  }

  @Override
  public String getDisplayString(String[] currentKey) {
    return "SessionCount-Udf-Display-String";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
    ois=arg0;
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  /**
   * This will help us copy objects from currrentKey to previousKeyHolder.
   *
   * @param currentKey
   * @throws HiveException
   */
  private void copyToPreviousKey(DeferredObject[] currentKey) throws HiveException {
    if (currentKey != null) {
        if (previousKey == null) {
            previousKey = new Object[currentKey.length - 1];
        }
        for (int index = 0; index < currentKey.length - 1; index++) {   
            previousKey[index]= ObjectInspectorUtils
                    .copyToStandardObject(currentKey[index].get(),this.ois[index]);

        }
    }   
  }

  /**
   * This will help us compare the currentKey and previousKey objects.
   *
   * @param currentKey
   * @return - true if both are same else false
   * @throws HiveException
   */
  private boolean sameAsPreviousKey(DeferredObject[] currentKey) throws HiveException {
    boolean status = false;

    //if both are null then we can classify as same
    if (currentKey == null && previousKey == null) {
      status = true;
    }

    //if both are not null and there legnth as well as
    //individual elements are same then we can classify as same
    if (currentKey != null && previousKey != null && currentKey.length - 1  == previousKey.length) {
      for (int index = 0; index < currentKey.length - 1; index++) {

        if (ObjectInspectorUtils.compare(currentKey[index].get(), this.ois[index],
                previousKey[index],
                ObjectInspectorFactory.getReflectionObjectInspector(previousKey[index].getClass(), ObjectInspectorOptions.JAVA)) != 0) {

          return false;
        }

      }
      status = true;
    }
    return status;
  }
}
