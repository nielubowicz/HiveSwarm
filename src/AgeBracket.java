package com.grooveshark.hive.udf;

import java.lang.Exception;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)

@Description(name = "agebracket", 
	     value = "_FUNC_(age) - Returns the age bracket to which the given age belongs",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_(45) FROM src LIMIT 1;\n"
	     + "  45-54\n")
public class AgeBracket extends GenericUDF {

    private Text result = new Text();
  public AgeBracket() {
  }

  /**
   * Converters for retrieving the arguments to the UDF.
   */
  private ObjectInspectorConverters.Converter[] converters;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException("_FUNC_ expects exactly 1 argument");
    }

    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i].getCategory() != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i,
            "A integer argument was expected but an argument of type " + arguments[i].getTypeName()
                + " was given.");

      }

      // Now that we have made sure that the argument is of primitive type, we can get the primitive
      // category
      PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) arguments[i])
          .getPrimitiveCategory();

      if (primitiveCategory != PrimitiveCategory.INT
          && primitiveCategory != PrimitiveCategory.VOID) {
        throw new UDFArgumentTypeException(i,
            "An integer argument was expected but an argument of type " + arguments[i].getTypeName()
                + " was given.");

      }
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    // We will be returning a Text object
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 1);
    if (arguments[0].get() == null) {
      return null;
    }

    Integer input = Integer.valueOf(converters[0].convert(arguments[0].get()).toString());

    return processInput(input);
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "age_bracket(" + children[0] + ")";
  }
  /**
   * Get the day of week from a date string.
   * 
   * @param ageInt
   *          the integer value of the age to be bracketed
   * @return a string with the age bracket, '13-17' or '35-44', e.g. 'underage' if the age is less than 13, null if the age is invalid (< 0).
   */
  public Text processInput(Integer age) {
    if (age <= 0 || age==null) {
      return null;
    }
    Text ageString=new Text();
    try {
     if (age < 13) {
ageString.set("underage");
     } else if (age <= 17) {
         ageString.set("13-17");
     } else if (age <= 24) {
         ageString.set("18-24");
     } else if (age <= 34) {
         ageString.set("25-34");
     } else if (age <= 44) {
         ageString.set("35-44");
     } else if (age <= 54) {
         ageString.set("45-54");
     } else {
         ageString.set("55+");
     }
  } catch (Exception e) {
      ageString.set("There was an error");
  };
      return ageString;
  }
}
