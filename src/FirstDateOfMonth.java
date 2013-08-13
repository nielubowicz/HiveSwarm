package com.grooveshark.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)

@Description(name = "firstdateofmonth", 
	     value = "_FUNC_(date) - Returns the first date in week given by date.",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('2011-08-30') FROM src LIMIT 1;\n"
	     + "  '2011-08-01'\n") 
public class FirstDateOfMonth extends UDF {
  private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private final Calendar calendar = Calendar.getInstance();

  private final Text result = new Text();

  public FirstDateOfMonth() {
  }

  /**
   * Get the first date of week (Monday) from a date string.
   * 
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd"
   * @return a datestring for the first day of hte month . null if the dateString is not a valid date
   *         string.
   */
  public Text evaluate(Text dateString) {
    if (dateString == null) {
      return null;
    }
    try {
      Date date = formatter.parse(dateString.toString());
      calendar.setTime(date);
      calendar.set(java.util.Calendar.DAY_OF_MONTH, 1);
      result.set(formatter.format(calendar.getTime()));
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

}
