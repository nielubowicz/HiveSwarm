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

@Description(name = "lastdateofmonth", 
	     value = "_FUNC_(date) - Returns the last date in week given by date. Uses Java Calendar getActualMaximum to determine the last day" 
         + " in the given month",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('2011-08-30') FROM src LIMIT 1;\n"
	     + "  '2011-08-31'\n") 
public class LastDateOfMonth extends UDF {
  private final static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private final static Calendar calendar = Calendar.getInstance();

  private final Text result = new Text();
  private Date date = null;

  public LastDateOfMonth() {
  }

  /**
   * Get the last date of week (Sunday) from a date string.
   * 
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd"
   * @return a datestring for the last day of hte month . null if the dateString is not a valid date
   *         string.
   */
  public Text evaluate(Text dateString) {
    if (dateString == null) {
      return null;
    }
    try {
      date = formatter.parse(dateString.toString());
      calendar.setTime(date);
      calendar.set(java.util.Calendar.DAY_OF_MONTH, calendar.getActualMaximum(java.util.Calendar.DAY_OF_MONTH));
      result.set(formatter.format(calendar.getTime()));
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

}

