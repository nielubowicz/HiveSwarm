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

@Description(name = "firstdateofweek", 
	     value = "_FUNC_(date) - Returns the first date in week given by date.",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('2011-08-30') FROM src LIMIT 1;\n"
	     + "  '2011-08-29'\n") // where 8/29 is a Monday
public class FirstDateOfWeek extends UDF {
  private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private final Calendar calendar = Calendar.getInstance();

  private final Text result = new Text();

  public FirstDateOfWeek() {
  }

  /**
   * Get the first date of week (Monday) from a date string.
   * 
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd"
   * @return an int from 1 to 7. null if the dateString is not a valid date
   *         string.
   */
  public Text evaluate(Text dateString) {
    if (dateString == null) {
      return null;
    }
    try {
      Date date = formatter.parse(dateString.toString());
      calendar.setTime(date);
      if (calendar.get(java.util.Calendar.DAY_OF_WEEK) >= java.util.Calendar.MONDAY) {
          calendar.set(java.util.Calendar.DAY_OF_WEEK, java.util.Calendar.MONDAY); // roll back day of week to monday
      } else {
          calendar.add(java.util.Calendar.WEEK_OF_YEAR, -1);
          calendar.set(java.util.Calendar.DAY_OF_WEEK, java.util.Calendar.MONDAY); // roll back day of week to monday
      }
      result.set(formatter.format(calendar.getTime()));
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

}
