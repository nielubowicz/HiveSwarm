package com.grooveshark.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(name = "sourceidmap", 
	     value = "_FUNC_(sourceflag) - Returns 'interactive','non-interactive' or 'mixed-interactive' depending on the sourceflag",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_(sourceflag) FROM streams LIMIT 1;\n"
	     + "  'interactive'\n")

public class SourceFlagMap extends UDF {

  private final static Text noninteractive = new Text("non-interactive");
  private final static Text interactive = new Text("interactive");
  private final static Text mixedinteractive = new Text("mixed-interactive");
  
  public Text evaluate(int SourceFlag) {
    if (
         (SourceFlag & 1) == 1 || 
         (SourceFlag & 2) == 2 ||
         (SourceFlag & 4) == 4 || 
         (SourceFlag & 256) == 256 ||
         (SourceFlag & 512) == 512 ) 
    { 
      return noninteractive;  
    }
    else if (
         (SourceFlag & 64) == 64 ||
         (SourceFlag & 128) == 128 ||
         (SourceFlag & 1024) == 1024 ||
         (SourceFlag & 2048) == 2048 ||
         (SourceFlag & 4096) == 4096 ||
         (SourceFlag & 16384) == 16384 )
    { 
       return mixedinteractive; 
    }
    return interactive;
  }
}
