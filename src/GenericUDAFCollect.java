package com.grooveshark.hive.udaf;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyLongObjectInspector;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * GenericUDAFCollect 
 *
 */
@Description(name = "collect", value = "_FUNC_(x) - Returns the collection containing all x for a given group")
public class GenericUDAFCollect extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(GenericUDAFCollect.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        
        //We need exactly one parameters
        if (parameters.length != 1) {
            throw new UDFArgumentLengthException("Collect requires 1 parameter");
        }

        GenericUDAFEvaluator evaluator;
        switch(parameters[0].getCategory()) {
    //        case LIST: 
    //            evaluator = new GenericUDAFCollectListEvaluator()
    //                break;
    //        case MAP: 
    //            evaluator = new GenericUDAFCollectMapEvaluator();
    //                break;
             case STRUCT: 
                 evaluator = new GenericUDAFCollectStructEvaluator();
                 break;
            default:
                    throw new UDFArgumentTypeException(0, "Given: " + parameters[0].getTypeName() 
                            + ": Collect cannot handle data type");
        }
        return evaluator;
    }

    public static class GenericUDAFCollectStructEvaluator extends GenericUDAFEvaluator {
            
        // input inspectors for PARTIAL1 and COMPLETE
        private StandardStructObjectInspector sOI;
        private static ArrayList<ObjectInspector> fois = new ArrayList<ObjectInspector>(); // for struct fields
        private static ArrayList<String> fnames = new ArrayList<String>(); // for struct fields
        
        // input inspectors for PARTIAL2 and FINAL
        private StandardListObjectInspector lloi;
        private static Object[] o;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        
            super.init(m, parameters);
            
            // initialize input inspectors
            // PARTIAL1 and COMPLETE both read from raw input data, so initialize inspectors for structs
            // while PARTIAL2 and FINAL both read from intermediate data, which in this case is a list
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                sOI = (StandardStructObjectInspector) parameters[0];
                List<StructField> listFields= (List<StructField>)sOI.getAllStructFieldRefs();
                for (StructField field: listFields) {
                    fnames.add(field.getFieldName());
                    fois.add(field.getFieldObjectInspector());
                }
                o = new Object[listFields.size()];
            } else {
                lloi = (StandardListObjectInspector) parameters[0];
                sOI = (StandardStructObjectInspector) lloi.getListElementObjectInspector();
            }

            // init output object inspectors - in both cases, its a list of structs
            // PARTIAL1 and PARTIAL2 both write intermediate data
            // while COMPLETE FINAL both write output 
            return ObjectInspectorFactory.getStandardListObjectInspector(sOI);
        }
        
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      // return an ArrayList where the first parameter is the window size
        return terminate(agg);
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      // final return value goes here
      StdAgg myagg = (StdAgg) agg;
      return myagg.collection;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
        // if we're merging two separate sets we're creating one table that's doubly long
        if (partial != null)  {
            StdAgg myagg = (StdAgg) agg;
            List<?> lis = new ArrayList((List<?>)lloi.getList(partial));
            for (Object o: lis) {
                myagg.collection.add(o);
            }

        }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        assert (parameters.length == 1);
        if (parameters[0] == null) {
            return;
        }
        
        try {
            StdAgg myagg = (StdAgg) agg;
            List<?> data = sOI.getStructFieldsDataAsList(parameters[0]);
            o = data.toArray();
            myagg.collection.add(o);
        } 
        catch (RuntimeException e) {
            LOG.warn(e.getLocalizedMessage() + "\nStack Trace: " + (e.getStackTrace()).toString());
        }
    }

    // Aggregation buffer definition and manipulation methods 
    static class StdAgg implements AggregationBuffer {
        ArrayList collection;
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      StdAgg result = new StdAgg();
      result.collection = new ArrayList();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
        StdAgg myagg = (StdAgg) agg;
        myagg.collection.clear();
    }
    }
}
