package com.func.udaf;

import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class GenericUDAFMedian extends GenericUDAFAverage {

    public GenericUDAFMedian(){
        super();
    }

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        return super.getEvaluator(parameters);
    }

    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo) throws SemanticException{
        return super.getEvaluator(paramInfo);
    }

}
