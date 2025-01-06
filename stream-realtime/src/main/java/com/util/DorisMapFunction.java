package com.util;

/**
 * @Package com.util.DorisMapFunction
 * @Author hou.dz
 * @Date 2025/1/3 9:08
 * @description:
 */
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.FieldNamingPolicy;
import org.apache.flink.api.common.functions.MapFunction;


public class DorisMapFunction<T> implements MapFunction<T, String> {
    private Gson gson;

    public DorisMapFunction() {
        this.gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
    }

    @Override
    public String map(T t) throws Exception {
        return gson.toJson(t);
    }
}


