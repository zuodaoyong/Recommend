package com.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUDF {

    public static void format_yyyyMM(SparkSession sparkSession){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMM");
        // 注册UDF，将timestamp转化为年月格式yyyyMM
        sparkSession.udf().register("yyyyMM", new UDF1<Integer, String>() {
            @Override
            public String call(Integer o) throws Exception {
                return simpleDateFormat.format(new Date(o*1000L));
            }
        }, DataTypes.StringType);
    }
}
