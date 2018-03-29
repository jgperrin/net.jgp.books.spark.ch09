package net.jgp.books.sparkWithJava.ch09.x.utils;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.apache.spark.sql.types.DataType;

@Retention(RetentionPolicy.RUNTIME)
public @interface SparkColumn {

    String name() default "";

    Class<? extends DataType> type() default DataType.class;

    boolean nullable() default true;
}
