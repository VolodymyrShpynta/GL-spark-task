package com.vshpynta.utils;

import lombok.experimental.UtilityClass;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An utility class for Spark SQL.
 */
@UtilityClass
public class SchemaUtils {

    /**
     * Creates the instance of Spark SQL {@link StructType} for specified fields names and types.
     * @param fieldNames - the collection of fields names.
     * @param fieldTypes - the collection of fields types.
     * @return - StructType of specified fields.
     */
    public static StructType createSchema(List<String> fieldNames, List<DataType> fieldTypes) {
        Assert.isTrue(fieldNames.size() == fieldTypes.size(), "Sizes should be equal");
        List<StructField> fields = Stream.iterate(0, id -> ++id)
                .limit(fieldNames.size())
                .map(id -> DataTypes.createStructField(fieldNames.get(id), fieldTypes.get(id), true))
                .collect(Collectors.toList());
        return DataTypes.createStructType(fields);
    }
}
