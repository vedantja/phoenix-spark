# phoenix-spark

An Apache Phoenix RDD for Apache Spark.

This RDD is intended to be an easier-to-use wrapper for Apache Phoenix within Apache Spark. It
includes an automatic conversion to a SchemaRDD for use within Spark SQL.

This is at the earliest stage of development. It will change frequently.

## Requirements

Development is done against the following:

* Java 8. It's been reported that you will experience OutOfMemoryErrors during the tests using Java 7.
* Apache Spark 1.1.0
* Hortonworks Data Platform 2.2 Preview 
  * Apache HBase 0.98.4
  * Apache Phoenix 4.3.0-SNAPSHOT (Built from the latest git sources)

## Usage

Given an existing SparkContext, you can connect to Phoenix and run basic RDD queries:

```scala
val rdd = PhoenixRDD.NewPhoenixRDD(sparkContext, 
  "sandbox.hortonworks.com:2181:/hbase-unsecure", // Insert your HBase connection string
  "MyTable",                                      // Your Phoenix table name. This is case-sensitive.   
  Array("Foo", "Bar"),                            // Columns to use. Case-sensitive.
  conf = new Configuration())                     // A Hadoop Configuration object. Phoenix config
                                                  // will be injected.

// get a count of matched rows

val count = rdd.count()
```

You can also link into Spark SQL, using the RDD above:

```scala
val sqlContext = new SQLContext(sc)

val schemaRDD = rdd.toSchemaRDD(sqlContext)

schemaRDD.registerTempTable("my_table")

val sqlRdd = sqlContext.sql("SELECT * FROM my_table")

val count = sqlRdd.count()
```

## TODO

* Automatically discover all columns if columns are not specified or null / Nil
* Fluent builder for making the underlying SQL query
* Predicate Pushdown (or, predicates of any sort at the Phoenix layer)

## License

Licensed under Apache Public License 2.0. See LICENSE.
