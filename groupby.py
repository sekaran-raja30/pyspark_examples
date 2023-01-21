from pyspark.sql.functions import col,sum,avg,max

data = [('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)]
rd=sc.parallelize(data,3)

columns = ['K', 'V']

dataframe = spark.createDataFrame(data, columns)
group = rd.groupByKey().toDF()
#reduce = rd.reduceByKey().toDF()
group1 = dataframe.groupBy('K').sum('V')
group.show()
group1.show()
#reduce.show()
#rd.map(x: (x,1)).reduceByKey(x,y: x+y)


simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.show()
df.printSchema()
df.groupBy("department").sum("salary").show(truncate=False)
df.groupBy("department").count().show(truncate=False)
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
      avg("salary").alias("avg_salary"), \
      sum("bonus").alias("sum_bonus"), \
      max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000) \
    .show(truncate=False)
