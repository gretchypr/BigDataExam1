#Code here

schooldf = spark.read.csv('/input/escuelasPR.csv')

schooldf.createOrReplaceTempView("school")

schools = spark.sql("select _c1 as District, _c2 as City, count(*) from school where _c0 == 'Arecibo' group by District, City ")
