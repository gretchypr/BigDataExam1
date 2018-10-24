#Code here

schooldf = spark.read.csv('/input/escuelasPR.csv')

studentsdf = spark.read.csv('/input/studentsPR.csv')

maleStudents = studentsdf.join(schooldf, (schooldf._c3 == studentsdf._c2)).filter(schooldf._c5 == 'Superior').filter((schooldf._c2 == 'Ponce') | (schooldf._c2 == 'San Juan')).filter(studentsdf._c5 == 'M').select(studentsdf._c6)

maleStudents.show()
