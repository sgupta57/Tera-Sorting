val start_time = System.currentTimeMillis()
val sortFile = sc.textFile("hdfs://localhost:9000/input/inputdata")

val sortedobj = sortFile.flatMap(line=>line.split("\n")).map(word=>(word.substring(0,10),word.substring(10))).sortByKey()

sortedobj.saveAsTextFile("hdfs://localhost:9000/output/output")
val end_time = System.currentTimeMillis()
println ("Total time taken :" + (end_time - start_time) + "ms")
println ("Total time in seconds:" + (end_time - start_time)/1000 + "sec")