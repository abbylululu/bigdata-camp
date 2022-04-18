# bigdata-camp
### week6-7 spark hw
#### 知识点:
1. `org.apache.hadoop.fs.FileSystem .listStatus(Path)`:
list all files & dirs of the param path
   
2. `sparkContext.makeRDD(seq: Seq[T], numSlices: Int = defaultParallelism)`:
numSlices: number of partitions to divide the seq into

3. `org.apache.hadoop.fs.FileUtil .copy(FileSystem srcFS, Path src, FileSystem dstFS, Path dst, boolean deleteSource, Configuration conf)`

#### 遇到的问题：
1. `java: package sun.misc does not exist` \
Using java11 with language level 8 implies cross-compilation, and the ide's only option for cross-compilation is the new "--release" javac option, 
   In this case the code will be resolved against special symbol files containing corresponding API signatures. "Closed"  sun.misc packages are not included into these symbol files, so javac cannot resolve such APIs.
   
2. `Spark Scala Error: java.lang.NoSuchMethodError: scala.collection.mutable.Buffer$.empty()Lscala/collection/GenTraversable` \
scala版本与spark版本不兼容，scala版本不向后兼容，spark 3.x用的是scala 2.12.x