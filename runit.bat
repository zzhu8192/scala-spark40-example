copy .\src\main\resources\test.csv c:\tmp /Y
mkdir c:\tmp\test1
pushd target
spark-submit  --class com.uj.example.Example example-1.0.0-SNAPSHOT-jar-with-dependencies.jar file:///tmp/test.csv /tmp/test1
popd
