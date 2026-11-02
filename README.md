## spark-job-with-testing-example

Spark example project, with unit testing (Spark 4.x / Scala 2.13 / Java 21)

### Table of Contents
- [System Requirements](#system-requirements)
- [Installation](#installation)
- [How to Run/Debug this project](#run-debug)

### System Requirements
In general, Spark is going to be both CPU and memory bound.  This is both true for running the job via spark-submit, or running Scala unit tests.   Also, the Scala compiler is a beast in terms of CPU and memory hog.  The Intellij IDEA is also memory and CPU intensive.  As such, the more cores and more memory you have, the better off your development experience will be:
- Ideal:  **32 GB RAM,  8+ cores / 16T SSD**
- Medium: **16 GB RAM,  4 cores / 8T, SSD**
- Masochist:   8 GB RAM, 4 cores  <- this is highly unlikely to run or compile well

 You need the following tools (IDE), runtimes, and or libraries. 
- Some distribution of Java 21 SDK runtime.  
- Intellij IDEA Community Edition (You do **NOT** need Enterprise edition)
- Spark 3.3.1 binary distribution (compiled for Hadoop 3.3 and with Scala 2.13)
- Hadoop Winutils binary for Spark 3.3.1 (windows only)
- *Optional*   Zeppelin 0.12.1 or Jupyter (for interactive testing of Spark using Scala)

### Installation
Follow instructions on this page (but please see Java 8 SDK installation instructions below first, or you may be violating Oracle licensing agreements)

1. Install Java 21 SDK - I recommend using the zip packages in the Java 21 (LTS) section (use apt on Linux, and download from Zulu Systems for Windows or MacOS
1. add SYSTEM ENV variable **JAVA_HOME=c:\jdk21**  (or full path to where you installed/unzipped this)
1. add **c:\jdk21\bin**  to your PATH
1. To test your installation, open new cmd prompt,
    run: **java -version**   
    You should see JVM runtime information

### Spark 4.2.1
1.  Goto **https://spark.apache.org/downloads.html**
1.  Download **spark-4.2.1-bin-hadoop3.4.tgz**   
1.  untar (using cmdline tar xvf) somewhere on your drive.  It will create a folder called: **spark-4.2.1-bin-hadoop3.4\**
1.  you can rename this folder to be something shorter if you wish
1.  add SYSTEM ENV variable **SPARK_HOME=** directory where you put this
1.  add SYSTEM ENV variable **HADOOP_HOME=** directory where you put this
1.  add **(base folder)\spark-4.2.1-bin-hadoop3.4\bin** to your PATH
1.  To test your installation, open new cmd prompt, 
1.  modify your conf/spark-defaults.conf to include **spark.driver.memory 12g**
   run: **spark-shell**   
   You should get a spark shell interpreter running

### If on Windows, you'll also need Hadoop Winutils binaries
1.  I recommend using files from here: https://github.com/cdarlint/winutils 
1.  unzip to c:\hadoop (or folder of your choice)
1.  copy the winutils.exe from this bin folder to your **spark\bin**  directory



### Build

Either use Intellij IDEA with scala support (import maven project)

OR

install maven 3.9.9+  (download zip from: https://maven.apache.org/ref/3.9.9/download.cgi , put the resulting maven/bin/ folder in PATH so mvn will be executable in cmdline)


### Build and run on cmdline

mvn package

This will do a mvn package which:
1. compiles scala source code
2. runs unit tests
3. builds jar including 3rd party dependencies in target/
4. clean out output directory, and


### Debug

Use Intellij IDEA's Debugger
Make sure to check the option in (Run Configurations):

**"Include Dependencies with Provided Scope"**

The maven build pom.xml has all Scala and Spark dependencies declared as "provided", which makes resulting binary super lean and small, but this means when you debug, you have to tell the IDE to included it from elsewhere.




### More Resources
- https://spark.apache.org/docs/latest/quick-start.html
- https://databricks.com/spark/about
