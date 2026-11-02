# Scala Spark 4.x Example Project

[![CI](https://github.com/zzhu8192/scala-spark40-example/actions/workflows/ci.yml/badge.svg)](https://github.com/zzhu8192/scala-spark40-example/actions/workflows/ci.yml)
[![Java 21](https://img.shields.io/badge/Java-21%20LTS-orange.svg)](https://adoptium.net/)
[![Scala 2.13](https://img.shields.io/badge/Scala-2.13.18-red.svg)](https://www.scala-lang.org/)
[![Apache Spark 4.2.1](https://img.shields.io/badge/Apache%20Spark-4.2.1-blue.svg)](https://spark.apache.org/)
[![Delta Lake 4.2.0](https://img.shields.io/badge/Delta%20Lake-4.2.0-00ADD8.svg)](https://delta.io/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

A production-ready Maven skeleton template for **Apache Spark 4.x** applications written in **Scala 2.13** running on **Java 21 (LTS)**. Includes pre-configured unit testing utilities, Delta Lake support, Hive metastore integration, sample configuration templates, lean Fat-JAR assembly packaging, and ready-to-run execution scripts.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [System Requirements](#system-requirements)
- [Prerequisites & Installation](#prerequisites--installation)
- [Delta Lake Integration Guide](#delta-lake-integration-guide)
- [Hive Metastore with Remote Derby Setup](#hive-metastore-with-remote-derby-setup)
- [Quick Start](#quick-start)
- [Running the Application](#running-the-application)
- [Running & Debugging in IntelliJ IDEA](#running--debugging-in-intellij-idea)
- [Troubleshooting & Common Errors](#troubleshooting--common-errors)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

Configuring Apache Spark 4.x with Scala 2.13, Java 21, and Delta Lake in Maven often runs into thorny setup issues:
- Java 21 strong module encapsulation (`--add-opens` requirements for Spark internal reflection).
- Maven assembly fat-JAR bloat when Spark / Scala runtime jars are bundled inadvertently instead of marked `provided`.
- IDE runtime classpath issues (`ClassNotFoundException`) when dependencies are marked `provided`.
- Delta Lake dependency wiring and catalog extensions.
- Spark unit test isolation, Derby metastore lock contention (`metastore_db`), and log verbosity.

This repository serves as a **battle-tested baseline skeleton** that solves these challenges out of the box so you can start writing Spark jobs and unit tests immediately.

---

## Key Features

- **Apache Spark 4.x & Delta Lake 4.x**: Tested with Spark 4.2.1 and Delta Lake 4.2.0.
- **Java 21 (LTS) & Scala 2.13**: Built for modern LTS Java runtimes with Java 17 bytecode compatibility flags.
- **Sample Spark Configurations**: Ready-to-use template files in [`conf/`](conf/):
  - [`conf/spark-defaults.conf.template`](conf/spark-defaults.conf.template): Pre-configured Delta catalog, SQL extensions, and adaptive query execution.
  - [`conf/core-site.xml.template`](conf/core-site.xml.template): Remote Derby Network Server connection for concurrent Hive metastore access.
- **Optimized Unit Testing (`TestSparkSession`)**:
  - Reusable test suite trait using `ScalaTest` (FlatSpec) and `JUnit 4`.
  - Selective log suppression (reduces noisy Spark/Hadoop/Hive logs during test execution).
  - Synchronized test execution to prevent local Hive/Derby metastore lock collisions.
  - In-memory DataFrame creation helpers (`mkDataFrame`).
- **Lean Assembly Fat-JAR**:
  - Configured with `maven-assembly-plugin` to package third-party dependencies while excluding Spark and Scala runtime libraries (`provided` scope).
- **Sample End-to-End ETL**:
  - CSV ingestion with customizable read options.
  - Spark SQL and DataFrame API transformations.
  - User Defined Functions (UDF) and Dataset `map` examples.
  - Parquet write with Gzip compression and Hive external table registration.
- **Cross-Platform Scripts**: Includes `./run.sh` for Linux/macOS and `run.bat` for Windows.

---

## Tech Stack

| Component | Version | Notes |
|:---|:---|:---|
| **Java** | 21 (LTS) | Compatible with OpenJDK / Temurin / Zulu / Corretto |
| **Scala** | 2.13.18 | Modern Scala 2.13 series |
| **Apache Spark** | 4.2.1 | `spark-core`, `spark-sql`, `spark-hive` |
| **Delta Lake** | 4.2.0 | `delta-spark` |
| **Apache Derby** | 10.16.1.1 | `derbyclient` (network client for remote metastore) |
| **Apache Maven** | 3.9.9+ | Build and dependency orchestration |
| **ScalaTest** | 3.2.19 | Unit testing framework |
| **JUnit** | 4.13.2 | Test runner integration (`JUnitRunner`) |
| **JaCoCo** | 0.8.12 | Code coverage metrics |

---

## System Requirements

Running Spark jobs locally and compiling Scala requires adequate memory and CPU:

| Tier | RAM | CPU Cores | Storage | Experience |
|:---|:---|:---|:---|:---|
| **Recommended** | 16 GB - 32 GB | 8+ cores / 16 threads | SSD | Fast compilation, smooth IDE debugging, snappy tests |
| **Minimum** | 8 GB - 16 GB | 4 cores / 8 threads | SSD | Functional, may feel slower during full builds |
| **Not Recommended** | < 8 GB | 2 cores | HDD | High risk of compilation timeouts and JVM out-of-memory errors |

---

## Prerequisites & Installation

### 1. Java 21 (LTS)
Install an OpenJDK 21 distribution, such as [Eclipse Temurin](https://adoptium.net/) or [Azul Zulu](https://www.azul.com/downloads/):
- **Linux (Ubuntu/Debian)**:
  ```bash
  sudo apt update && sudo apt install -y openjdk-21-jdk
  ```
- **macOS (Homebrew)**:
  ```bash
  brew install openjdk@21
  ```
- **Windows**: Download the `.msi` or `.zip` installer from Adoptium. Set your environment variables:
  - `JAVA_HOME` pointing to your JDK 21 root folder (e.g., `C:\Program Files\Eclipse Adoptium\jdk-21...`).
  - Add `%JAVA_HOME%\bin` (Windows) or `$JAVA_HOME/bin` (Linux/macOS) to `PATH`.

Verify:
```bash
java -version
```

### 2. Apache Maven 3.9+
Download and install Maven 3.9.9 or later from [maven.apache.org](https://maven.apache.org/download.cgi).
Verify:
```bash
mvn -version
```

### 3. Apache Spark 4.x (Optional, for CLI `spark-submit`)
If you want to run jobs via the `spark-submit` command-line tool:
1. Download `spark-4.2.1-bin-hadoop3.tgz` from the [Apache Spark Downloads page](https://spark.apache.org/downloads.html).
2. Extract the archive to a directory of your choice.
3. Set environment variables:
   - `SPARK_HOME=/path/to/spark-4.2.1-bin-hadoop3`
   - Add `$SPARK_HOME/bin` (or `%SPARK_HOME%\bin`) to `PATH`.

### 4. Windows Hadoop Winutils (Windows Only)
On Windows, Hadoop native file operations require `winutils.exe` and `hadoop.dll`:
1. Download Hadoop 3.3/3.4 compatible binaries from [cdarlint/winutils](https://github.com/cdarlint/winutils).
2. Place them in `C:\hadoop\bin`.
3. Set environment variable `HADOOP_HOME=C:\hadoop` and add `C:\hadoop\bin` to `PATH`.
4. Copy `winutils.exe` and `hadoop.dll` into `%SPARK_HOME%\bin` as well.

---

## Delta Lake Integration Guide

Integrating Delta Lake with Apache Spark often confuses new users because **two distinct requirements** must both be satisfied:
1. **Runtime Jars on the Classpath**: Delta Lake dependencies are marked `<scope>provided</scope>` in [`pom.xml`](pom.xml) so the assembly Fat-JAR stays small. However, standard Spark distributions **do not bundle Delta Lake by default**.
2. **Spark SQL Configuration**: Spark needs to be explicitly told to use Delta's parser extension and catalog; otherwise, commands like `USING delta`, `MERGE INTO`, `VACUUM`, and `RESTORE` will fail to parse.

### Step 1: Supplying Delta Jars

Choose **one** of the following methods:

- **Method A: Copy JARs to `$SPARK_HOME/jars` (Recommended for Local Dev)**:
  Download the matching Delta Lake JARs from Maven Central and drop them directly into your local `$SPARK_HOME/jars/` directory:
  - `delta-spark_2.13-4.2.0.jar`
  - `delta-storage-4.2.0.jar`
  Once copied, any `spark-submit` or `spark-shell` execution automatically has Delta on its classpath.

- **Method B: Dynamic Resolution via `--packages`**:
  Pass the Delta Maven coordinates when invoking `spark-submit`:
  ```bash
  spark-submit \
    --packages io.delta:delta-spark_2.13:4.2.0 \
    --class com.uj.example.Example \
    target/example-1.0.0-SNAPSHOT-jar-with-dependencies.jar
  ```

### Step 2: Configuring Spark for Delta

Set the required Delta catalog and SQL extension properties using **any** of the following approaches:

- **Approach 1: In `conf/spark-defaults.conf` (System-wide)**:
  Add the following lines to `$SPARK_HOME/conf/spark-defaults.conf` (see [`conf/spark-defaults.conf.template`](conf/spark-defaults.conf.template)):
  ```properties
  spark.sql.extensions               io.delta.sql.DeltaSparkSessionExtension
  spark.sql.catalog.spark_catalog    org.apache.spark.sql.delta.catalog.DeltaCatalog
  ```

- **Approach 2: In Scala Application Code (`SparkSession.builder`)**:
  ```scala
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder()
    .appName("DeltaApplication")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  ```

- **Approach 3: Via CLI Arguments**:
  ```bash
  spark-submit \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --class com.uj.example.Example \
    target/example-1.0.0-SNAPSHOT-jar-with-dependencies.jar
  ```

---

## Hive Metastore with Remote Derby Setup

### Why Use a Remote Derby Network Server?

By default, Spark uses an **embedded Apache Derby database** for its local Hive metastore. Embedded Derby writes to a local directory `./metastore_db/` and places an exclusive lock (`db.lck`) on the files.

**The Problem**:
Only **one JVM process** can hold the embedded Derby lock at a time. If you run a Spark job, a `spark-shell`, or unit tests while another Spark process is running, the second process will fail immediately:
```
ERROR XSDB6: Another instance of Derby may have already booted the database /.../metastore_db
```

**The Solution**:
Run Apache Derby as a **locally hosted Network Server** (remoting enabled via DRDA on port `1527`). All Spark processes connect over TCP without acquiring local file locks, allowing **multiple concurrent Spark sessions, CLI shells, and unit test suites** to share the same local Hive catalog safely.

### Required Dependencies: `derbyclient.jar`

- Embedded Derby uses `derby.jar` (`org.apache.derby.jdbc.EmbeddedDriver`), which ships with standard Spark distributions.
- Remote Derby requires `org.apache.derby.jdbc.ClientDriver`, which is packaged in **`derbyclient.jar`**.
- Spark's default distribution **does not include `derbyclient.jar`**.

To resolve this dependency:
1. **In Maven**: This project already includes `org.apache.derby:derbyclient:10.16.1.1` (`provided` scope) in [`pom.xml`](pom.xml).
2. **In `$SPARK_HOME/jars`**: Download `derbyclient-10.16.1.1.jar` from Maven Central and copy it into `$SPARK_HOME/jars/`:
   ```bash
   curl -o "$SPARK_HOME/jars/derbyclient-10.16.1.1.jar" \
     https://repo1.maven.org/maven2/org/apache/derby/derbyclient/10.16.1.1/derbyclient-10.16.1.1.jar
   ```

### Configuration Files

Sample templates are provided in the [`conf/`](conf/) directory:

#### 1. [`conf/core-site.xml.template`](conf/core-site.xml.template)
Copy this file to `$SPARK_HOME/conf/core-site.xml`:
```xml
<configuration>
    <!-- Connect to local Derby Network Server over TCP on port 1527 -->
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:derby://127.0.0.1:1527/metastore;create=true</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.apache.derby.jdbc.ClientDriver</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>APP</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>mine</value>
    </property>
</configuration>
```

#### 2. [`conf/spark-defaults.conf.template`](conf/spark-defaults.conf.template)
Copy this file to `$SPARK_HOME/conf/spark-defaults.conf`:
```properties
spark.sql.catalogImplementation  hive
spark.sql.warehouse.dir          /tmp/hive-warehouse
```

### Starting the Derby Network Server

Start the Derby server before launching Spark applications:

- **Linux / macOS**:
  ```bash
  # Using Apache Derby binary:
  $DERBY_HOME/bin/startNetworkServer -h 127.0.0.1 -p 1527
  
  # Or directly via Java:
  java -classpath "/usr/local/derby/lib/*" org.apache.derby.drda.NetworkServerControl start -h 127.0.0.1 -p 1527
  ```
- **Windows**:
  ```cmd
  %DERBY_HOME%\bin\startNetworkServer.bat -h 127.0.0.1 -p 1527
  ```
- **Verify**: Check that port 1527 is listening:
  ```bash
  ss -tlpn | grep 1527
  ```

Once running, any Spark job or `spark-shell` configured with `core-site.xml` will connect to the remote metastore concurrently without conflicts.

---

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/zzhu8192/scala-spark40-example.git
cd scala-spark40-example
```

### 2. Run Unit Tests
```bash
mvn clean test
```
This compiles the Scala source code, configures the required Java 21 `--add-opens` flags via Surefire, boots the in-memory Spark test session, and runs the test suite.

### 3. Package the Assembly JAR
```bash
mvn clean package -DskipTests
```
The output JAR is generated at:
```
target/example-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

---

## Running the Application

### Using the Included Helper Scripts

#### Linux / macOS
```bash
./run.sh
```

#### Windows
```cmd
run.bat
```

The script verifies that `target/example-1.0.0-SNAPSHOT-jar-with-dependencies.jar` exists, checks for `spark-submit` on `PATH`, and executes the job with the bundled `src/main/resources/test.csv` input.

### Using `spark-submit` Directly

```bash
spark-submit \
  --class com.uj.example.Example \
  --master "local[*]" \
  target/example-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  file://$(pwd)/src/main/resources/test.csv \
  /tmp/spark-example-output
```

---

## Running & Debugging in IntelliJ IDEA

### Step 1: Open the Project
1. Open IntelliJ IDEA (Community or Ultimate edition).
2. Click **File -> Open** and select the `scala-spark40-example` root directory.
3. Allow IntelliJ to import the project as a Maven project.

### Step 2: Configure Project SDK
1. Go to **File -> Project Structure -> Project**.
2. Set **SDK** to **21** (JDK 21).
3. Ensure the Scala plugin is installed in IntelliJ.

### Step 3: Configure the Run/Debug Configuration

> [!IMPORTANT]
> **Enable "Include dependencies with Provided scope"**
>
> In [`pom.xml`](pom.xml), Apache Spark, Delta Lake, Derby client, and Scala dependencies are marked `<scope>provided</scope>` so they are not packaged inside the final deployment JAR (since the cluster runtime already provides them).
>
> When running or debugging locally inside IntelliJ, IntelliJ by default strips `provided` dependencies from the classpath. You MUST enable this setting:
> 1. Open your Run/Debug Configuration (**Run -> Edit Configurations...**).
> 2. Select your Application configuration for `com.uj.example.Example`.
> 3. Click **Modify options** (or expand configuration options).
> 4. Check **"Include dependencies with 'provided' scope"**.

### Step 4: Add Java 21 JVM VM Options (If Running Main Class Directly)
When executing or debugging `Example.main` directly in IntelliJ without `spark-submit`, add the following JVM options to the Run Configuration:

```text
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
```
*(Note: These options are already configured automatically in [`pom.xml`](pom.xml) for `mvn test`.)*

---

## Troubleshooting & Common Errors

### 1. `ClassNotFoundException: org.apache.spark.sql.SparkSession`
- **Symptom**: When running `Example.main` or running tests in IntelliJ, the run immediately crashes with a `ClassNotFoundException` or `NoClassDefFoundError` for Spark classes.
- **Cause**: Spark dependencies are marked `<scope>provided</scope>` in [`pom.xml`](pom.xml).
- **Fix**: In your IntelliJ Run Configuration, click **Modify Options** and ensure **Include dependencies with 'provided' scope** is enabled.

---

### 2. `IllegalAccessException: module java.base does not open java.lang to unnamed module`
- **Symptom**: Spark fails to initialize or crashes during serialization / memory allocation with:
  ```
  java.lang.IllegalAccessException: class org.apache.spark.storage.StorageUtils$
  cannot access class sun.nio.ch.DirectBuffer (in module java.base)
  ```
- **Cause**: Strong module encapsulation introduced in Java 17 and enforced in Java 21 prevents reflective access to internal JDK packages.
- **Fix**: Ensure the required `--add-opens` and `--add-exports` JVM arguments (listed in the IntelliJ section above and configured in [`pom.xml`](pom.xml) under `maven-surefire-plugin`) are present in your JVM arguments.

---

### 3. Windows: `Could not locate executable null\bin\winutils.exe in the Hadoop binaries`
- **Symptom**: Running tests or jobs on Windows fails with `java.io.IOException: Could not locate executable ... winutils.exe`.
- **Cause**: Hadoop uses Windows-specific native helper binaries (`winutils.exe` and `hadoop.dll`) for permissions and filesystem emulation.
- **Fix**:
  1. Download the binaries from [winutils](https://github.com/cdarlint/winutils) (select Hadoop 3.3.x or 3.4.x).
  2. Set `HADOOP_HOME=C:\hadoop` where `C:\hadoop\bin\winutils.exe` exists.
  3. Add `%HADOOP_HOME%\bin` to your system `PATH`.
  4. Restart your terminal or IDE.

---

### 4. `Namespace test_db was created concurrently. Ignoring.` or Derby Metastore Lock Errors
- **Symptom**: Warning logs during test runs or errors like `ERROR XSDB6: Another instance of Derby may have already booted the database`.
- **Cause**: Embedded Apache Derby only allows one JVM process to lock `metastore_db` at a time.
- **Fix**:
  - The project's [`TestSparkSession`](src/test/scala/com/uj/testutils/TestSparkSession.scala) trait isolates and synchronizes test runs to prevent test collisions.
  - If a stale lock remains after an unexpected JVM crash, delete the local `metastore_db/` directory:
    ```bash
    rm -rf metastore_db/ derby.log spark-warehouse/
    ```
  - For multi-process concurrency during local development, use the [Remote Derby Network Server setup](#hive-metastore-with-remote-derby-setup) with [`conf/core-site.xml.template`](conf/core-site.xml.template).

---

### 5. Maven: `Plugin is modifying testCompileSourceRoots through add(), which will not work in Maven 4.0.0`
- **Symptom**: A Maven build warning emitted by `scala-maven-plugin` during compilation.
- **Cause**: Upstream `scala-maven-plugin` compatibility notice for the future Maven 4.0 release.
- **Status**: Harmless. Can be suppressed if desired by adding `-Dmaven.project.sourceRoots.warningsDisabled=true` to Maven flags.

---

## Project Structure

```text
scala-spark40-example/
├── .editorconfig               # Cross-editor formatting and indentation rules
├── .gitignore                  # Git ignore rules for Maven, Spark, and IDE artifacts
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions CI workflow (Java 21 / Maven test)
├── conf/
│   ├── core-site.xml.template  # Sample configuration for remote Derby Hive Metastore
│   └── spark-defaults.conf.template # Sample Spark & Delta Lake default properties
├── CONTRIBUTING.md             # Contribution guidelines
├── LICENSE                     # Apache License 2.0
├── README.md                   # Project documentation
├── pom.xml                     # Maven project configuration with Spark 4.x / Scala 2.13
├── run.sh                      # Shell runner script for Linux / macOS
├── run.bat                     # Batch runner script for Windows
└── src/
    ├── main/
    │   ├── resources/
    │   │   └── test.csv        # Sample input data for the ETL pipeline
    │   └── scala/
    │       └── com/uj/example/
    │           └── Example.scala  # Sample Spark job (ETL, UDFs, Hive table registration)
    └── test/
        └── scala/
            └── com/uj/
                ├── example/
                │   └── ExampleTest.scala        # ScalaTest suite verifying transforms & SQL
                └── testutils/
                    └── TestSparkSession.scala   # Reusable, log-suppressed Spark test fixture
```

---

## Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](https://github.com/zzhu8192/scala-spark40-example/issues). Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## License

This project is licensed under the **Apache License 2.0** - see the [LICENSE](LICENSE) file for details.
