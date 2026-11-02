@echo off
setlocal enabledelayedexpansion

REM Script to run the Spark example job locally via spark-submit on Windows
set "PROJECT_DIR=%~dp0"
set "JAR_FILE=%PROJECT_DIR%target\example-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
set "INPUT_CSV=%PROJECT_DIR%src\main\resources\test.csv"
set "OUTPUT_DIR=%TEMP%\spark-example-output"

if not exist "%JAR_FILE%" (
    echo Jar file not found at: %JAR_FILE%
    echo Please build the project first: mvn clean package
    exit /b 1
)

where spark-submit >nul 2>nul
if %errorlevel% neq 0 (
    echo Error: spark-submit not found in PATH.
    echo Ensure SPARK_HOME is set and SPARK_HOME\bin is added to your PATH.
    exit /b 1
)

if exist "%OUTPUT_DIR%" (
    echo Cleaning previous output directory at %OUTPUT_DIR%...
    rmdir /s /q "%OUTPUT_DIR%"
)

echo Running Spark job...
spark-submit --class com.uj.example.Example --master "local[*]" "%JAR_FILE%" "file:///%INPUT_CSV:\=/%" "%OUTPUT_DIR:\=/%"

if %errorlevel% equ 0 (
    echo Job finished successfully! Output written to: %OUTPUT_DIR%
) else (
    echo Spark job encountered an error. Exit code: %errorlevel%
    exit /b %errorlevel%
)
endlocal
