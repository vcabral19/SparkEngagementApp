# SparkEngagementApp
A Java Spark Application for Ingest and Calculate Engagement for recently registered user

## tl;dr
spark-submit passing step1 or step2 as arguments

## Requirements
#### Open JDK 11.0.10
#### Spark 3.1.1
#### Maven 3.8.1
#### A Spark cluster running

If you are running it locally you can check how to use a standalone Spark for easy deployment here:
https://spark.apache.org/docs/latest/spark-standalone.html

## Usage
This Application have 2 different steps:

* Ingestion of .json source data on two different events on Parquet (User Registered Events and Application Loaded Events)  a.k.a. "**step1**"
* Running an analytical job on top of the two Parquet datasets to calculate the fraction of users who Loaded the App at least once in the Calendar Week after the registration a.k.a. "**step2**"

To run step2 you need to first run step1 in order to generate the data for it to be done.

### Generating .jar

In order to generate the .jar file for the package just run:
```bash
mvn --file pom.xml clean
```

and then

```bash
mvn --file pom.xml package
```
The artifact should be created at "target/SparkEngagementApp-1.0-SNAPSHOT.jar" in the project folder by default.

### Running the steps

You can easily spark-submit the application specifying the step as an argument.

Remember to pass: 
#### step1
for the ingestion and
#### step2
for getting the print with the result of the engagement metric like in the following example


```bash
spark-submit --class com.jspark.EventsProcessor target/SparkEngagementApp-1.0-SNAPSHOT.jar step1
```

Considering that "target/SparkEngagementApp-1.0-SNAPSHOT.jar" is you generated .jar for the application and "step1" is the argument.

### Project Structure

```bash
.
├── SparkEngagementApp.iml
├── data
│   ├── processed
│   │   ├── app_loaded
│   │   └── registered 
│   └── raw
├── pom.xml
├── src
   ├── main
      ├── java
      │   └── com
      │       └── jspark
      │           ├── EventsProcessor.java
      │           ├── analytics
      │           │   └── FirstWeekEngagementMetricCalculator.java
      │           ├── config
      │           │   └── SparkJobVariables.java
      │           ├── generic
      │           │   └── transformations
      │           │       └── SchemaTransformations.java
      │           ├── ingestion
      │           │   ├── ApplicationLoadingIngestion.java
      │           │   ├── IngestionInterface.java
      │           │   └── UserRegistrationIngestion.java
      │           ├── read
      │           │   ├── DataFormats.java
      │           │   ├── DataSourceInterface.java
      │           │   └── LocalFileSystemDataSource.java
      │           ├── schema
      │           │   ├── ApplicationLoadingSchema.java
      │           │   └── UserRegistrationSchema.java
      │           ├── steps
      │           │   ├── AnalyticsFirstWeekEngagementStep.java
      │           │   ├── EventsIngestorStep.java
      │           │   └── StepInterface.java
      │           └── write
      │               └── DataWriter.java
      └── resources
          └── config.properties


```


* EventProcessor.java: Job entrypoint class

* analytics:
spark core logic for the Engagement Metric
* config:
object used to obtain variables for either the System (env vars) or the properties file in the resources folder

* generic.transformations:
static logic with higher pontential to be reused (something like a utils)

* ingestion:
ingestion logic for the step1

* read:
encapsulated data reader module

* schema
static single point of truth for the processed datasets schema

* steps
definition of the steps (step1 ingestion and step2 analytics)

* write
encapsulated writer module

and finally 
* data: raw directory is where you should put your data

## Possible Improvements

* Define a standarized logger object
* Changing the prints to log

Feel free to take a look at the code and let me know if you have any questions.
