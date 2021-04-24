package com.jspark;

import com.jspark.config.SparkJobVariables;
import com.jspark.steps.AnalyticsFirstWeekEngagementStep;
import com.jspark.steps.EventsIngestorStep;

public class EventsProcessor {

    final private SparkJobVariables sparkJobVariables = new SparkJobVariables();
    public final String eventsSourcePath = sparkJobVariables.getEnvVarOrProperty("EVENTS_SOURCE_PATH");
    public final String processedRegisteredEventsPath = System.getProperty("user.dir") +
            sparkJobVariables.getEnvVarOrProperty("PROCESSED_REGISTERED_EVENTS_PATH");
    public final String processedAppLoadedEventsPath = System.getProperty("user.dir") +
            sparkJobVariables.getEnvVarOrProperty("PROCESSED_APPLOADED_EVENTS_PATH");
    public final String sparkMasterConfig = sparkJobVariables.getEnvVarOrProperty("SPARK_MASTER");

    public EventsProcessor(){}

    public void execute(String[] args){
        try {
            String step = args[0];
            if(step.equals("step1")){
                EventsIngestorStep pipelineStep = new EventsIngestorStep(eventsSourcePath,
                        processedRegisteredEventsPath, processedAppLoadedEventsPath, sparkMasterConfig);
                pipelineStep.run();
            }
            else{
                AnalyticsFirstWeekEngagementStep pipelineStep = new AnalyticsFirstWeekEngagementStep(
                        processedRegisteredEventsPath, processedAppLoadedEventsPath, sparkMasterConfig);
                pipelineStep.run();
            }
        }
        catch (ArrayIndexOutOfBoundsException exception){
            System.out.println("You must specify a valid step as argument for this Spark Application");
            System.out.println("Enter \"step1\" for running the Events Ingestion Step");
            System.out.println("Or \"step2\" (or anything else really) for Engagement of User Registered on it's first week");
            throw exception;
        }
    }

    public static void main(String[] args){
        EventsProcessor eventsProcessor = new EventsProcessor();
        eventsProcessor.execute(args);

    }
}
