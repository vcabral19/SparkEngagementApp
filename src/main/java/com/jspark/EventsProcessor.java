package com.jspark;

import com.jspark.steps.AnalyticsFirstWeekEngagementStep;
import com.jspark.steps.EventsIngestorStep;

public class EventsProcessor {
    // private static final Logger LOGGER = Logger.getLogger(EventsProcessor.class.getName() );

    public static void main(String[] args){
        try {
            String step = args[0];
            if(step.equals("step1")){
                EventsIngestorStep pipelineStep = new EventsIngestorStep();
                pipelineStep.run();
            }
            else{
                AnalyticsFirstWeekEngagementStep pipelineStep = new AnalyticsFirstWeekEngagementStep();
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
}
