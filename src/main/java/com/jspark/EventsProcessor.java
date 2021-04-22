package com.jspark;

import java.util.logging.Level;
import java.util.logging.Logger;

public class EventsProcessor {
    // private static final Logger LOGGER = Logger.getLogger(EventsProcessor.class.getName() );

    public static void main(String[] args){
        try {
            String step = args[0];
            if(step.equals("step1")){
                EventsIngestorStep.run();
            }
            else{
                AnalalyticsFirstWeekEngamentStep.run();
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
