package com.company;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.PriorityQueue;

class Job
{
    String  jobName;
    String  jobType;
    int     duration;
    boolean recurring;

    public Job(String jobName, int duration, boolean recurring, String jobType) {
        this.jobName = jobName;
        this.duration = duration;
        this.recurring = recurring;
        this.jobType = jobType;
    }
}

class Task implements Runnable
{
    private String name;

    public Task(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public void run()
    {
        try {
            System.out.println("Doing a task " + this.getName() + " during : " + name + " - Time - " + new Date());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}


public class Scheduler {

    static class ProcessThread extends Thread {

        public void run(){
            while(!exitScheduler) {
                Job job = null;

                if (executor != null) {
                    synchronized (queue) {
                        if (queue != null && !queue.isEmpty()) {
                            job = queue.remove();
                            if (jobCounter < jobLimit) {
                                ++jobCounter;
                            } else {
                                queue.add(job);
                                job = null;
                            }
                        }
                    }

                    if (job != null) {
                        launchJob(job.jobName,
                                job.duration,
                                job.recurring);
                    }

                    if (job != null) {
                        if (job.recurring) {
                            synchronized(queue) {
                                queue.add(job);
                            }
                        }
                        --jobCounter;
                    }
                }
            }

            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    static class JobComparator implements Comparator<Job> {

        public int compare(Job j1, Job j2) {
            return (!j1.recurring && !j2.recurring) ? 0 : (j1.recurring) ? 1 : -1;
        }
    }

    private static ScheduledExecutorService executor = null;
    private static  int poolLimit=2;
    private static  int jobLimit = 2;
    private static  int jobCounter = 0;
    private static  PriorityQueue<Job> queue = new PriorityQueue<Job>(5, new JobComparator());
    private static  boolean exitScheduler = false;
    private static ProcessThread processThread = null;

    public static void initExecutor() {
        executor = Executors.newScheduledThreadPool(poolLimit);
    }
    public static void initProcessThread() {
        processThread = new ProcessThread();
        processThread.start();
    }

    public static String getProcessThreadStatus() {
        return processThread.getState().toString();
    }
    public static void exitProcessThread() throws InterruptedException {
        exitScheduler = true;

        while (processThread.getState() != Thread.State.TERMINATED) {
            Thread.sleep(500);
        }
    }

    public static void launchJob(String jobName, int duration, boolean recurring) {
        Task task = new Task (jobName);
        executor.schedule(task, duration , TimeUnit.SECONDS);
    }

    public static void processJob(String jobName, int duration, boolean recurring, String jobType) {
        Job job = new Job(jobName, duration, recurring, jobType);

        synchronized(queue) {
            queue.add(job);
        }
    }

    public static void main(String[] args) throws InterruptedException {
	// This code is just for testing!!!
        initExecutor();
        initProcessThread();

        processJob("one", 2, true, "Type1");
        processJob("two", 2, true, "Type1");
        processJob("three", 1, false, "Type1");
        processJob("four", 3, false, "Type1");

        Thread.sleep(1000);

        System.out.println("Status: " + getProcessThreadStatus());

        exitProcessThread();

        System.out.println("Status: " + getProcessThreadStatus());
    }
}
