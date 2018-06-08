package utils;

import java.util.ArrayList;
import java.util.List;

public class ETLPipelineBuilder {

    private Object locker = new Object();

    private List<PipelineTask> tasks = new ArrayList<>();

    private ETLPipelineBuilder() {
    }

    public static ETLPipelineBuilder getBuilder() {
        return new ETLPipelineBuilder();
    }

    public ETLPipelineBuilder step(PipelineTask task) {
        tasks.add(task);
        return this;
    }

    public boolean run() {
        List<Thread> threads = new ArrayList<>();

        tasks.forEach(t -> {
            Thread th = new TaskThread(t);
            th.setUncaughtExceptionHandler((throwThread, e) -> {
                e.printStackTrace();
                threads.forEach(Thread::interrupt);
            });
            threads.add(th);
        });
        threads.forEach(Thread::start);
        return true;
    }

    private class TaskThread extends Thread {

        private long checkDelay = 100;

        private PipelineTask task;

        public TaskThread(PipelineTask task) {
            this.task = task;
        }

        @Override
        public void run() {
            try {
                if (!task.canSkip()) {
                    System.out.println(String.format("Start '%s' task.", task.taskName()));
                    while (!task.canRun()) {
                        System.out.println("Awaiting success flag for begin...");
                        Thread.sleep(checkDelay);
                    }
                    System.out.println(String.format("Running '%s' task...", task.taskName()));
                    task.run();
                    task.complete();
                    System.out.println(String.format("Task '%s' successful complete.", task.taskName()));
                } else {
                    System.out.println(String.format("Skip this step, because '%s' already successful complete.", task.taskName()));
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}