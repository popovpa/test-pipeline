package utils;

import utils.exceptions.ETLException;

public interface PipelineTask {

    String taskName();

    boolean canRun();

    boolean canSkip();

    void run() throws ETLException;

    void complete();
}