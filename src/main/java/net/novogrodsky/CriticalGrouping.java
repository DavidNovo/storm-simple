package net.novogrodsky;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by davidnovogrodsky_wrk on 4/19/14.
 */
public class CriticalGrouping implements CustomStreamGrouping {

    List<Integer> target;


    /**
     * Tells the stream grouping at runtime the tasks in the target bolt.
     * This information should be used in chooseTasks to determine the target tasks.
     *
     * It also tells the grouping the metadata on the stream this grouping will be used on.
     */
    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> targets) {
        List<Integer> sorted = new ArrayList<Integer>(targets);
        Collections.sort(sorted);
        target = Arrays.asList(sorted.get(0));
    }

    /**
     * This function implements a custom stream grouping. It takes in as input
     * the number of tasks in the target bolt in prepare and returns the
     * tasks to send the tuples to.
     *
     */
    @Override
    public List<Integer> chooseTasks(int i, List<Object> objects) {
        return target;
    }
}
