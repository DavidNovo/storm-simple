package net.telematics;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by david.j.novogrodsky on 4/21/2014.
 */
public  class CriticalStreamGrouping implements CustomStreamGrouping {
    int numTasks = 0;
    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId,
                        List<Integer> integers) {
        numTasks = integers.size();
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> objects) {

        List<Integer> boltsID = new ArrayList();
        if (objects.size()>0){
            // getting the tuple from the stream, I hope
            String currentSeverity = objects.get(0).toString();
            // testing for the desired value in the tuple
            if (currentSeverity.contains("Critical")) {
                boltsID.add(0); //add correct id
            }
        }
        return boltsID;
    }

}
