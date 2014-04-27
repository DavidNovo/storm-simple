package net.telematics;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;


public class CriticalSeverityBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        // output of critical streams only
        //System.out.println("entering the criticalSeverityBolt.execute()");
        int number = tuple.getInteger(0);
        String severity = tuple.getString(2);
        if (tuple.getString(2).contains("Critical")) {
            System.out.println("CRITICAL!!! - the number is:" + number + "   severity is:" + severity);
        }

        // since I am not emitting anything
        // the tuple below is null
        //collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // nothing here because I am not sending to another bolt
    }
}
