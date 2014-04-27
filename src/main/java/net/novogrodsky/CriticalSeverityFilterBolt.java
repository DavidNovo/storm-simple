package net.telematics;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;


public class CriticalSeverityFilterBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
         public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        int number = tuple.getInteger(0);
        String severity = tuple.getString(2);
        if (tuple.getString(2).contains("Critical")) {
            collector.emit(tuple, new Values(tuple.getInteger(0), null, tuple.getString(2)));
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("DeviceID", "type", "severity"));
    }
}
