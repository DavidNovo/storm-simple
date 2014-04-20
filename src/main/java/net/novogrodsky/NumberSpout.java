package net.novogrodsky;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by davidnovogrodsky_wrk on 4/12/14.
 */
public class NumberSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random randomNumberGenerator;
    private static int currentNumber = 1;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        randomNumberGenerator = new Random();
    }

    @Override
    public void nextTuple() {
        // define the severity level
        String[] severityLevels = new String[]{ "Critical", "High", "Medium", "Low", "Debug" };
        String severity = severityLevels[randomNumberGenerator.nextInt(severityLevels.length)];

        // Emit the next tuples
        collector.emit(new Values(new Integer(currentNumber++), null, severity));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("DeviceID", "type", "severity"));
    }

}
