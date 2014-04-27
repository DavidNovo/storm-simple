package net.telematics;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.io.*;
import java.util.Map;

/**
 * Created by david.j.novogrodsky on 4/25/2014.
 */
public class FilePrintBolt implements IBasicBolt {

    private FileWriter fileWriter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        File filename = new File("FirstPrintBolt.txt");

        try {
            this.fileWriter = new FileWriter(filename);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

        @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
           // System.out.println( tuple);
            try {
                fileWriter.write(tuple.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    @Override
    public void cleanup() {
        try {
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
