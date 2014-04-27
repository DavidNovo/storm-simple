package net.telematics;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class DeviceTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        /*
        // this topology works
        builder.setSpout("spout", new DeviceSpout());
        builder.setBolt("prime", new PrimeNumberBolt())
                .shuffleGrouping("spout");
        builder.setBolt("criticalBolt", new CriticalSeverityBolt())
               .shuffleGrouping("prime");
        //
        */


        // this topology also works
        // note the tuples sent to the FilePrintBolt are the same as the
        // one sent to the PrinterBolt
        // and....I can output information to a file!!
        builder.setSpout("spout", new DeviceSpout());
        builder.setBolt("prime", new PrimeNumberBolt())
                .shuffleGrouping("spout");
        builder.setBolt("criticalSeverityFilter", new CriticalSeverityFilterBolt())
                .shuffleGrouping("prime");
        builder.setBolt("printerBolt", new PrinterBolt())
                .fieldsGrouping("criticalSeverityFilter", new Fields("severity"));
        builder.setBolt("filePrintBolt", new FilePrintBolt())
                .fieldsGrouping("criticalSeverityFilter", new Fields("severity"));


        /*
        // this topology is testing the new custom stream grouping
        // the custom grouping does not work
        builder.setSpout("spout", new DeviceSpout());
        builder.setBolt("prime", new PrimeNumberBolt())
                .shuffleGrouping("spout");
        // this is testing the new custom grouping
        builder.setBolt("printerBolt", new PrinterBolt())
                .customGrouping("prime", new CriticalStreamGrouping());
        //
        */
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("telematics", conf, builder.createTopology());

        // to prevent the topology from running for a long time...
        Utils.sleep(10000);
        cluster.killTopology("telematics");
        cluster.shutdown();

    }
}
