package net.novogrodsky;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by davidnovogrodsky_wrk on 4/12/14.
 */
public class PrimeNumberTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new NumberSpout());
        builder.setBolt("prime", new PrimeNumberBolt())
                .shuffleGrouping("spout");


        Config conf = new Config();


        // conf.setDebug(true);
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
