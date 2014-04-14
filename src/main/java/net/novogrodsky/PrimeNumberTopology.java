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
    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new NumberSpout());
        builder.setBolt("prime", new PrimeNumberBolt())
                .shuffleGrouping("spout");


        Config conf = new Config();
/*

// making sure workers are deployed to cluster
        // remove these lines to run on command line
        // java -jar storm-test-1.0-SNAPSHOT.jar from the target directory.
        conf.setNumWorkers(2);
        conf.setMaxSpoutPending(5000);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

        // this is used to stop the toology when run locally
        // remove when deployed to a cluster
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
*/
 /*       if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
*/

        conf.setDebug(true);
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        //Utils.sleep(10000);
        //cluster.killTopology("test");
        //cluster.shutdown();
    }
}
