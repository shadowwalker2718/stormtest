package stormtest;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class LogAnalyserStorm {
  public static void main(String[] args) throws Exception{
    //Create Config instance for cluster configuration
    Config config = new Config();
    //config.setDebug(false);
    config.setDebug(true);

    //
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());

    builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt())
      .shuffleGrouping("call-log-reader-spout", "streamone")
      .shuffleGrouping("call-log-reader-spout", "streamtwo");

    builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt())
      .fieldsGrouping("call-log-creator-bolt", "streamone", new Fields("call"))
      .fieldsGrouping("call-log-creator-bolt", "streamtwo", new Fields("call"));

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
    Utils.sleep(30000);

    //Stop the topology
    cluster.killTopology("LogAnalyserStorm");
    cluster.shutdown();
  }
}
