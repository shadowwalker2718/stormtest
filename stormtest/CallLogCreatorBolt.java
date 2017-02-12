package stormtest;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CallLogCreatorBolt implements IRichBolt {
  //Create instance for OutputCollector which collects and emits tuples to produce output
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
       this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
    	if(tuple.getSourceStreamId().equals("streamone")) {
       String from = tuple.getString(0);
       String to = tuple.getString(1);
       Integer duration = tuple.getInteger(2);
       collector.emit("streamone", new Values(from + " - " + to, duration));
    	} else {
       String from = tuple.getString(0);
       String to = tuple.getString(1);
       Integer duration = tuple.getInteger(2);
       collector.emit("streamtwo", new Values("iqbal" + from + " - " + to, duration));    		
    	}
    collector.ack(tuple);
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
       //declarer.declare(new Fields("call", "duration"));
       declarer.declareStream("streamone", new Fields("call", "duration"));
       declarer.declareStream("streamtwo", new Fields("call", "duration"));
    }
     
    @Override
    public Map<String, Object> getComponentConfiguration() {
       return null;
    }
}
