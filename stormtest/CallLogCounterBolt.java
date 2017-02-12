package stormtest;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CallLogCounterBolt implements IRichBolt {
    Map<String, Integer> counterMap;
    Map<String, Long> durationMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
       this.counterMap = new HashMap<String, Integer>();
       this.durationMap = new HashMap<String, Long>();
       this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
    	if(tuple.getSourceStreamId().equals("streamone")) {
    		   String call = tuple.getString(0);
    	       Integer duration = tuple.getInteger(1);
    	         
    	       if(!counterMap.containsKey(call)){
    	          counterMap.put(call, 1);
    	          durationMap.put(call, (long)duration);
    	       }else{
    	          Integer c = counterMap.get(call) + 1;
    	          counterMap.put(call, c);
    	          Long d = durationMap.get(call) + duration;
    	          durationMap.put(call, d);
    	       }
    	} else {
    		String call = tuple.getString(0) + "asif";
 	       Integer duration = tuple.getInteger(1);
 	         
 	       if(!counterMap.containsKey(call)){
 	          counterMap.put(call, 1);
 	          durationMap.put(call, (long)duration);
 	       }else{
 	          Integer c = counterMap.get(call) + 1;
 	          counterMap.put(call, c);
 	          Long d = durationMap.get(call) + duration;
 	          durationMap.put(call, d);
 	       }
    	}
         
       collector.ack(tuple);
    }

    @Override
    public void cleanup() {
       for(Map.Entry<String, Integer> entry:counterMap.entrySet()){
          System.out.println("Counter: " + entry.getKey()+" : " + entry.getValue());
       }
       for(Map.Entry<String, Long> entry:durationMap.entrySet()){
           System.out.println("Duration: " + entry.getKey()+" : " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
       declarer.declare(new Fields("call"));
    }
     
    @Override
    public Map<String, Object> getComponentConfiguration() {
       return null;
    }
}
