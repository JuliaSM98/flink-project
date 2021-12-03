package master.VehicleTelematics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Collector.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import java.util.*;
import java.lang.*;
import java.io.*;

import java.nio.file.Paths;


public class AccidentReporter {
	
	
	public AccidentReporter() {
	}
	
	public static SingleOutputStreamOperator<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> AccidentReport(SingleOutputStreamOperator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> input) {
		
		SingleOutputStreamOperator<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> output = input
		.keyBy(new KeySelector<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>,Tuple5<Integer,Integer,Integer,Integer,Integer>>() {
			
			// Create a Keyed Stream for events with the same VID (car identifier), going through the same highway and direction
			// This is done to prevent the program from mixing up different cars and trayectos
			
			@Override
			
			public Tuple5<Integer,Integer,Integer,Integer,Integer> getKey(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> event) throws Exception {
				
				Tuple5<Integer,Integer,Integer,Integer,Integer> out = new Tuple5<Integer,Integer,Integer,Integer,Integer>(event.f1, event.f3, event.f5, event.f6, event.f7);
				return out;
				
			}
		})
		.countWindow(4,1)
        .apply(new WindowFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>,Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>,Tuple5<Integer,Integer,Integer,Integer,Integer>,GlobalWindow>() {
        	
        	@Override
        		
        	public void apply(Tuple5<Integer,Integer,Integer,Integer,Integer> key, GlobalWindow window, Iterable<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> in, Collector<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> out) throws Exception {
        			
        		
        		int cnt = 1;
				int time1 = 0;
				int time2 = 0;
        		
        		for(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> event : in) {
        				
        				if(cnt == 1) {
							time1 = event.f0;
						}
						else if(cnt == 4) {
							time2 = event.f0;
						}
						
						cnt += 1;
					
				}
				
				if (cnt < 4) {
					return;
				}
				
				// Output = time1, time2, vid, xway, seg, dir, pos
				out.collect(new Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>(time1,time2,key.f0,key.f1,key.f3,key.f2,cnt-1));
				
			}
		});
		
		return output;
	
	
	
		
	}
}
