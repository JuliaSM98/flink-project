package master.VehicleTelematics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import java.util.*;
import java.lang.*;
import java.io.*;

import java.nio.file.Paths;


public class AvgSpdCtrl {
	
	
	public AvgSpdCtrl() {
	}
	
	public static SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Double>> AverageSpeedControl(SingleOutputStreamOperator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> input) {
		
		SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Double>> output = input
		.filter(new FilterFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
			
			// Only keep tuples within the specified segment [52, 56]
			
			@Override
			public boolean filter(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> event) throws Exception {
				
				if(event.f6 >= 52 && event.f6 <= 56){
					return true;
				}
				else {
					return false;
				}
				
			}
			
		})
		.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
			
			// A car may go through the same highway and direction in different trips. It is necessary to separe trips separed by empty recordings. 
			// To do that, we assign timestamps and watermarks that will be used later on this purpose
			
			@Override
			
			public long extractAscendingTimestamp(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> event) {
				return event.f0*1000;
			}
		})
		.keyBy(new KeySelector<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>,Tuple3<Integer,Integer,Integer>>() {
			
			// Create a Keyed Stream for events with the same VID (car identifier), going through the same highway and direction
			// This is done to prevent the program from mixing up different cars and trayectos
			
			@Override
			
			public Tuple3<Integer,Integer,Integer> getKey(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> event) throws Exception {
				
				Tuple3<Integer,Integer,Integer> out = new Tuple3<Integer,Integer,Integer>(event.f1, event.f3, event.f5);
				return out;
				
			}
		})
		.window(EventTimeSessionWindows.withGap(Time.seconds(31)))
        .apply(new WindowFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>,Tuple6<Integer,Integer,Integer,Integer,Integer,Double>,Tuple3<Integer,Integer,Integer>,TimeWindow>() {
        	
        	@Override
        		
        	public void apply(Tuple3<Integer,Integer,Integer> key, TimeWindow window, Iterable<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> in, Collector<Tuple6<Integer,Integer,Integer,Integer,Integer,Double>> out) throws Exception {
        			
				int pos_min = 527999;
        		int pos_max = 0;
        		int time_min = Integer.MAX_VALUE;
        		int time_max = Integer.MIN_VALUE;
        		int seg_min = 99;
        		int seg_max = 0;

       			for(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> event : in) {
        				
       				pos_min = Integer.min(pos_min,event.f7);
       				pos_max = Integer.max(pos_max,event.f7);
					time_min = Integer.min(time_min,event.f0);
					time_max = Integer.max(time_max,event.f0);
					seg_min = Integer.min(seg_min,event.f6);
					seg_max = Integer.max(seg_max,event.f6);
					
				}				
				
				if(seg_min != 52  || seg_max != 56) {
					return;
				}
				
				double averageSpeed = ((double)(pos_max-pos_min)/(double)(time_max - time_min)); // mps
				averageSpeed = averageSpeed * 2.23694; // 1 mps = 2.23694 mph
				
				if(averageSpeed > 60) {	
					out.collect(new Tuple6<Integer,Integer,Integer,Integer,Integer,Double>(pos_min,time_max,key.f0,key.f1,key.f2,averageSpeed));
				}
			}
		});
		
		return output;
	
	
	
		
	}
}
