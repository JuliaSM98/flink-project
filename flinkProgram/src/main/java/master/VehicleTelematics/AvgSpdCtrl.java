package master.VehicleTelematics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import java.util.*;
import java.lang.*;
import java.io.*;

import java.nio.file.Paths;


public class AvgSpdCtrl {
	
	
	public AvgSpdCtrl() {
	}
	
	public SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Double>> AverageSpeedControl(SingleOutputStreamOperator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> input) {
		
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
		.keyBy(new KeySelector<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
			
			// Create a Keyed Stream for events with the same VID (car identifier), going through the same highway and direction
			// This is done to prevent the program from mixing up different trips from the same car
			
			@Override
			
			public Tuple3<Integer,Integer,Integer> getKey(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> event) throws Exception {
				
				Tuple3<Integer,Integer,Integer> out = new Tuple3<Integer,Integer,Integer>(event.f1, event.f3, event.f5);
				return out;
				
			}
		})
		.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() input {
			
			// Similarly, a car may go through the same highway and direction in different trips
			// To do that, we assign timestamps and watermarks that will be used later on this purpose
			
			@Override
			
			public long extractAscendingTimestamp(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> event) {
				return event.f0*1000;
			}
		})
		.window(EventTimeSessionWindows.withGap(Time.seconds(31)))
        .apply(new AvgWindowFunction());
		
	
	
	
	
		
	}
}
