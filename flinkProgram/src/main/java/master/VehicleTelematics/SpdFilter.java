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


public class SpdFilter {
	
	
	public SpdFilter() {
	}
	
	public SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> speedFilter(SingleOutputStreamOperator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> input) {
		
		
		SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> output = input
		.filter(new FilterFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
			
			    	
			@Override
			public boolean filter(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> event) throws Exception {
				
				if(event.f2 > 90){
					return true;
				}
				
				else {
					return false;
				}
				
			}
			
		})
		.map(new MapFunction<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>() {
			
			@Override
			
			public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> map(Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> event) throws Exception {
				Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> filteredEvent = new Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>();
				filteredEvent.f0 = event.f0;
				filteredEvent.f1 = event.f1;
				filteredEvent.f2 = event.f3;
				filteredEvent.f3 = event.f6;
				filteredEvent.f4 = event.f5;
				filteredEvent.f5 = event.f2;
				
				return filteredEvent;
			}
		});
		
		return output;
	}
}
		
		
