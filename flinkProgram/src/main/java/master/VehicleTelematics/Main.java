package master.VehicleTelematics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import java.util.*;
import java.lang.*;
import java.io.*;

import java.nio.file.Paths;

public class Main {

    // main() 
    public static void main(String[] args){
    
    
		// execution environment setup
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// reading input and output data
		String inFilePath = args[0];
		String outFilePath = args[1];
		// CsvReader csvFile = env.readCsvFile(inFilePath);
		DataStreamSource<String> source = env.readTextFile(inFilePath);

		SingleOutputStreamOperator<Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>> filterOut = source.map(new MapFunction<String, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
            @Override
            public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> map(String in) throws Exception {
                String[] fieldArray = in.split(",");
                Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> out = new Tuple8(Integer.parseInt(fieldArray[0]), Integer.parseInt(fieldArray[1]), Integer.parseInt(fieldArray[2]), Integer.parseInt(fieldArray[3]), Integer.parseInt(fieldArray[4]), Integer.parseInt(fieldArray[5]), Integer.parseInt(fieldArray[6]), Integer.parseInt(fieldArray[7]));

                return out;
            }
        });
        
        
    	SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> speedControl = new SpdFilter().speedFilter(filterOut);
    	SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Double>> avgSpeedControl = new AvgSpdCtrl().AverageSpeedControl(filterOut);
    	SingleOutputStreamOperator<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> accReporter = new AccidentReporter().AccidentReport(filterOut);
    	
    	
    	speedControl.writeAsCsv(Paths.get(outFilePath, "speedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    	avgSpeedControl.writeAsCsv(Paths.get(outFilePath, "avgspeedfines.csv").toString(), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    	accReporter.writeAsCsv(Paths.get(outFilePath, "accidents.csv").toString(), FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    	try {
            env.execute("Main");
        } catch (Exception e) {
            e.printStackTrace();
        }
    

    }
}
