package com.nibin.crunch;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception
    {
        ToolRunner.run(new Configuration(), new App(), args);
    }
    
    public int run(String[] args) throws Exception{
    	if(args.length!=2){
    		System.err.println("Invalid Number of arguments");
    		
    		System.err.println();
    		GenericOptionsParser.printGenericCommandUsage(System.err);
    	return 1;
    	}
    
    String inputPath = args[0];
    String outputPath = args[1];
    
    Pipeline pipeline = new MRPipeline(App.class,getConf());
    
    PCollection<String> lines = pipeline.readTextFile(inputPath);
    
    PCollection<String> words = lines.parallelDo(new Tokenizer(), Writables.strings());
		
    PTable<String, Long> counts = words.count();
    
    pipeline.writeTextFile(counts, outputPath);
    
    PipelineResult result = pipeline.done();
    
    return result.succeeded() ? 0 : 1;    
    }
}
