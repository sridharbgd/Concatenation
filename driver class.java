{\rtf1\ansi\ansicpg1252\cocoartf1504\cocoasubrtf760
{\fonttbl\f0\fnil\fcharset0 Menlo-Regular;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww13020\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 package combiner;\
import org.apache.hadoop.conf.Configuration;\
import org.apache.hadoop.conf.Configured;\
import org.apache.hadoop.fs.FileSystem;\
import org.apache.hadoop.fs.Path;\
import org.apache.hadoop.io.LongWritable;\
import org.apache.hadoop.io.Text;\
import org.apache.hadoop.mapreduce.Job;\
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;\
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;\
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;\
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;\
import org.apache.hadoop.util.GenericOptionsParser;\
import org.apache.hadoop.util.Tool;\
import org.apache.hadoop.util.ToolRunner;\
\
import combiner.MultipleMap1;\
import combiner.MultipleMap2;\
import combiner.MultipleReducer;\
\
public class MultipleFiles extends Configured implements Tool\{\
\
	private Configuration conf;\
	@Override\
	public Configuration getConf()\
	\{\
	return conf;\
	\}\
	@Override\
	public void setConf(Configuration conf)\
	\{\
		this.conf=conf;\
	\}\
	@Override\
	public int run(String []args)throws Exception\
	\{\
		\
		Job MultipleFiles=new Job(getConf());\
		MultipleFiles.setJobName("mat word count");\
		MultipleFiles.setJarByClass(this.getClass());\
		MultipleFiles.setMapperClass(MultipleMap1.class);\
		MultipleFiles.setMapperClass(MultipleMap2.class);\
		MultipleFiles.setReducerClass(MultipleReducer.class);\
		//wordcountjob.setCombinerClass(WordCountCombiner.class);\
		//wordcountjob.setPartitionerClass(WordCountPartitioner.class);\
		MultipleFiles.setMapOutputKeyClass(Text.class);\
		MultipleFiles.setMapOutputValueClass(Text.class);\
		MultipleFiles.setOutputKeyClass(Text.class);\
		MultipleFiles.setOutputValueClass(Text.class);\
		FileInputFormat.setInputPaths(MultipleFiles,new Path(args[0]));\
		//FileInputFormat.setInputPaths(wordcountjob,new Path(args[1]));\
		FileOutputFormat.setOutputPath(MultipleFiles,new Path(args[1]));\
		MultipleFiles.setNumReduceTasks(1);\
		return MultipleFiles.waitForCompletion(true)==true? 0:1;\
	\}\
	public static void main(String []args)throws Exception\
	\{\
		ToolRunner.run(new Configuration(),new MultipleFiles(),args);\
	\}\
\
\}\
}