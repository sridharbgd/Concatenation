{\rtf1\ansi\ansicpg1252\cocoartf1504\cocoasubrtf760
{\fonttbl\f0\fnil\fcharset0 Menlo-Regular;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww13020\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 package combiner;\
\
import java.io.IOException;\
\
import org.apache.hadoop.io.LongWritable;\
import org.apache.hadoop.io.Text;\
import org.apache.hadoop.mapreduce.Mapper;\
public class MultipleMap1 extends Mapper<LongWritable,Text,Text,Text>\
	\{\
	Text keyEmit = new Text();\
	Text valEmit = new Text();\
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException\
		\{\
			String line=value.toString();\
			String[] words=line.split("-");\
			keyEmit.set(words[0]);\
			valEmit.set(words[1]);\
			context.write(keyEmit, valEmit);\
		\}\
	\}\
}