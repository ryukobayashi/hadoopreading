package com.example.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class HDFSMapRed extends Configured implements Tool {

    public static class HDFSMap extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();

        protected void map(LongWritable key, Text value, Context context)
                    throws java.io.IOException ,InterruptedException {
            word.set(value.toString());
            context.write(word, one);
        }
    }

    public static class HDFSReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                    throws java.io.IOException ,InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[1];
        String outputPath = args[2];

        Job job = new Job(getConf(), "HDFSMapRed");
        job.setJarByClass(HDFSMapRed.class);

        job.setMapperClass(HDFSMap.class);
        job.setCombinerClass(HDFSReduce.class);
        job.setReducerClass(HDFSReduce.class);

        TextInputFormat.setInputPaths(job, inputPath);
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
