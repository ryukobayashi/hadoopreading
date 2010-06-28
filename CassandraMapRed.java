package com.example.mapreduce;

import java.util.Arrays;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class CassandraMapRed extends Configured implements Tool {
    private static final String KEYSPACE = "Keyspace1";
    private static final String COLUMN_FAMILY = "hassandra";
    private static final String CONF_COLUMN_NAME = "columnName";
    private static final String COLUMN_NAME = "name";

    public static class CassandraMap extends Mapper<String, SortedMap<byte[], IColumn>, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();
        private String columnName;

        protected void map(String key, SortedMap<byte[], IColumn> columns, Context context)
                    throws java.io.IOException ,InterruptedException {
            IColumn column = columns.get(columnName.getBytes());
            if (column == null) {
                return;
            }

            word.set(new String(column.value()));
            context.write(word, one);
        }

        protected void setup(Context context)
                    throws java.io.IOException ,InterruptedException {
            this.columnName = context.getConfiguration().get(CONF_COLUMN_NAME);
        }
    }

    public static class CassandraReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
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
        String outputPath = args[1];
        Configuration conf = getConf();

        conf.set(CONF_COLUMN_NAME, COLUMN_NAME);
        Job job = new Job(conf, "CassandraMapRed");
        job.setJarByClass(CassandraMapRed.class);

        job.setMapperClass(CassandraMap.class);
        job.setCombinerClass(CassandraReduce.class);
        job.setReducerClass(CassandraReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(ColumnFamilyInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        ConfigHelper.setColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
        // Cassandraのレンジサイズ
        ConfigHelper.setRangeBatchSize(job.getConfiguration(), 50000);
        SlicePredicate predicate =
            new SlicePredicate().setColumn_names(Arrays.asList(COLUMN_NAME.getBytes()));
        ConfigHelper.setSlicePredicate(job.getConfiguration(), predicate);

        return job.waitForCompletion(true) ? 0 : -1;
    }
}
