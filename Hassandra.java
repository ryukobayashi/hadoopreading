package com.example;

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

import com.example.mapreduce.CassandraMapRed;
import com.example.mapreduce.HDFSMapRed;

public class Hassandra {
    private static final Log log = LogFactory.getLog(Hassandra.class);

    private static final String HDFS = "hdfs";
    private static final String CASSANDRA = "cassandra";

    /**
     * @param args
     */
    public static void main(String[] args) {
        Hassandra h = new Hassandra();
        System.exit(h.run(args));
    }

    public int run(String[] args) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int status = -1;
        try {
            if (args[0].equals(HDFS)) {
                status = ToolRunner.run(new HDFSMapRed(), args);
            } else if (args[0].equals(CASSANDRA)) {
                status = ToolRunner.run(new CassandraMapRed(), args);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        stopWatch.stop();
        log.info("response time: " + stopWatch.getTime());

        return status;
    }
}
