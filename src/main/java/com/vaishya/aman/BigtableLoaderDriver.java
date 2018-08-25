package com.vaishya.aman;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BigtableLoaderDriver extends Configured implements Tool {
        /*
        args[0] = Path from where data is to be imported
        args[1] = Name of the table
        * */
        public final static Logger LOG = LoggerFactory.getLogger(BigtableLoaderDriver.class);
        public static String[] columns;


        public int run(String[] args) throws Exception  {

            Configuration conf = HBaseConfiguration.create();

            for (int i = 0; i < columns.length; ++i) {

                LOG.info("column" + i + " " + columns[i]);
                conf.set("column" + i, columns[i]);
            }
            conf.set("mapred.job.queue.name", "long_running_2");
            conf.set("COLUMN_FAMILY",columns[0]);
            TableName tableName = TableName.valueOf(args[1]);
            Job job = Job.getInstance(conf, "BigTableLoader");
            job.setInputFormatClass(TextInputFormat.class);
            job.setNumReduceTasks(0);
            TextInputFormat.addInputPath(job, new Path(args[0]));
            job.setJarByClass(BigtableLoaderDriver.class);
            job.setMapperClass(CSVToHBaseLoaderMapper.TokenizerMapper.class);
            TableMapReduceUtil.initTableReducerJob(tableName.getNameAsString(), null, job);
            return job.waitForCompletion(true) ? 0 : 1;
        }

        public static void main(String[] args) throws Exception {
            int res = ToolRunner.run(new Configuration(), new BigtableLoaderDriver(), args);
            //GenericOptionsParser opts = new GenericOptionsParser();
            System.exit(res);
        }

    }