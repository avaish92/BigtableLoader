package com.vaishya.aman;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CSVToHBaseLoaderMapper {
    private final static Log LOG = LogFactory.getLog(CSVToHBaseLoaderMapper.class);


    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, Put> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] fields = value.toString().split("\t");
            try {
                for (int i = 1; i < fields.length; ++i) {
                    if (!(fields[i].isEmpty())) {
                        Put put = new Put(fields[0].getBytes());
                        String col = conf.get("column" + (i));
                        put.addColumn(conf.get("COLUMN_FAMILY").getBytes(), Bytes.toBytes(col), Bytes.toBytes(fields[i]));
                        context.write(null,put);
                    }
                }
            } catch (Exception ex) {
                LOG.fatal(ex);
                context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
                return;
            }
        }
    }
}
