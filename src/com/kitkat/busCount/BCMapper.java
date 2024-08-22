package com.kitkat.busCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

public class BCMapper extends Mapper<Object, Text, Text, LongWritable> {

    private Text dayOfWeekKey = new Text();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",");

        if (columns.length < 7) {
            return;
        }

        String dateString = String.format("%04d-%02d-%02d",
                Integer.parseInt(columns[0]),
                Integer.parseInt(columns[1]),
                Integer.parseInt(columns[2]));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.parse(dateString, formatter);
        int dayOfWeek = date.get(ChronoField.DAY_OF_WEEK);

        dayOfWeekKey.set(String.valueOf(dayOfWeek));
        context.write(dayOfWeekKey, new LongWritable(Long.parseLong(columns[5])));
        context.write(dayOfWeekKey, new LongWritable(Long.parseLong(columns[6])));
    }
}
