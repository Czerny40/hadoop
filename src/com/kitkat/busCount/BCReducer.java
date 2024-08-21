package com.kitkat.busCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BCReducer extends Reducer<IntWritable, LongWritable, Text, LongWritable> {
    private static final String[] DAYS_OF_WEEK = {
            "None",
            "월",
            "화",
            "수",
            "목",
            "금",
            "토",
            "일"
    };

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Reducer<IntWritable, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long sum = 0;

        for (LongWritable val : values) {
            sum += val.get();
        }

        String dayOfWeekSting = convertDayOfWeek(key.get());
        context.write(new Text(dayOfWeekSting), new LongWritable(sum));
    }

    private String convertDayOfWeek(int dayOfWeek) {
        if (dayOfWeek < 1 || dayOfWeek > 7) {
            return "None";
        }
        return DAYS_OF_WEEK[dayOfWeek];
    }
}
