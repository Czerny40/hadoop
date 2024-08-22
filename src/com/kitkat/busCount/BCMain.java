package com.kitkat.busCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BCMain {
    public static void main(String[] args) throws Exception {
//    21 ~ 23년도 버스데이터를 가지고
//    요일별 승차인원과 하차인원 수를 구해서
//    txt 파일로 만들기
//    1. 년도별 버스데이터를 불러오기
//    2. 연,월,일,버스번호,정류장,승차인원,하차인원 형태의 csv파일 3개
//    3. 요일 컬럼은 없는데 어떻게 설정해줄지
//    4. 모두 더해서
//    5. txt 파일로 생성
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setMapperClass(BCMapper.class);
        job.setCombinerClass(BCReducer.class);
        job.setReducerClass(BCReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        String fileName = null;
        for (int i = 2021; i <= 2023; i++) {
            fileName = String.format("/busData%d.csv", i);
            FileInputFormat.addInputPath(job, new Path(fileName));
        }
        FileOutputFormat.setOutputPath(job, new Path("/busResult"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}