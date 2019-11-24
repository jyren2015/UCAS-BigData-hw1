import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * @author RJY
 * @version 1.0.0
 * MapReduce Hw2Part1
 */
public class Hw2Part1 {

    public static class Calculating{//this class is used to calculating the average time and counts
        int count = 0;
        float totalTime = 0;
        private void count_times(Iterable<Text> valueIn){
            for(Text record : valueIn){
                String strRecord = record.toString();
                StringTokenizer partOfRecord = new StringTokenizer(strRecord);
                int times = Integer.valueOf(partOfRecord.nextToken());
                float time = Float.valueOf(partOfRecord.nextToken());
                totalTime = totalTime + time * times;
                count = count + times;
            }
        }
    }

    //This is the Mapper class
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
        private Text keyOut = new Text();
        private Text valueOut = new Text();
        private Boolean flag;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer record = new StringTokenizer(value.toString(), "\n");
            while(record.hasMoreTokens()){
                int recordArray_length;
                String oneRecord = record.nextToken();
                String[] recordArray = oneRecord.split(" ");
                recordArray_length = recordArray.length;
                //Check whether condition of format is met
                if(recordArray_length != 3){
                    continue;
                }

                String source = recordArray[0];
                String destination = recordArray[1];
                String time = recordArray[2];

                Pattern floatNum = Pattern.compile("^[0-9]*\\.?[0-9]+$");
                flag = floatNum.matcher(time).matches();
                if(!flag){
                    continue;
                }

                float floatTime = Float.valueOf(time);
                String str_key = source + " " + destination;
                String str_value = "1 " + Float.toString(floatTime);
                keyOut.set(str_key);
                valueOut.set(str_value);
                context.write(keyOut, valueOut);
            }
        }
    }

    public static class CountAndAvgCombiner extends Reducer<Text, Text, Text, Text>{
        private Text valueOut = new Text();
        public void reduce(Text keyIn, Iterable<Text> valueIn, Context context)throws IOException, InterruptedException{
            int count;
            float totalTime;

            Calculating calculating = new Calculating();
            calculating.count_times(valueIn);
            count = calculating.count;
            totalTime = calculating.totalTime;

            float avgTime;
            avgTime = totalTime/count;
            String str_count = Integer.toString(count);
            String str_avgTime = Float.toString(avgTime);
            String str_value = str_count + " " + str_avgTime;
            valueOut.set(str_value);
            context.write(keyIn, valueOut);
        }
    }

    //This is the Reducer class
    //Formalize the output
    public static class CountAndAvgReducer extends Reducer<Text, Text, Text, Text>{
        private Text result_key = new Text();
        private Text result_value = new Text();
        public void reduce(Text keyIn, Iterable<Text> valueIn, Context context)throws IOException, InterruptedException{
            int count;
            float totalTime;

            Calculating calculating = new Calculating();
            calculating.count_times(valueIn);
            count = calculating.count;
            totalTime = calculating.totalTime;

            float avgTime;
            avgTime = totalTime/count;
            double doubleTime = (double)avgTime;
            String str_count = Integer.toString(count);
            String result_avgTime = String.format("%.3f",doubleTime);
            //generate the result value
            String str_value = str_count + " " + result_avgTime;
            //result key is same as the keyIn
            result_key.set(keyIn);
            result_value.set(str_value);
            context.write(result_key, result_value);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: Count_And_Avg <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Count_And_Avg");
        job.setJarByClass(Hw2Part1.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(CountAndAvgCombiner.class);
        job.setReducerClass(CountAndAvgReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //add the input paths as given by command line
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        //add the output path as given by the command line
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}
