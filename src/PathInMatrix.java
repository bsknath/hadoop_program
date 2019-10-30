import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PathInMatrix {
    public static class PathFinderInMatrix extends Mapper < Object, Text, Text, DoubleWritable > {
        int m, n, path;
        int[] xny = new int[2];

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                for (int i = 0; i < 2; i++) {
                    xny[i] = Integer.parseInt(str);
                }
            }
            m = xny[0];
            n = xny[1];

            int[][] count = new int[m][n];

            for (int i = 0; i < m; i++)
                count[i][0] = 1;

            for (int j = 0; j < n; j++)
                count[0][j] = 1;

            for (int i = 1; i < m; i++) {
                for (int j = 1; j < n; j++)
                    count[i][j] = count[i - 1][j] + count[i][j - 1];
            }
            path =  count[m - 1][n - 1];
        }

        public void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            context.write(new Text("Path from (0,0) to (m-1,n-1): "), new  DoubleWritable(path));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Finding possible Path b/w (0,0) and (m-1,n-1)");
        job.setJarByClass(PathInMatrix.class);
        job.setMapperClass(PathFinderInMatrix.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
