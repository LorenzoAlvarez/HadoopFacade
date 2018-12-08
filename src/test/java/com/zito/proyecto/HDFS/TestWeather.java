package com.zito.proyecto.HDFS;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Map/Reduce Test
 */
public class TestWeather 
{
	
	
	/**
	 * Mapper
	 * @author zito
	 *
	 */
	public static class MaxTemperatureMapper
		extends Mapper<LongWritable, Text, Text, IntWritable> {

		public MaxTemperatureMapper() {
			
		}
		
		private static final int MISSING = 9999;
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String year = line.substring(15, 19);
			int airTemperature;
			if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
				airTemperature = Integer.parseInt(line.substring(88, 92));
			} else {
				airTemperature = Integer.parseInt(line.substring(87, 92));
			}
			String quality = line.substring(92, 93);
			if (airTemperature != MISSING && quality.matches("[01459]")) {
				context.write(new Text(year), new IntWritable(airTemperature));
			}
		}
	}
	
	/**
	 * Reducer
	 * @author zito
	 *
	 */
	public static class MaxTemperatureReducer
		extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public MaxTemperatureReducer(){
			
		}
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
						throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(key, new IntWritable(maxValue));
		}
	}
	
	
    public static void main( String[] args )
    {
    	System.out.println( "---TEST---" );
    	String ip = "192.168.1.147";
    	String port = "8020";
    	System.setProperty("hadoop.home.dir", "/");
    	System.out.println("connecting to: " +"hdfs://"+ip+":"+port );

    	//System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    	try {
    		String user = "cloudera";
			FacadeHadoopFS facadeHDFS  = new FacadeHadoopFS(ip,port,user);
			FacadeHadoopJobs facadeJobs = new FacadeHadoopJobs(
			user,
			ip,
			"8021",
			ip,
			port); 
			System.out.println("/*****************************************************************/");
			System.out.println("connected");
			System.out.println("/*****************************************************************/");
			//TEST
			
			//Function:
			String localSrc1 = "/home/zito/Documentos/1901";
			String localSrc2 = "/home/zito/Documentos/1902";
			String localSrc3 = "/home/zito/Documentos/1903";
			String localSrc4 = "/home/zito/Documentos/1904";
			String localSrc5 = "/home/zito/Documentos/1905";
			
			String dst = "/user/cloudera/weather/";
			
			//remove output
			facadeHDFS.rm("/user/cloudera/salida/_SUCCESS");
			facadeHDFS.rm("/user/cloudera/salida/part-r-00000");
			facadeHDFS.rm("/user/cloudera/salida");
			
			//removes input
			facadeHDFS.rm(dst+"1901");
			facadeHDFS.rm(dst+"1902");
			facadeHDFS.rm(dst+"1903");
			facadeHDFS.rm(dst+"1904");
			facadeHDFS.rm(dst+"1905");
			facadeHDFS.rm(dst);
			
			//make the directory
			if (!facadeHDFS.test(dst))
				facadeHDFS.mkdir(dst);
			
			//copy the first file
			if (facadeHDFS.cpFromLocal(localSrc1, dst))
				System.out.println("File" + localSrc1 + " copied!");
			
			else
				System.out.println("File" + localSrc1 + " not copied, check your local path");
			
			//copy the second file
			if (facadeHDFS.cpFromLocal(localSrc2, dst))
				System.out.println("File" + localSrc2 + " copied!");
			
			else
				System.out.println("File" + localSrc2 + " not copied, check your local path");
			
			//copy the third file
			if (facadeHDFS.cpFromLocal(localSrc3, dst))
				System.out.println("File" + localSrc3 + " copied!");
			
			else
				System.out.println("File" + localSrc3 + " not copied, check your local path");
			
			//copy the fourth file
			if (facadeHDFS.cpFromLocal(localSrc4, dst))
				System.out.println("File" + localSrc4 + " copied!");
			
			else
				System.out.println("File" + localSrc4 + " not copied, check your local path");
			
			//copy the fifth file
			if (facadeHDFS.cpFromLocal(localSrc5, dst))
				System.out.println("File" + localSrc5 + " copied!");
			
			else
				System.out.println("File" + localSrc5 + " not copied, check your local path");
			
			//HERE START The map reduce job
			//facadeHDFS.sendJob("Trabajo prueba", MaxTemperatureMapper.class, MaxTemperatureReducer.class, Text.class, IntWritable.class, localSrc1, "weather");
			facadeJobs.summitJob("cloudera","TEST WEATHER", TestWeather.MaxTemperatureMapper.class, TestWeather.MaxTemperatureReducer.class, TestWeather.class, Text.class, IntWritable.class, dst, "salida");
/*			Job job = Job.getInstance(facadeHDFS.getConf());
			
			job.setJarByClass(TestWeather.class);
			job.setJobName("Max temperature");
			FileInputFormat.addInputPath(job,   new Path(localSrc1));
			FileOutputFormat.setOutputPath(job, new Path("Weather/salida"));
			job.setMapperClass(MaxTemperatureMapper.class);
			job.setReducerClass(MaxTemperatureReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
*/			
			
			/*
			 * 
			 * IMPORTANTE
			 * UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username);
				ugi.doAs(new PrivilegedExceptionAction<MyMapReduceWrapperClass>() {
    				public Object run() throws Exception {
        				MyMapReduceWrapperClass mr = new MyMapReduceWrapperClass();
        				ToolRunner.run(mr, null);
        				return mr;
    				}
				});
			 * 
			 */
			
			
			System.out.println("/*****************************************************************/");
			System.out.println("Exit");
			System.out.println("/*****************************************************************/");
			facadeHDFS.close();
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}

