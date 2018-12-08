package com.zito.proyecto.HDFS;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
public class TestAirQuality 
{
	
	
	/**
	 * Mapper
	 * @author zito
	 *
	 */
	  public static class AirQualityMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		 
		private static final String SEPARATOR = ";";
 
		/**
		 * DIA; CO (mg/m3);NO (ug/m3);NO2 (ug/m3);O3 (ug/m3);PM10 (ug/m3);SH2 (ug/m3);PM25 (ug/m3);PST (ug/m3);SO2 (ug/m3);PROVINCIA;ESTACIóN
		 * 01/01/1997; 1.2; 12; 33; 63; 56; ; ; ; 19 ;áVILA ;ávila
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			final String[] values = value.toString().split(SEPARATOR);
 
			// final String date = format(values[0]);
			final String co = format(values[1]);
			// final String no = format(values[2]);
			// final String no2 = format(values[3]);
			// final String o3 = format(values[4]);
			// final String pm10 = format(values[5]);
			// final String sh2 = format(values[6]);
			// final String pm25 = format(values[7]);
			// final String pst = format(values[8]);
			// final String so2 = format(values[9]);
			final String province = format(values[10]);
			// final String station = format(values[11]);
 
			if (NumberUtils.isNumber(co.toString())) {
				context.write(new Text(province), new DoubleWritable(NumberUtils.toDouble(co)));
			}
		}
 
		private String format(String value) {
			return value.trim();
		}
	}
	/**
	 * Reducer
	 * @author zito
	 *
	 */
	 public static class AirQualityReducer extends Reducer<Text, DoubleWritable, Text, Text> {
			 
			private final DecimalFormat decimalFormat = new DecimalFormat("#.##");
	 
			public void reduce(Text key, Iterable<DoubleWritable> coValues, Context context) throws IOException, InterruptedException {
				int measures = 0;
				double totalCo = 0.0f;
	 
				for (DoubleWritable coValue : coValues) {
					totalCo += coValue.get();
					measures++;
				}
	 
				if (measures > 0) {
					context.write(key, new Text(decimalFormat.format(totalCo / measures)));
	     		}
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
			String localSrc1 = "/home/zito/Documentos/Calidad_del_aire.csv";			
			String dst = "/user/cloudera/AIR/";
			
			//remove output
			facadeHDFS.rm("/user/cloudera/salida/_SUCCESS");
			facadeHDFS.rm("/user/cloudera/salida/part-r-00000");
			facadeHDFS.rm("/user/cloudera/salida");
			
			//removes input
			facadeHDFS.rm(dst+"Calidad_del_aire.csv");
			facadeHDFS.rm(dst);
			
			//make the directory
			
			if (!facadeHDFS.test(dst))
				facadeHDFS.mkdir(dst);
			
			//copy the file
			if (facadeHDFS.cpFromLocal(localSrc1, dst))
				System.out.println("File " + localSrc1 + " copied!");
			
			else
				System.out.println("File " + localSrc1 + " not copied, check your local path");
			
			
			/*
			 * 
			 * public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("AirQualityManager required params: {input file} {output dir}");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		final Job job = new Job(getConf());
		job.setJarByClass(AirQualityManager.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(AirQualityMapper.class);
		job.setReducerClass(AirQualityReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}
			 * 
			 */
			
			facadeJobs.summitJob("cloudera", "testAIR", TestAirQuality.AirQualityMapper.class,
					TestAirQuality.AirQualityReducer.class, TestAirQuality.class, Text.class, DoubleWritable.class, dst, "Salida");
			
			/*
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

