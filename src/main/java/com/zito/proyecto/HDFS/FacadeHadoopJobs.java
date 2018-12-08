package com.zito.proyecto.HDFS;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

public class FacadeHadoopJobs {

	private Job job;
	
	public FacadeHadoopJobs (String mapRedJobTrackerIp,
			String mapRedJobTrackerPort,
			String fsDefaultNameIp,
			String fsDefaultNamePort
			)
			throws IOException, InterruptedException{
		
		//hacer el conf bien para crear el trabajo
		Configuration conf = new Configuration();
		
		String mapRedJobTracker = mapRedJobTrackerIp + ":"	+ mapRedJobTrackerPort;
		String fsDefault 		= "hdfs://" + fsDefaultNameIp +    ":"	+ fsDefaultNamePort;

		conf.set("mapreduce.jobtracker.address",	mapRedJobTracker);
		conf.set("fs.default.name",					fsDefault);
		
		job = null;
		try {
			job = Job.getInstance(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		/*
        Configuration conf = getConf();

        conf.set("mapred.job.tracker", "mapr-vm:9001");
        conf.set("fs.default.name", "maprfs://mapr-vm:7222/");
        conf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");

        Job job = new Job(conf, "Word count");
        job.setJarByClass(WordCount.class);
        */
	}
	
	
	public FacadeHadoopJobs(String user, String mapRedJobTrackerIp, String mapRedJobTrackerPort, String fsDefaultNameIp, String fsDefaultNamePort) {
		// TODO Auto-generated constructor stub
		UserGroupInformation ugi;
		try {
			ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
			
			ugi.doAs(new PrivilegedExceptionAction<Void>() {

				@Override
				public Void run() throws Exception {
					// TODO Auto-generated method stub
					
					Configuration conf = new Configuration();
					
					String mapRedJobTracker = mapRedJobTrackerIp + ":"	+ mapRedJobTrackerPort;
					String fsDefault 		= "hdfs://" + fsDefaultNameIp +    ":"	+ fsDefaultNamePort;

					conf.set("mapreduce.jobtracker.address",	mapRedJobTracker);
					conf.set("fs.default.name",					fsDefault);
					
					job = null;
					
						job = Job.getInstance(conf);
						
					
					return null;
				}
				
			});
		}
		catch (IOException | InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}


	public void summitJob(
			String user,
			String jobName,
			Class<? extends Mapper> 		mapper,
			Class<? extends Reducer> 		reducer,
			Class<?>				 		jobByClass,
			Class<?> 						outputKeyClass,
			Class<?> 						outputValueClass,
			String pathInput,
			String pathOutput)
	{	
	    UserGroupInformation ugi;
		try {
			ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
			
		    ugi.doAs(new PrivilegedExceptionAction<Void>() {
		  	  
		    	public Void run() throws Exception {
					//Submit a job
					System.out.println("**Setting Job's Name: "+ jobName);
					job.setJobName(jobName);
					
					System.out.println("**Setting job: "+ jobByClass);
					// here you have to set the jar which is containing your 
					// map/reduce class, so you can use the mapper class
					job.setJarByClass(jobByClass);
					
					
					System.out.println("**Setting Mapper: "+ mapper.getName());
					job.setMapperClass(mapper);
					System.out.println("**Setting Reducer: "+ reducer.getName());
					// here you have to put your reducer class
					job.setReducerClass(reducer);
					
					// key/value of your reducer output
					job.setOutputKeyClass(outputKeyClass);
					job.setOutputValueClass(outputValueClass);
					// this is setting the format of your input, can be TextInputFormat
					//job.setInputFormatClass(inputFormat);
					// same with output
					//job.setOutputFormatClass(outputFormat);
					// here you can set the path of your input
					try {
						SequenceFileInputFormat.addInputPath(job, new Path(pathInput));
					} catch (IllegalArgumentException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					Path out = new Path(pathOutput);
			/*		try {
						fs.delete(out, true);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			*/		// finally set the empty out path
					TextOutputFormat.setOutputPath(job, out);

					// this waits until the job completes and prints debug out to STDOUT or whatever
					// has been configured in your log4j properties.
					try {
						boolean success = job.waitForCompletion(true);
						//esto lo he puesto nuevo
						System.exit(success ? 0:1);
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}					
					
					return null;

		    	}
		    });
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}
	
	public void summitJoba(
			String jobName,
			Class<? extends Mapper> 		mapper,
			Class<? extends Reducer> 		reducer,
			Class<?>				 		jobByClass,
			Class<?> 						outputKeyClass,
			Class<?> 						outputValueClass,
			String pathInput,
			String pathOutput)
	{	
		
		System.out.println("**Setting Job's Name: "+ jobName);
		job.setJobName(jobName);
		
		System.out.println("**Setting job: "+ jobByClass);
		// here you have to set the jar which is containing your 
		// map/reduce class, so you can use the mapper class
		job.setJarByClass(jobByClass);
		
		
		System.out.println("**Setting Mapper: "+ mapper.getName());
		job.setMapperClass(mapper);
		System.out.println("**Setting Reducer");
		// here you have to put your reducer class
		job.setReducerClass(reducer);
		
		// key/value of your reducer output
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		// this is setting the format of your input, can be TextInputFormat
		//job.setInputFormatClass(inputFormat);
		// same with output
		//job.setOutputFormatClass(outputFormat);
		// here you can set the path of your input
		try {
			SequenceFileInputFormat.addInputPath(job, new Path(pathInput));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Path out = new Path(pathOutput);
/*		try {
			fs.delete(out, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
*/		// finally set the empty out path
		TextOutputFormat.setOutputPath(job, out);

		// this waits until the job completes and prints debug out to STDOUT or whatever
		// has been configured in your log4j properties.
		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	
	/*
	 * 	
	public void sendJob(String jobName, 
			Class<? extends Mapper> 		mapper,
			Class<? extends Reducer> 		reducer,
			Class<?> outputKeyClass,
			Class<?> outputValueClass,
			//Class<? extends InputFormat> 	inputFormat,
			//Class<? extends OutputFormat> 	outputFormat,
			String pathInput,
			String pathOutput
			){
		// create a new job based on the configuration
		Job job = null;
		try {
			job = Job.getInstance(fs.getConf());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//write the job name
		job.setJobName(jobName);
		// here you have to put your mapper class
		job.setMapperClass(mapper);
		// here you have to put your reducer class
		job.setReducerClass(reducer);
		// here you have to set the jar which is containing your 
		// map/reduce class, so you can use the mapper class
		job.setJarByClass(mapper);
		
		// key/value of your reducer output
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		// this is setting the format of your input, can be TextInputFormat
		//job.setInputFormatClass(inputFormat);
		// same with output
		//job.setOutputFormatClass(outputFormat);
		// here you can set the path of your input
		try {
			SequenceFileInputFormat.addInputPath(job, new Path(pathInput));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Path out = new Path(pathOutput);
		try {
			fs.delete(out, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// finally set the empty out path
		TextOutputFormat.setOutputPath(job, out);

		// this waits until the job completes and prints debug out to STDOUT or whatever
		// has been configured in your log4j properties.
		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}

	 * 
	 */
}
