package com.zito.proyecto.HDFS;

import java.io.FileNotFoundException;
import java.io.FilePermission;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Clase Fachada para la implementación de operaciones
 * 	HDFS con guarana
 * @author Lorenzo Álvarez
 *
 */
public class FacadeHadoopFS {

	private FileSystem fs;
	
	//-metodos de la fachada-//
	
/**
 * Constructor class FacadeHadoopFs	
 * @throws InterruptedException 
 * @throws IOException 
 */
	public FacadeHadoopFS (String ip, String port, String user) throws IOException, InterruptedException{
		URI uri = null;
		uri = URI.create("hdfs://"+ip+":"+port);
		Configuration conf = new Configuration();
		System.out.println(uri.toString());
		
		fs = FileSystem.newInstance(uri, conf, user);
		
	}
	
/**
 * List the statuses of the
 *   files/directories in the given path
 * @param path name of the string
 * @return The filestatus of everyfile given the path
 */
	public FileStatus[] ls(String string){
		FileStatus[] listStatus = null;
		
		Path path = new Path(string);
		
		try {
			listStatus = fs.listStatus(path);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//funcion a añadir listStatus
		return listStatus;
	}
	
/**
 * Show disk usage given a path
 * @param path 
 * return Stadistics
 */
	public FsStatus du (){
		FsStatus fsStatus = null;
		/*
		public FsStatus getStatus()
	   			throws IOException
			Returns a status object describing the use and capacity of the file system. If the file system has multiple partitions, the use and capacity of the partition pointed to by the specified path is reflected.
			Parameters:
			Returns:
				a FsStatus object
			Throws:
				IOException - see specific implementation
		*/
		
		try {
			fsStatus = fs.getStatus();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fsStatus;
		
		
	}

/**
 * move a file given a path to the destiny
 * @param src
 * @param dst
 * @return true if success
 */
	public boolean mv(String src, String dst){
		boolean done = false;
		
		Path stringSrc = new Path(src);
		Path stringDst = new Path(dst);
		//copy		
		try {
			done = FileUtil.copy(fs, stringSrc, fs, stringDst, true, fs.getConf());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//delete file src
		return done;
	}
	
/**
 * copy a file or directory given a path to the destiny
 * @param src
 * @param dst
 * @return true if succes
 */
	public boolean cp(String src, String dst){
		boolean done = false;
		
		Path stringSrc = new Path(src);
		Path stringDst = new Path(dst);
		//copy		
		try {
			done = FileUtil.copy(fs, stringSrc, fs, stringDst, false, fs.getConf());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return done;
	}

	/**
	 * Remove a file from HDFS Given a path
	 * @param src
	 * @return true if success
	 */
	public boolean rm(String stringSrc){
		Path src = new Path(stringSrc);
		boolean done = false;
		try {
			done = fs.delete(src, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return done;
		
	}
	
	/**
	 * copy a file from the local File System to HDFS
	 * @param localSrc
	 * @param dst
	 * @return true if success
	 */
	public boolean cpFromLocal(String localSrc, String dst){
		boolean done = false;
		
		Path localSrcpath	= new Path(localSrc);
		Path dstpath	 	= new Path(dst);
		
		try {
			fs.copyFromLocalFile(localSrcpath, dstpath);
			done = true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return done;
		
	}
	
	public boolean cpToLocal(String src, String localDst){
		boolean done = false;
		
		Path srcPath			= new Path(src);
		Path localDstPath	 	= new Path(localDst);
		
		try {
			fs.copyToLocalFile(srcPath, localDstPath);
			done = true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return done;
	}
	
	/**
	 * move a file from the local file system to HDFS
	 * @param localSrc
	 * @param dst
	 * @return
	 */
	public boolean mvFromLocal(String localSrc, String dst){
		boolean done = false;
		
		Path localSrcpath	= new Path(localSrc);
		Path dstpath	 	= new Path(dst);
		
		try {
			fs.moveFromLocalFile(localSrcpath, dstpath);
			done = true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return done;
	}
	
	/**
	 * make a directory given a path
	 * @param dst
	 * @return true if success
	 */
	public boolean mkdir (String dstString){
		boolean done = false;
		Path dst = new Path(dstString);
		try {
			done = fs.mkdirs(dst);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
		
	}
	
	/**
	 * Changes the replication
	 *  factor of a file.
	 *  
	 *  If path is a directory then the command
	 *   recursively changes the replication
	 *   factor of all files under the directory
	 *   tree rooted at path.
	 */
	public void setrep(String src, short replication){
		Path stringSrc = new Path (src);
		
		try {
			fs.setReplication(stringSrc, replication);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Create a file given the path
	 * @param src
	 * @return
	 */
	public boolean touch (String stringSrc){
		boolean done = false;
		Path src = new Path(stringSrc);
		
		try {
			done = fs.createNewFile(src);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return done;
	}
	
	/**
	 * check if the path exist
	 * @param src
	 * @return
	 */
	public boolean test (String src){
		boolean done = false;
		
		Path pathSrc = new Path (src);
		try {
			fs.exists(pathSrc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
		
	}
	
	/**
	 * return all the information given a path
	 * @param src
	 * @return 
	 */
	public FileStatus stat (String src){
		Path stringSrc = new Path(src);
		
		try {
				return fs.getFileStatus(stringSrc);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return null;
	}
	
	/**
	 * returns the file given a path
	 * @param filename
	 * @return
	 */
/*
	public FSDataInputStream cat (String filename){
		
		Path filenameSrc = new Path(filename);
		
		try {
			return fs.open(filenameSrc);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
*/

	
	/**
	 * shows the last KB of a file given a path
	 * @param filename
	 */
/*
	public byte[] tail(String filename){
		
		Path stringFilename = new Path(filename);
		
		FileStatus stats = this.stat(filename);
		long size = stats.getLen();
		long position = size - 1024;
		FSDataInputStream file;
		byte[] out = null;
		try {
			file = fs.open(stringFilename);
			file.readFully(position, out);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return out; 
		
	}
	
*/	
	/**
	 * Changes the permissions given a path and the new permission
	 * @param src
	 * @param permission
	 */
	public void chmod(String src, String permission){
		
		Path stringSrc = new Path (src);
		FsPermission fsPermission = new FsPermission(permission);
		
		
		try {
			fs.setPermission(stringSrc, fsPermission);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Change user of a file given the user and the path to the file
	 * @param src
	 * @param username
	 */
	public void chown(String src, String username){
		
		Path stringSrc = new Path(src);
		
		FileStatus stats = this.stat(src);
		String group = stats.getGroup();
		try {
			fs.setOwner(stringSrc, username, group);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Change the group of a file given the groupuser and the path to the file
	 * @param src
	 * @param groupname
	 */
	public void chgrp(String src, String groupname){
		
		Path stringSrc = new Path(src);
		
		FileStatus stats = this.stat(src);
		String user = stats.getOwner();
		try {
			fs.setOwner(stringSrc, user, groupname);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Close the hdfs filesystem class
	 */
	public void close(){
		try {
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
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

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return fs.getConf();
	}
}
