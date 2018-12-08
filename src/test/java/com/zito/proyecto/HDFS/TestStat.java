package com.zito.proyecto.HDFS;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Hello world!
 *
 */
public class TestStat 
{
    public static void main( String[] args )
    {
    	System.out.println( "---TEST---" );
    	String ip = "192.168.1.140";
    	String port = "8020";
    	System.setProperty("hadoop.home.dir", "/");
    	System.out.println("connecting to: " +"hdfs://"+ip+":"+port );
    	//System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    	try {
			FacadeHadoopFS facadeHDFS = new FacadeHadoopFS(ip,port,"cloudera");
			System.out.println("connected");
			
			//TEST
			
			//Function:
			String src = "hola";
			
			FileStatus stats = facadeHDFS.stat(src);
			

			System.out.println(stats.getPath());
			System.out.println(stats.isFile());
			System.out.println(stats.getPermission());
			System.out.println(stats.getGroup());
			System.out.println(stats.getOwner());
			
			System.out.println("Exit");
			facadeHDFS.close();
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}

