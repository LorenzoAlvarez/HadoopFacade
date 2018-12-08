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
public class Testmv 
{
    public static void main( String[] args )
    {
    	System.out.println( "---TEST---" );
    	String ip = "192.168.1.133";
    	String port = "8020";
    	System.setProperty("hadoop.home.dir", "/");
    	System.out.println("connecting to: " +"hdfs://"+ip+":"+port );
    	//System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    	try {
			FacadeHadoopFS facadeHDFS = new FacadeHadoopFS(ip,port,"cloudera");
			System.out.println("/*****************************************************************/");
			System.out.println("connected");
			System.out.println("/*****************************************************************/");
			//TEST
			
			//Function:
			String src = "hola";
			String dst = "h/movido";
			
			if (!facadeHDFS.test(src))
				facadeHDFS.touch(src);
			
			if (facadeHDFS.mv(src,dst))
				System.out.println("File " + src + " moved to -> " + dst);
			else
				System.out.println("File " + dst + " not moved corretly");
			
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

