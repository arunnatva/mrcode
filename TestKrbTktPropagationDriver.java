package com.arun.mr;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
//import org.apache.hadoop.security.Credentials;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hive.jdbc.HiveConnection;
//import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.log4j.Logger;



public class TestKrbTicketPropagationDriver extends Configured implements Tool {

	final static Logger logger = Logger.getLogger(TestKrbTicketPropagationDriver.class);
	
	public int run(String[] args) throws Exception {
		
		logger.info("run() : Starting");
		logger.info("Total Number of Arguments : " + args.length);
					
		Configuration conf = getConf();
		
		
		logger.info(" number of threads requested : " + conf.get("mapreduce.job.reduces"));
		logger.info("Application Name : " + args[1]);
		
		Job job = Job.getInstance(conf);
		job.setJobName("Test Krb job");
		
		//Pass the Configuration File on local fileSystem into Distributed Cache and it can be accessed in either Mapper and/or Reducer
		Configuration jobsConf = job.getConfiguration();
		
		//Inject the command line arguments into Hadoop's Configuration Object
				
		//Add the Configuration file into Distributed Cache
		addToDistributedCache(job);		
								
		job.setJarByClass(TestKrbTicketPropagationDriver.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(TestMapper.class);
			
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
				
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path("/tmp/alvout");
				
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		
		
		
		/*
		 * Delete output filepath if already exists
		 */
		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		//addHiveDelegationToken(jobsConf,job.getCredentials(), "jdbc:hive2://hostname:10000/default","hive/hostname@REALM.COM");
		return job.waitForCompletion(true) ? 0: 1;
		
	}
		
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		logger.info("TestKrbTktPropagationDriver.main() : Starting");
		
				
		TestKrbTicketPropagationDriver testKrbTicketPropagationDriver = new TestKrbTicketPropagationDriver();
		int res = ToolRunner.run(testKrbTicketPropagationDriver, args);
		System.exit(res);
		
	}

	/*
	public void addHiveDelegationToken(Configuration conf,Credentials creds, String url, String principal) throws Exception {
        
		Class.forName("org.apache.hive.jdbc.HiveDriver");

        Connection con = DriverManager.getConnection(url + ";principal=" + principal);
        // get delegation token for the given proxy user
        String tokenStr = ((HiveConnection) con).getDelegationToken(UserGroupInformation.getCurrentUser().getShortUserName(), principal);
        logger.info("current user : " + UserGroupInformation.getCurrentUser().getShortUserName() );
        
        //String tokenStr = ((HiveConnection) con).getDelegationToken("nbssnhsd", principal);
                
        con.close();

        Token<DelegationTokenIdentifier> hive2Token = new Token<DelegationTokenIdentifier>();
        hive2Token.decodeFromUrlString(tokenStr);
        creds.addToken(new Text("hive.server2.delegation.token"), hive2Token);
        creds.addToken(new Text(HiveAuthFactory.HS2_CLIENT_TOKEN), hive2Token);
        
    } */
	
	public void addToDistributedCache(Job job) {
			
		try {
														
			
			job.addCacheFile(new URI("/user/xxx/testmr/"+"alv_krbtkt_"+ System.getProperty("user.name")+"#krbtkt"));	
			job.addCacheFile(new URI("/user/xxxxx/testmr/gss-jaas.conf#gss-jaas"));
			
		} catch (URISyntaxException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
}
