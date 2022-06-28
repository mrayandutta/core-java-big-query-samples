package learningfromexperience.bqsamples;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.UUID;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

public class BQSamples {
	
	final static String SECRET_KEY_JSON_LOCATION = "/bq-poc-354616-cf6e8b041c4b.json";
	final static String PROJECT_ID = "bq-poc-354616";
	final static String DATASET_ID = "bg_test_dataset";
	
	public static void main(String[] args) throws IOException, GeneralSecurityException, InterruptedException {
		readData();
	}
	public static void readData() throws IOException, GeneralSecurityException, InterruptedException {
	    // Instantiate a client and specify credentials when constructing a client
	    GoogleCredentials credentials = GoogleCredentials.fromStream(BQSamples.class.getResourceAsStream(SECRET_KEY_JSON_LOCATION))
	            .createScoped(Collections.singleton("https://www.googleapis.com/auth/cloud-platform"));
	   
	   BigQuery bigquery = BigQueryOptions.newBuilder()
               .setProjectId(PROJECT_ID)
               .setCredentials(credentials)
               .build().getService();
	   
	    String query ="select * from `"+PROJECT_ID+"."+DATASET_ID+".employee`";
	    QueryJobConfiguration queryConfig =
	            QueryJobConfiguration.newBuilder(query)
	                .setUseLegacySql(false)
	                .build();

	        // Create a job ID so that we can safely retry.
	        
	        JobId jobId = JobId.of(UUID.randomUUID().toString());
	        System.out.println("jobId:"+jobId);
	        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
	        //Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
	        

	        // Wait for the query to complete.
	        queryJob = queryJob.waitFor();

	        // Check for errors
	        if (queryJob == null) {
	          throw new RuntimeException("Job no longer exists");
	        } else if (queryJob.getStatus().getError() != null) {
	          // You can also look at queryJob.getStatus().getExecutionErrors() for all
	          // errors, not just the latest one.
	          throw new RuntimeException(queryJob.getStatus().getError().toString());
	        }

	        // Get the results.
	        TableResult result = queryJob.getQueryResults();

	        // Print all pages of the results.
	        for (FieldValueList row : result.iterateAll()) {
	          // String type
	          String id = row.get(0).getStringValue();
	          String name = row.get(1).getStringValue();
	          System.out.println("id:"+ id+",name:"+name);
	        }
	   
	}

}
