package org.chongwm.crest.kendra.transcribe;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
//https://www.youtube.com/watch?v=JeJ46YlpPqw
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.EventBridgeException;
import software.amazon.awssdk.services.transcribe.TranscribeClient;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.TranscribeException;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJob;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJobStatus;

/**
 * Handler value for Lambda execution
 */
public class TranscribeJobHandler implements RequestHandler<Map<String, Object>, Void>
{

	private static final String JOB_NAME_KEY = "JobName";
	private static final String DYNAMODB_TABLE_NAME = "transcribeJobs";
	private static final String DDB_CLIENT_SECRET = "ddbSecret";
	private static final String EVENT_BUS_RULE_NAME = "transcribe-job-completed-rule";
	private static final String EVENT_BUS_REGION = "eu-west-1";
	private static final String EVENT_BUS_CLIENT_SECRET = "ebSecret";


	private final TranscribeClient transcribeClient;
	private final EventBridgeClient eventBridgeClient;

	public TranscribeJobHandler()
	{
		transcribeClient = TranscribeClient.create();
		eventBridgeClient = EventBridgeClient.create();

	}
	
	
	public Void handleRequest(Map<String, Object> event, Context context) 
	{
        LambdaLogger logger = context.getLogger();
        logger.log("Lambda function triggered\n");

        String jobName = null;
        try 
        {// Extract job name from the EventBridge event
            Map<String, String> detail = (Map<String, String>) event.get("detail");
            jobName = detail.get("TranscriptionJobName");
            String jobStatus=detail.get("TranscriptionJobStatus");
            logger.log(jobName+" status is "+jobStatus+"\n");
            logger.log("Everything is in Context-Detail "+detail.toString()+"\n");

            // Describe the Transcribe job to get more details
            //DescribeTranscriptionJobRequest describeJobRequest = DescribeTranscriptionJobRequest.builder().transcriptionJobName(jobName).build();
            //TranscriptionJob transcribeJob = transcribeClient.describeTranscriptionJob();
            GetTranscriptionJobRequest r = GetTranscriptionJobRequest.builder().transcriptionJobName(jobName).build();
            TranscriptionJob transcriptionJob = transcribeClient.getTranscriptionJob(r).transcriptionJob();
            
            if (transcriptionJob.transcriptionJobStatus() == TranscriptionJobStatus.COMPLETED)
            {
                /*Map<String, Object> dynamoDbItem = new HashMap<>();
                dynamoDbItem.put("JobName", rr.transcriptionJob().transcriptionJobName());
                dynamoDbItem.put("Status", rr.transcriptionJob().transcriptionJobStatusAsString());

                ddbClient.putItem(r -> r.tableName(DYNAMODB_TABLE_NAME).item(dynamoDbItem));*/
            	
            	logger.log(jobName+" status is "+transcriptionJob.transcriptionJobStatusAsString());
                // Remove the event rule after processing
            	//eventBridgeClient.deleteRule(null);eventBridgeClient.deleteRule(r -> r.name(EVENT_BUS_RULE_NAME));
            	
            	if ((transcriptionJob.transcript() != null) && (transcriptionJob.transcript().transcriptFileUri() != null))
				{

					String transcriptS3Uri = transcriptionJob.transcript().transcriptFileUri();
					System.out.println("Transcript located at:" + transcriptS3Uri);
					String localTmpTranscriptFile="/tmp/transcribe/"+jobName;
					String transcriptJson = new String (urlToByteArray(transcriptS3Uri, localTmpTranscriptFile)); //TODO: Look for DynamoDB object where TranscribeJobName matches and write the output of urlToByteArray() to the object and mark status for Transcribe as COMPLETE
					//TODO: ActiveMQKendra code to be updated to read DynamoDB and forward to Kendra
				}            	
            }
            else
            {	
            	//TODO: Look for DynamoDB object where TranscribeJobName matches and mark status for Transcribe as FAILED.
            }
        } catch (EventBridgeException | TranscribeException | IOException e) {
            logger.log(e.getMessage());
            e.getStackTrace();
        }

        return null;
    }

	
	public byte[] urlToByteArray(String fileUrl, String localPath) throws IOException
	{
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		URL url = new URL(fileUrl);		
	    try 
	    {
			byte[] chunk = new byte[4096];
	        int bytesRead;

	        InputStream stream = url.openStream();

	        while ((bytesRead = stream.read(chunk)) > 0) 
	        {
	            outputStream.write(chunk, 0, bytesRead);
	        }

	    } 
	    catch (IOException e) 
	    {
	        e.printStackTrace();
	        return null;
	    }

	    return outputStream.toByteArray();
		
	}
	
	
	public static byte[] readInputStream(InputStream inputStream) throws IOException 
	{
		  ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		  int nRead;
		  byte[] data = new byte[16384];

		  while ((nRead = inputStream.read(data, 0, data.length)) != -1) 
		  {
		    buffer.write(data, 0, nRead);
		  }

		  buffer.flush();
		  return buffer.toByteArray();
	}
	
	
	public void downloadFile(String fileUrl, String localPath) throws IOException
	{
		URL url = new URL(fileUrl);
		InputStream inputStream = url.openStream();
		String dirOnly = localPath.substring(0, localPath.lastIndexOf('/'));
		Path dir = Paths.get(dirOnly);
		Files.createDirectories(dir);
		Files.copy(inputStream, Paths.get(localPath), StandardCopyOption.REPLACE_EXISTING);

		inputStream.close();
	}
	


}