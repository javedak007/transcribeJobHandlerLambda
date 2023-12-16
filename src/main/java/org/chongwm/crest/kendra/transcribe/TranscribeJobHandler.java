package org.chongwm.crest.kendra.transcribe;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;  
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
//https://www.youtube.com/watch?v=JeJ46YlpPqw
import java.util.Map;

import javax.net.ssl.SSLContext;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import software.amazon.awssdk.core.ResponseBytes; 
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.kendra.KendraClient;
import software.amazon.awssdk.services.kendra.model.BatchPutDocumentRequest;
import software.amazon.awssdk.services.kendra.model.BatchPutDocumentResponse;
import software.amazon.awssdk.services.kendra.model.ContentType;
import software.amazon.awssdk.services.kendra.model.Document;
import software.amazon.awssdk.services.kendra.model.DocumentAttribute;
import software.amazon.awssdk.services.kendra.model.DocumentAttributeValue;
import software.amazon.awssdk.services.kendra.model.Principal;
import software.amazon.awssdk.services.kendra.model.PrincipalType;
import software.amazon.awssdk.services.kendra.model.ReadAccessType;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.transcribe.TranscribeClient;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.Tag; 
import software.amazon.awssdk.services.transcribe.model.TranscriptionJob;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJobStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
/**
 * Handler value for Lambda execution
 */
public class TranscribeJobHandler implements RequestHandler<Map<String, Object>, Void>
{


	private final DynamoDbClient dynamoDbClient;
	private final TranscribeClient transcribeClient;
	LambdaLogger logger ;
	public TranscribeJobHandler()
	{
		transcribeClient = TranscribeClient.create();
		dynamoDbClient = DynamoDbClient.create();
		//EventBridgeClient.create();

	}

	public Void handleRequest(Map<String, Object> event, Context context)
	{
		logger = context.getLogger();
		logger.log("Lambda function triggered\n");

		String jobName = null;
		try
		{// Extract job name from the EventBridge event
			Map<String, String> detail = (Map<String, String>) event.get("detail");
			jobName = detail.get("TranscriptionJobName");
			String jobStatus = detail.get("TranscriptionJobStatus");
			logger.log(jobName + " status is " + jobStatus + "\n");
			logger.log("Everything is in Context-Detail " + detail.toString() + "\n");

			// Describe the Transcribe job to get more details
			// DescribeTranscriptionJobRequest describeJobRequest = DescribeTranscriptionJobRequest.builder().transcriptionJobName(jobName).build();
			// TranscriptionJob transcribeJob = transcribeClient.describeTranscriptionJob();
			GetTranscriptionJobRequest r = GetTranscriptionJobRequest.builder().transcriptionJobName(jobName).build();
			TranscriptionJob transcriptionJob = transcribeClient.getTranscriptionJob(r).transcriptionJob();


					String tableName = "";
					String nodeId = "";
					String jsonPayloadString = "";

					for (int i = 0; i < transcriptionJob.tags().size(); i++) {
						Tag e = transcriptionJob.tags().get(i);
						if(e.key().equals("nodeId"))
							nodeId = e.value();
						else if(e.key().equals("tableName"))
							tableName = e.value();
						/*else if(e.key().equals("bucket"))
							bucket = e.value();*/
					} 

					if(nodeId.isEmpty() || tableName.isEmpty())
					return null;

					GetItemRequest itemReq = GetItemRequest.builder().tableName(tableName).key(Collections.singletonMap("nodeId", AttributeValue.builder().s(nodeId).build())).build();
					 
					Map<String,AttributeValue> returnedItem = dynamoDbClient.getItem(itemReq).item();
					if(returnedItem != null){
						jsonPayloadString = returnedItem.get("jsonPayload").s();
					}

					if(jsonPayloadString.isEmpty())
					return null;

					Map<String, AttributeValue> attributeMap = new HashMap<>(); 
					attributeMap.putAll(returnedItem);

			if (transcriptionJob.transcriptionJobStatus() == TranscriptionJobStatus.COMPLETED)
			{ 

				if ((transcriptionJob.transcript() != null) && (transcriptionJob.transcript().transcriptFileUri() != null))
				{ 

					JsonObject jsonPayload = JsonParser.parseString(jsonPayloadString).getAsJsonObject();

					//String transcriptS3Uri = transcriptionJob.transcript().transcriptFileUri();
					//System.out.print("Transcript located at:" + transcriptS3Uri + "."); 
					
					/*String transcriptJson = null;
					if (transcriptS3Uri.contains("X-Amz-Security-Token"))
					{
						System.out.println();
						String localTmpTranscriptFile = "/tmp/transcribe/" + jobName;
						transcriptJson = new String(urlToByteArray(transcriptS3Uri, localTmpTranscriptFile)); // TODO: Look for DynamoDB object where TranscribeJobName matches and write the output of urlToByteArray() to the object and mark status for Transcribe as COMPLETE
					} else
					{
						System.out.println(" Retrieve it yourself.");
						S3Client sc = S3Client.create();
						ResponseBytes<GetObjectResponse> response = sc.getObjectAsBytes(GetObjectRequest.builder().bucket("").key("").build()); // TODO Send transcription back to Alfresco as rendition, and send bytestream to Kendra for indexing
						transcriptJson = response.asByteArray().toString();
					}
					// TODO: ActiveMQKendra code to be updated to read DynamoDB and forward to Kendra
					//TODO handle BatchPutDocumentResponse.*/

					S3Client sc = S3Client.create();
					ResponseBytes<GetObjectResponse> response = sc.getObjectAsBytes(GetObjectRequest.builder().bucket(jsonPayload.get("lambdaBucket").getAsString()).key(jobName + ".json").build()); // TODO Send transcription back to Alfresco as rendition, and send bytestream to Kendra for indexing
					String transcriptJsonString = response.asString(Charset.defaultCharset()); 
					JsonObject transcriptJson = JsonParser.parseString(transcriptJsonString).getAsJsonObject();
					JsonArray transcriptsArray = transcriptJson.get("results").getAsJsonObject().get("transcripts").getAsJsonArray(); 
					ArrayList<String> finalContentArray = new ArrayList<>();

					transcriptsArray.forEach(t -> { 
						 finalContentArray.add(t.getAsJsonObject().get("transcript").getAsString());
					}); 
  
					sendToKendra(String.join("\n", finalContentArray),jsonPayload); 
					updateAlfrescoProperties(String.join("\n", finalContentArray),jsonPayload,nodeId);
  
					attributeMap.put("jsonPayload", AttributeValue.builder().s("").build());
					attributeMap.put("kendraAction",AttributeValue.builder().s("Node Created").build());
					attributeMap.put("kendraStatus",AttributeValue.builder().s("success").build());
					PutItemRequest pit = PutItemRequest.builder().tableName(tableName).item(attributeMap).build();
					dynamoDbClient.putItem(pit); 
					
				}
			} else
			{   
				    attributeMap.put("jsonPayload", AttributeValue.builder().s("").build());
					attributeMap.put("kendraAction",AttributeValue.builder().s("Node Created").build());
					attributeMap.put("kendraStatus",AttributeValue.builder().s("failed").build()); 
					attributeMap.put("kendraError",AttributeValue.builder().s(transcriptionJob.failureReason()+"").build());
					PutItemRequest pit = PutItemRequest.builder().tableName(tableName).item(attributeMap).build();
					dynamoDbClient.putItem(pit);
			}
		} catch (Exception  e)
		{
			logger.log(e.getMessage()); 
			//failedReasonToDynamo(tableName, attributeMap, e.getMessage()+"");
			//e.printStackTrace();
		}  	

		return null;
	} 
	 

	protected String AlfrescoNodeMetadaFromDynamoDB()
	{
		return null;
	}
	protected BatchPutDocumentResponse sendToKendra(String transcript,JsonObject jsonPayload) throws Exception
	{

		// JsonObject jsonResource = jsonPayload.getAsJsonObject("data").getAsJsonObject("resource");
		// ArrayList<DocumentAttribute> docAttr = CommonConst.generateDefaultAttribute(jsonResource);
		// ArrayList<Principal> docPrinciple = getUserPermissionFromNode(jsonPayload);
		// TODO: Above is to get Alfresco node's metadata and permissions

		String title = jsonPayload.getAsJsonObject("data").getAsJsonObject("resource").get("name").getAsString();
		String nodeId = jsonPayload.getAsJsonObject("data").getAsJsonObject("resource").get("id").getAsString();
		
		//String mimeType = "text/plain";
		 
		Collection<Principal> docPrinciple = getUserPermissionFromNode(jsonPayload); 
		String kendraIndexId = jsonPayload.get("lambdaKendraIndexId").getAsString();  

		JsonObject jsonResource = jsonPayload.getAsJsonObject("data").getAsJsonObject("resource"); 

		ArrayList<DocumentAttribute> docAttr = genrateDefaultAttribute(jsonResource,jsonPayload.get("lambdaRepoId").getAsString(),jsonPayload.get("lambdaDataSourceId").getAsString(),jsonPayload.get("lambdaDigiWorksUrl").getAsString()); 

		Document newDoc = Document.builder().title(title).id(nodeId).blob(SdkBytes.fromString(transcript,Charset.defaultCharset())).contentType(ContentType.PLAIN_TEXT).attributes(docAttr).accessControlList(docPrinciple).build();

		BatchPutDocumentRequest batchPutDocumentRequest = BatchPutDocumentRequest.builder().indexId(kendraIndexId).documents(newDoc).build();

		// ActiveMQMessageRoute.kendra.batchPutDocument(batchPutDocumentRequest);
		KendraClient kc = KendraClient.create();
		//kc.batchPutDocument(batchPutDocumentRequest);
		return kc.batchPutDocument(batchPutDocumentRequest);
		// System.out.println(String.format("BatchPutDocument Result: %s", result));

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

		} catch (IOException e)
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

	public ArrayList<DocumentAttribute> genrateDefaultAttribute(JsonObject jsonResource,String repoId , String dataSourceId,String digiWorksUrl) throws Exception{
        ArrayList<DocumentAttribute> docAttr = new ArrayList<DocumentAttribute>();

       // JsonObject jsonResource = jsonPayload.getAsJsonObject("data").getAsJsonObject("resource"); 
       String createdAt = jsonResource.get("createdAt").getAsString();
       String modifiedAt = jsonResource.get("modifiedAt").getAsString();

       if(createdAt.contains("+"))
         createdAt = StringUtils.substringBefore(createdAt,"+") + "Z";

       if(modifiedAt.contains("+"))
         modifiedAt = StringUtils.substringBefore(modifiedAt,"+") + "Z";

        docAttr.add( DocumentAttribute.builder().key("_created_at").
            value(DocumentAttributeValue.fromDateValue(Instant.parse(createdAt))).build()); 
        docAttr.add( DocumentAttribute.builder().key("_last_updated_at").
            value(DocumentAttributeValue.fromDateValue(Instant.parse(modifiedAt))).build()); 

        String modfiedDateOnly = jsonResource.get("modifiedAt").getAsString();
        modfiedDateOnly = StringUtils.substringBefore(modfiedDateOnly,"T") + "T00:00:00.000Z";

        //System.out.println(modfiedDateOnly);

        docAttr.add( DocumentAttribute.builder().key("last_updated_date_only").
            value(DocumentAttributeValue.fromDateValue(Instant.parse(modfiedDateOnly))).build()); 


        docAttr.add( DocumentAttribute.builder().key("createdByUser").
            value(DocumentAttributeValue.fromStringValue(jsonResource.getAsJsonObject("createdByUser").get("id").getAsString())).build()); 
       
        docAttr.add( DocumentAttribute.builder().key("modifiedByUser").
            value(DocumentAttributeValue.fromStringValue(jsonResource.getAsJsonObject("modifiedByUser").get("id").getAsString())).build()); 
       

		docAttr.add( DocumentAttribute.builder().key("_source_uri").
            value(DocumentAttributeValue.fromStringValue(digiWorksUrl +"/#/documents/document-view/"+jsonResource.get("id").getAsString())).build()); 
    
  
       // docAttr.add( DocumentAttribute.builder().key("_source_uri").
        //    value(DocumentAttributeValue.fromStringValue(repoId)).build()); 

        docAttr.add( DocumentAttribute.builder().key("repo_id").
            value(DocumentAttributeValue.fromStringValue(repoId)).build()); 

		docAttr.add( DocumentAttribute.builder().key("al_repository_id").
            value(DocumentAttributeValue.fromStringValue(repoId)).build());  
			
            
        docAttr.add( DocumentAttribute.builder().key("_data_source_id").
            value(DocumentAttributeValue.fromStringValue(dataSourceId)).build()); 

		/* if(!Configuration.dataSourceExecutionId.isEmpty())
			docAttr.add( DocumentAttribute.builder().key("_data_source_sync_job_execution_id").
				value(DocumentAttributeValue.fromStringValue(Configuration.dataSourceId)).build());  */

        docAttr.add( DocumentAttribute.builder().key("_version").
            value(DocumentAttributeValue.fromStringValue(
                jsonResource.getAsJsonObject("properties").has("cm:versionLabel") ?
                jsonResource.getAsJsonObject("properties").get("cm:versionLabel").getAsString() : "v1"
                )).build()); 


		if(jsonResource.getAsJsonObject("properties").has("cm:author"))
          docAttr.add( DocumentAttribute.builder().key("_authors").
            value(DocumentAttributeValue.fromStringListValue( 
                Collections.singletonList(jsonResource.getAsJsonObject("properties").get("cm:author")+"") 
                )).build()); 

        if(jsonResource.getAsJsonObject("properties").has("cm:description"))
        docAttr.add( DocumentAttribute.builder().key("al_document_description").
            value(DocumentAttributeValue.fromStringValue( 
                jsonResource.getAsJsonObject("properties").get("cm:description")+"" 
                )).build()); 


        docAttr.add( DocumentAttribute.builder().key("al_document_title").
            value(DocumentAttributeValue.fromStringValue(jsonResource.get("name").getAsString())).build()); 
        docAttr.add( DocumentAttribute.builder().key("al_document_size").
            value(DocumentAttributeValue.fromLongValue(jsonResource.getAsJsonObject("content").get("sizeInBytes").getAsLong())).build()); 
       
        return docAttr;
    }

	
    ArrayList<Principal> getUserPermissionFromNode(JsonObject jsonPayload) throws Exception{ 

    ArrayList<Principal> docPrincipals = new ArrayList<Principal>(); 

    ArrayList<String> grpList =  new ArrayList<String>(Arrays.asList(jsonPayload.get("lambdaGrpList").getAsString().split(",")));
    ArrayList<String> userList = new ArrayList<String>(Arrays.asList(jsonPayload.get("lambdaUserList").getAsString().split(",")));
 

    for(String grp : grpList){
		if(!grp.isEmpty()){
        docPrincipals.add( Principal.builder()
        .access(ReadAccessType.ALLOW)
        .type(PrincipalType.GROUP)
        .name(grp)
        .build());
		}
    } 
    
    for(String name : userList){
		if(!name.isEmpty()){
        docPrincipals.add( Principal.builder()
        .access(ReadAccessType.ALLOW)
        .type(PrincipalType.USER)
        .name(name)
        .build());
	 }
    } 

    return docPrincipals;

    }


	void updateAlfrescoProperties(String str,JsonObject jsonPayload,String nodeId){
		try {

			String url = jsonPayload.get("lambdaAfrescoUrl").getAsString();
			String authorization = jsonPayload.get("lambdaAuthorization").getAsString(); 

			SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, (chain, authType) -> true).build(); // SSL context that trusts all certificates

		
			HttpUriRequest request1 = RequestBuilder.put()
              .setUri(url+"/alfresco/api/-default-/public/alfresco/versions/1/nodes/"+nodeId)
              .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
              .setHeader("Authorization", authorization)
              .setEntity(new StringEntity("{ \"properties\":{ \"crestAiMl:transcription\":\""+str+"\"}}"))
              .build();

			//CloseableHttpClient httpclient1 = HttpClients.createDefault();
			CloseableHttpClient  httpclient1 = HttpClients.custom().setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build()).setSSLContext(sslContext).setSSLHostnameVerifier((hostname, session) -> true).build();
  
         	CloseableHttpResponse response1 = httpclient1.execute(request1);

			logger.log("property update "+ response1.getStatusLine().getStatusCode()); 
			 
			
		} catch (Exception e) { 
			logger.log(e.getMessage()+"");
		}
		

	}


	 

}