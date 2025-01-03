package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class App implements RequestHandler<S3Event, String> {
    @Override
    public String handleRequest(S3Event event, Context context) {
            context.getLogger().log("Received event: " + event);
            // Get the S3 event details
            String bucketName = event.getRecords().get(0).getS3().getBucket().getName();
            String objectKey = event.getRecords().get(0).getS3().getObject().getKey();
            context.getLogger().log("Bucket: " + bucketName + ", Object: " + objectKey);

            context.getLogger().log("Making call to elastic beanstalk");

            String url = "http://texttopdfconverstion-env-1.eba-fm93rxki.us-east-1.elasticbeanstalk.com/?bucketName=" + bucketName + "&objectKey=" + objectKey;
            try{
                CloseableHttpClient client = HttpClients.createDefault();
                HttpPost httpPost = new HttpPost(url);
                String jsonString = "{\"bucketName\":\"" + bucketName + "\",\"objectKey\":\"" + objectKey + "\"}";
                httpPost.setEntity(new StringEntity(jsonString));
                httpPost.setHeader("Accept", "application/json");
                httpPost.setHeader("Content-type", "application/json");
                client.execute(httpPost);
            }
            catch(Exception e){
                context.getLogger().log("Error in making call to elastic beanstalk");
            }
           
        return "HTTP request sent to Elastic Beanstalk application";
    }
}