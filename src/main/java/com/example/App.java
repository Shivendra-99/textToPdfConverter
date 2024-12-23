package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class App implements RequestHandler<S3Event, Map<String, Object>> {

    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final ObjectMapper objectMapper;

    public App() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JodaModule());
    }

    @Override
    public Map<String, Object> handleRequest(S3Event event, Context context) {
        Map<String, Object> response = new HashMap<>();
        try {
            // Log the event details
            String eventJson = objectMapper.writeValueAsString(event);
            context.getLogger().log("Received event: " + eventJson);

            // Get the S3 event details
            String bucketName = event.getRecords().get(0).getS3().getBucket().getName();
            String objectKey = event.getRecords().get(0).getS3().getObject().getKey();

            // Decode the object key
            objectKey = URLDecoder.decode(objectKey, StandardCharsets.UTF_8.name());

            context.getLogger().log("Bucket name: " + bucketName);
            context.getLogger().log("Object key: " + objectKey);

            // Download the file from S3
            context.getLogger().log("Starting the download of the file from S3");
            S3Object s3Object = s3Client.getObject(bucketName, objectKey);
            S3ObjectInputStream s3InputStream = s3Object.getObjectContent();
            String fileContent = new String(s3InputStream.readAllBytes(), StandardCharsets.UTF_8);

            context.getLogger().log("Starting the conversion of the file to PDF");

            // Create a PDF from the text content
            Document document = new Document();
            ByteArrayOutputStream pdfOutputStream = new ByteArrayOutputStream();
            PdfWriter.getInstance(document, pdfOutputStream);
            document.open();
            for (String line : fileContent.split("\n")) {
                document.add(new Paragraph(line));
            }
            document.close();

            context.getLogger().log("PDF conversion completed");

            // Define the new object key for the PDF
            String pdfKey = objectKey.replace(".txt", ".pdf");

            context.getLogger().log("Uploading the PDF to S3");

            // Upload the PDF back to S3 with content length
            byte[] pdfBytes = pdfOutputStream.toByteArray();
            ByteArrayInputStream pdfInputStream = new ByteArrayInputStream(pdfBytes);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(pdfBytes.length);
            metadata.setContentType("application/pdf");
            s3Client.putObject(bucketName, pdfKey, pdfInputStream, metadata);

            context.getLogger().log("PDF uploaded to S3 successfully");

            response.put("statusCode", 200);
            response.put("body", "File converted and uploaded successfully");
        } catch (IOException | DocumentException e) {
            context.getLogger().log("Error during PDF conversion or upload: " + e.getMessage());
            response.put("statusCode", 500);
            response.put("body", "Error: " + e.getMessage());
        } catch (Exception e) {
            context.getLogger().log("General error: " + e.getMessage());
            response.put("statusCode", 400);
            response.put("body", "Error: " + e.getMessage());
        }
        return response;
    }
}