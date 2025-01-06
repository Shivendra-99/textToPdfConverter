package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request().withBucketName(bucketName)
                    .withPrefix("output/");
            ListObjectsV2Result result = s3Client.listObjectsV2(listObjectsV2Request);
            // Will get the list of keys in the bucket
            List<String> keys = result.getObjectSummaries().stream().map(s3ObjectSummary -> s3ObjectSummary.getKey())
                    .collect(Collectors.toList());

            for (String key : keys) {
                if (key.equals("output/")) {
                    continue;
                }
                context.getLogger().log("Key: " + key);
                String KeyContains = key.split("/")[1];
                String objectKeyContains = objectKey.split("/")[1];

                if (KeyContains.startsWith(objectKeyContains.substring(0, 3))) {
                    return updateThePDFFile(key, context, bucketName, objectKey, response);
                }
            }

            // Download the file from S3
            context.getLogger().log("Starting the download of the file from S3");
            S3Object s3Object = s3Client.getObject(bucketName, objectKey);
            try (S3ObjectInputStream s3InputStream = s3Object.getObjectContent();
                 BufferedReader fileContent = new BufferedReader(new InputStreamReader(s3InputStream, StandardCharsets.UTF_8))) {

                context.getLogger().log("Starting the conversion of the file to PDF");

                // Create a PDF from the text content
                PDDocument document = new PDDocument();
                PDPage page = new PDPage();
                document.addPage(page);
                PDPageContentStream contentStream = new PDPageContentStream(document, page);
                contentStream.setFont(PDType1Font.HELVETICA, 12);
                float margin = 50;
                float yPosition = page.getMediaBox().getHeight() - margin;
                float leading = 14.5f;
                float startX = margin;
                float startY = yPosition;

                contentStream.beginText();
                contentStream.newLineAtOffset(startX, startY);
                String line;
                while ((line = fileContent.readLine()) != null) {
                    line = line.replace("\r", "");
                    if (yPosition <= margin) {
                        contentStream.endText();
                        contentStream.close();
                        page = new PDPage();
                        document.addPage(page);
                        contentStream = new PDPageContentStream(document, page);
                        contentStream.setFont(PDType1Font.HELVETICA, 12);
                        yPosition = page.getMediaBox().getHeight() - margin;
                        contentStream.beginText();
                        contentStream.newLineAtOffset(startX, startY);
                    }
                    contentStream.showText(line);
                    contentStream.newLineAtOffset(0, -leading);
                    yPosition -= leading;
                }
                contentStream.endText();
                contentStream.close();

                ByteArrayOutputStream pdfOutputStream = new ByteArrayOutputStream();
                document.save(pdfOutputStream);
                document.close();

                context.getLogger().log("PDF conversion completed");

                // Define the new object key for the PDF
                String pdfKey = objectKey.replace(".txt", ".pdf");
                pdfKey = pdfKey.replace("input", "output");

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
            }
        } catch (IOException e) {
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

    public Map<String, Object> updateThePDFFile(String outObject, Context context, String bucketName, String objectKey,
                                                Map<String, Object> response) {
        try {
            context.getLogger().log("inside update pdf method");

            // Reading the text file content from s3
            S3Object Texts3Objects = s3Client.getObject(bucketName, objectKey);
            try (S3ObjectInputStream s3InputStreams = Texts3Objects.getObjectContent();
                 BufferedReader fileContent = new BufferedReader(new InputStreamReader(s3InputStreams, StandardCharsets.UTF_8))) {

                context.getLogger().log("Text File content read successfully");

                // Downloading and Reading the PDF file content from s3
                context.getLogger().log("Starting the download of the PDF file from S3");
                S3Object PDFs3Object = s3Client.getObject(bucketName, outObject);
                try (S3ObjectInputStream s3InputStream = PDFs3Object.getObjectContent()) {
                    PDDocument document = PDDocument.load(s3InputStream);

                    // Updating the PDF content
                    context.getLogger().log("Starting the update of the PDF file");
                    PDPage page = new PDPage();
                    document.addPage(page);
                    PDPageContentStream contentStream = new PDPageContentStream(document, page);
                    contentStream.setFont(PDType1Font.HELVETICA, 12);
                    float margin = 50;
                    float yPosition = page.getMediaBox().getHeight() - margin;
                    float leading = 14.5f;
                    float startX = margin;
                    float startY = yPosition;
                    contentStream.beginText();
                    contentStream.newLineAtOffset(startX, startY);
                    String line;
                    while ((line = fileContent.readLine()) != null) {
                        line = line.replace("\r", "");
                        if (yPosition <= margin) {
                            contentStream.endText();
                            contentStream.close();
                            page = new PDPage();
                            document.addPage(page);
                            contentStream = new PDPageContentStream(document, page);
                            contentStream.setFont(PDType1Font.HELVETICA, 12);
                            yPosition = page.getMediaBox().getHeight() - margin;
                            contentStream.beginText();
                            contentStream.newLineAtOffset(startX, startY);
                        }
                        contentStream.showText(line);
                        contentStream.newLineAtOffset(0, -leading);
                        yPosition -= leading;
                    }
                    contentStream.endText();
                    contentStream.close();

                    ByteArrayOutputStream pdfOutputStream = new ByteArrayOutputStream();
                    document.save(pdfOutputStream);
                    document.close();

                    context.getLogger().log("PDF modification completed");

                    // Upload the updated PDF to S3
                    byte[] pdfBytes = pdfOutputStream.toByteArray();
                    ByteArrayInputStream pdfInputStream = new ByteArrayInputStream(pdfBytes);
                    ObjectMetadata metadata = new ObjectMetadata();
                    metadata.setContentLength(pdfBytes.length);
                    metadata.setContentType("application/pdf");
                    s3Client.putObject(bucketName, outObject, pdfInputStream, metadata);

                    context.getLogger().log("PDF uploaded to S3 successfully");

                    response.put("statusCode", 200);
                    response.put("body", "File updated and uploaded successfully");
                }
            }
        } catch (IOException e) {
            context.getLogger().log("Error during PDF conversion or upload from update pdf: " + e.getMessage());
            response.put("statusCode", 500);
            response.put("body", "Error from update pdf: " + e.getMessage());
        } catch (Exception e) {
            context.getLogger().log("General error from update pdf: " + e.getMessage());
            response.put("statusCode", 400);
            response.put("body", "Error from update pdf: " + e.getMessage());
        }
        return response;
    }
}