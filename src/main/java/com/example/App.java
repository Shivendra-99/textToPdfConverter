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
import com.itextpdf.text.pdf.PdfCopy;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.text.pdf.parser.PdfTextExtractor;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

            // List the objects in the bucket
            ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request().withBucketName(bucketName)
                    .withPrefix("output/");
            ListObjectsV2Result result = s3Client.listObjectsV2(listObjectsV2Request);
            // Will get the list of keys in the bucket
            List<String> keys = result.getObjectSummaries().stream().map(s3ObjectSummary -> s3ObjectSummary.getKey())
                    .collect(Collectors.toList());

            for (String key : keys) {
                if(key.equals("output/")) {
                    continue;
                }
                context.getLogger().log("Key: " + key);
                String KeyContains=key.split("/")[1];
                String objectKeyContains=objectKey.split("/")[1];

                if (KeyContains.startsWith(objectKeyContains.substring(0, 3))) {
                    return updateThePDFFile(key, context, bucketName, objectKey, response);
                }
            }

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
             pdfKey = pdfKey.replace("input/", "output/");
            context.getLogger().log("Uploading the PDF to S3: " + pdfKey);

            // Upload the PDF back to S3 with content length
            byte[] pdfBytes = pdfOutputStream.toByteArray();
            ByteArrayInputStream pdfInputStream = new ByteArrayInputStream(pdfBytes);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(pdfBytes.length);
            metadata.setContentType("application/pdf");
            s3Client.putObject(bucketName, pdfKey, pdfInputStream, metadata);

            context.getLogger().log("PDF uploaded to S3 successfully");

            response.put("statusCode", 200);
            response.put("body", "File uploaded successfully");
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

    public Map<String, Object> updateThePDFFile(String outObject, Context context, String bucketName, String objectKey,
             Map<String, Object> response) {
        try {

            context.getLogger().log("inside update pdf method");

            // Reading the text file content from s3
            S3Object Texts3Objects = s3Client.getObject(bucketName, objectKey);
            S3ObjectInputStream s3InputStreams = Texts3Objects.getObjectContent();
            String TextfileContent = new String(s3InputStreams.readAllBytes(), StandardCharsets.UTF_8);
            context.getLogger().log("Text File content: " + TextfileContent);

            // Downloading and Reading the PDF file content from s3
            S3Object PDFs3Object = s3Client.getObject(bucketName, outObject);
            S3ObjectInputStream s3InputStream = PDFs3Object.getObjectContent();

            // Updating the PDF content
            context.getLogger().log("Updating pdf content " + new String(s3InputStream.readAllBytes(), StandardCharsets.UTF_8));
            PdfReader pdfReader = new PdfReader(s3InputStream);
            ByteArrayOutputStream pdfOutputStream = new ByteArrayOutputStream();
            Document document = new Document();
            PdfCopy copy = new PdfCopy(document, pdfOutputStream);
            document.open();
            copy.addDocument(pdfReader);
            document.newPage();
            for (String content : TextfileContent.split("\n")) {
                document.add(new Paragraph(content));
            }
            document.close();
            copy.close();
            pdfReader.close();

             // Extract and log the content of the merged PDF
            PdfReader mergedPdfReader = new PdfReader(new ByteArrayInputStream(pdfOutputStream.toByteArray()));
            StringBuilder pdfContent = new StringBuilder();
            for (int i = 1; i <= mergedPdfReader.getNumberOfPages(); i++) {
                pdfContent.append(PdfTextExtractor.getTextFromPage(mergedPdfReader, i));
            }
            mergedPdfReader.close();
            context.getLogger().log("Merged PDF content: " + pdfContent.toString());

            context.getLogger().log("PDF content update completed and uploading pdf to s3" );

            // Uploading the updated PDF to S3
            byte[] pdfBytes = pdfOutputStream.toByteArray();
            ByteArrayInputStream pdfInputStream = new ByteArrayInputStream(pdfBytes);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(pdfBytes.length);
            metadata.setContentType("application/pdf");
            s3Client.putObject(bucketName, outObject, pdfInputStream, metadata);
            response.put("statusCode", 200);
            response.put("body", "File updated and uploaded successfully"+pdfOutputStream.toByteArray().toString());
        } catch (IOException | DocumentException e) {
            context.getLogger().log("Error during PDF conversion or upload from update pdf: " + e.getMessage());
            response.put("statusCode", 500);
            response.put("body", "Error from update pdf : " + e.getMessage());
        } catch (Exception e) {
            context.getLogger().log("General error from update pdf: " + e.getMessage());
            response.put("statusCode", 400);
            response.put("body", "Error from update pdf: " + e.getMessage());
        }
        return response;
    }
}