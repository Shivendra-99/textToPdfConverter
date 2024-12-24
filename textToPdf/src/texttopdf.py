import json
import boto3
import urllib.parse
from fpdf import FPDF

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Log the event details
        print("Received event: " + json.dumps(event, indent=2))

        # Get the S3 event details
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        # Decode the object key
        object_key = urllib.parse.unquote_plus(object_key)

        print(f"Bucket name: {bucket_name}")
        print(f"Object key: {object_key}")

        # Download the file from S3
        print("Starting the download of the file from S3")
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = file_obj['Body'].read().decode('utf-8')

        print("Starting the conversion of the file to PDF")

        # Create a PDF from the text content
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)
        for line in file_content.splitlines():
            pdf.cell(200, 10, txt=line, ln=True)

        pdf_output = pdf.output(dest='S').encode('latin1')

        print("PDF conversion completed")

        # Define the new object key for the PDF
        pdf_key = object_key.replace('.txt', '.pdf')

        print("Uploading the PDF to S3")

        # Upload the PDF back to S3
        s3_client.put_object(Bucket=bucket_name, Key=pdf_key, Body=pdf_output, ContentType='application/pdf')

        print("PDF uploaded to S3 successfully")

        return {
            'statusCode': 200,
            'body': 'File converted and uploaded successfully'
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }