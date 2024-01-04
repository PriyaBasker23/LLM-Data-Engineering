
import json
import boto3
import csv
from time import sleep

s3key_col = "file_location"
s3bucket = "data-platform-firebreak-llm-unstructured"
s3 = boto3.client('s3')



def textract_retrier(JobId):
    retries = 0
    textractmodule = boto3.client('textract' , region_name='eu-west-1')
    response = textractmodule.get_document_text_detection(
        JobId=JobId
    )
    if response["JobStatus"] == "SUCCEEDED":
        return response
    while retries < 6 and response["JobStatus"] != "SUCCEEDED":
        try:
            response = textractmodule.get_document_text_detection(
                JobId=JobId
            )
            retries += 1
            sleep(10)
        except Exception as e:
            print(e)
    return response

 
def textract(s3_key):
    textractmodule = boto3.client('textract', region_name='eu-west-1')
    response = textractmodule.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
            'Bucket': s3bucket,
            'Name': s3_key
            }
        }
    )
    text_response = textract_retrier(response["JobId"])
   
    text = []
    full_text=""
    try:
      blocks = text_response["Blocks"]
    except Exception as e:
        print(e)
        return str(text_response.keys())
    for block in text_response["Blocks"]:
        # print(block)
        if block["BlockType"] == "LINE":
            text.append({block["Text"]:str(block["Confidence"])})
            full_text+=block["Text"]+"\n"
    return text, str(full_text)


def lambda_handler(event, context):
   
    # filename=event['queryStringParameters']['filename']
    filename="data/economy.pdf"
    data,full_text=textract("data/"+filename)
    csv_file_path = '/tmp/output.csv'

    # Writing to CSV file
    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['Extracted_txt','Confidence'])
        for d in data:
            key, value = list(d.keys())[0], list(d.values())[0]
            csv_writer.writerow([key, value])

    # # Upload the CSV file to S3
    s3.upload_file(csv_file_path, s3bucket, 'output/'+filename+'.csv')
   
  #   Write the whole text in text file 
    output_file_name="/tmp/"+filename+".txt"
    with open(output_file_name, 'w') as file:
        file.write(full_text)

    # # Upload the CSV file to S3
    s3.upload_file(output_file_name, s3bucket, 'output_text/'+filename+'.txt')

  # trim to 2000 charcs
    text_trim = full_text[:1800]
    print(text_trim)
    payload = {
        'inputs':text_trim
    }

    # Create a SageMaker runtime client
    sm_runtime = boto3.client('sagemaker-runtime')

    # Convert the payload to JSON
    payload_json = json.dumps(payload)
    
    # used face-bart-text-summarisation model 
    endpoint_name="huggingface-pytorch-tgi-inference-2024-01-04-14-37-16-716"
    # Invoke the SageMaker endpoint
    response = sm_runtime.invoke_endpoint(
        EndpointName=endpoint_name,
        ContentType='application/json',
        Body=payload_json
    )

    # Parse the response from the SageMaker endpoint
    result = json.loads(response['Body'].read().decode())

    # Log the result (you can customize this based on your Lambda function's requirements)
    print("SageMaker Endpoint Response:", result)
    
    return {
        'statusCode': 200,
        'body': result
    }
   