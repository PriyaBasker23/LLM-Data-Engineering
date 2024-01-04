import os
from io import StringIO 
import boto3
from PIL import Image
from io import BytesIO
from textractor import Textractor
from textractor.visualizers.entitylist import EntityList
from textractor.data.constants import TextractFeatures, Direction, DirectionalFinderType


def lambda_handler(event, context):
    s3 = boto3.resource('s3', region_name='eu-west-1')
    bucket = s3.Bucket("data-platform-firebreak-llm-unstructured")
    object = bucket.Object("data/doc.png")
    response = object.get()
    file_stream = response['Body']
    im = Image.open(file_stream)
    print(im)
    
    extractor = Textractor(region_name="eu-west-1")

    document = extractor.analyze_document(
        file_source=im,
        features=[TextractFeatures.TABLES],
        save_image=True
    )
    
    print(document)
    
    table = EntityList(document.tables[0])
    df=table[0].to_pandas()
    print(df)

# #   WRITE TO S3 BUCKET
#     with StringIO() as csv_buffer:
#         df.to_csv(csv_buffer, index=False)
    
#         s3_client = boto3.client('s3')
#         response = s3_client.put_object(
#             Bucket=bucket, Key="data/pdf_table.csv", Body=csv_buffer.getvalue()
#         )
        
#         status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
#         if status == 200:
#             print(f"Successful S3 put_object response. Status - {status}")
#         else:
#             print(f"Unsuccessful S3 put_object response. Status - {status}")

  