import json
import boto3
import codecs

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    read_bucket_name = 's3-lambda-dynamodb-visualization'
    write_bucket_name = 's3-lambda-dynamodb-autoupload'

    #original_key = '10Records.csv'
    original_key = unquote_plus(record['s3']['object']['key'])
    obj = s3.get_object(Bucket=read_bucket_name, Key=original_key)

    lines = []
    line_count = 0
    file_count = 0
    MAX_LINE_COUNT = 1000

    def create_split_name(file_count):
        return f'{original_key}-{file_count}'

    def create_body(lines):
        return ''.join(lines)

    for ln in codecs.getreader('utf-8')(obj['Body']):
        if line_count > MAX_LINE_COUNT:
            key = create_split_name(file_count)

            s3.put_object(
                Bucket=write_bucket_name,
                Key=key,
                Body=create_body(lines)
            )

            lines = []
            line_count = 0
            file_count += 1

        lines.append(ln)
        line_count += 1

    if len(lines) > 0:
        file_count += 1
        key = create_split_name(file_count)
        s3.put_object(
                Bucket=write_bucket_name,
                Key=key,
                Body=create_body(lines)
        )

    return {
        'statusCode': 200,
        'body': { 'file_count': file_count }
    }