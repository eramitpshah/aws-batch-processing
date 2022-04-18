import boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("employees")

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_filename = event["Records"][0]["s3"]["object"]["key"]
    resp = s3_client.get_object(Bucket=bucket_name,Key=s3_filename)
    data = resp["Body"].read().decode("utf-8")
    employees = data.split("\n")
    #print(employees)
    for emp in employees:
        #print(emp)
        emp_data = emp.split(",")
        # Add data into DynamoDb
        try:
            table.put_item(
                Item = {
                    "id":emp_data[0],
                    "firstname":emp_data[1],
                    "lastname":emp_data[2],
                    "gender":emp_data[3],
                    "place":emp_data[4]
                }
            )
        except Exception as e:
            print ("Exception occurred!")
        