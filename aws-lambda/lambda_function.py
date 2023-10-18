import urllib.parse
import boto3

from datetime import datetime

from synop2bufr import transform as transform_synop

print('Loading function')
s3 = boto3.client('s3')


def handler(event, context):

    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8') # noqa
    size = event['Records'][0]['s3']['object']['size']
    print("object="+key+" received with size="+str(size))
    if size == 0:
        print("object="+key+" size=0, don't process !")
        return 0

    filename = key.split('/')[-1]
    foldername = key.replace(filename, '')

    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")

    # TODO: extract year and month from the file name
    year_utc = datetime.utcnow().year
    month_utc = datetime.utcnow().month

    # TODO: read the metadata file from S3
    metadata_file = open('/function/station_list.csv', 'r')

    nbufr_created = 0
    bufr_generator = transform_synop(
        body,
        metadata_file.read(),
        year_utc,
        month_utc
    )
    for item in bufr_generator:
        if 'bufr4' in item and item['bufr4'] is not None:
            identifier = item['_meta']['id']
            print('identifier='+identifier)
            s3.put_object(
                Bucket='wis2box-public',
                Key=foldername+identifier+'.bufr4',
                Body=item['bufr4']
            )
            nbufr_created += 1
        else:
            print('No BUFR message created for '+item['_meta']['id'])
    print('Created '+str(nbufr_created)+' BUFR messages')

    return 0
