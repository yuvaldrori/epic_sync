import json
import urllib2
import boto3
import botocore


def main():
    BUCKET = 'epic-archive-mirror'
    API = 'http://epic.gsfc.nasa.gov/api'
    ARCHIVE = 'http://epic.gsfc.nasa.gov/epic-archive'

    def get_available_dates():
        data = urllib2.urlopen(
            '{endpoint}/images.php?available_dates'.format(
                endpoint=API))
        return json.loads(data.read())

    def get_list_by_date(date):
        data = urllib2.urlopen(
            '{endpoint}/images.php?date={date}'.format(
                endpoint=API, date=date))
        return json.loads(data.read())

    def get_images_names_by_date(image_list):
        images = []
        for l in image_list:
            images.append(l['image'])
        return images

    def get_image_data(image_path):
        data = urllib2.urlopen(image_path)
        return data

    def list_exists(date):
        client = boto3.client('s3')
        try:
            client.head_object(
                Bucket=BUCKET,
                Key='images_{date}.json'.format(
                    date=date))
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return False
        return True

    def upload_file(data, key):
        client = boto3.client('s3')
        client.put_object(
            Body=data.read(),
            Bucket=BUCKET,
            Key=key,
            ContentType=data.info()['Content-type'])

    dates = get_available_dates()
    for date in dates:
        if not list_exists(date):
            image_list = get_list_by_date(date)
            images = get_images_names_by_date(image_list)
            for image in images:
                thumb = '{endpoint}/thumbs/{image}.jpg'.format(
                    endpoint=ARCHIVE, image=image)
                jpg = '{endpoint}/jpg/{image}.jpg'.format(
                    endpoint=ARCHIVE, image=image)
                png = '{endpoint}/png/{image}.png'.format(
                    endpoint=ARCHIVE, image=image)
                thumb_key = 'images/thumbs/{image}.jpg'.format(image=image)
                jpg_key = 'images/jpg/{image}.jpg'.format(image=image)
                png_key = 'images/png/{image}.png'.format(image=image)
                upload_file(get_image_data(thumb), thumb_key)
                upload_file(get_image_data(jpg), jpg_key)
                upload_file(get_image_data(png), png_key)
            list_path = 'images/list/images_{date}.json'.format(date=date)
            upload_file(json.dumps(image_list, date), list_path)

if __name__ == '__main__':
    main()
