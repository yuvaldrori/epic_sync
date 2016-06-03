import json
import urllib2
import boto3
import botocore
from time import sleep


def main():
    BUCKET = 'epic-archive-mirror'
    API = 'http://epic.gsfc.nasa.gov/api'
    ARCHIVE = 'http://epic.gsfc.nasa.gov/epic-archive'
    RETRIES = 5

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
        for i in range(RETRIES):
            if data.code == 200:
                return data
            sleep(1)
        return None

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
            all_images_downloaded = True
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
                thumb_data = get_image_data(thumb)
                jpg_data = get_image_data(jpg)
                png_data = get_image_data(png)
                if thumb_data is None or jpg_data is None or png_data is None:
                    all_images_downloaded = False
                    break
                upload_file(thumb_data, thumb_key)
                upload_file(jpg_data, jpg_key)
                upload_file(png_data, png_key)
            if all_images_downloaded:
                list_path = 'images/list/images_{date}.json'.format(date=date)
                upload_file(json.dumps(image_list, date), list_path)

if __name__ == '__main__':
    main()
