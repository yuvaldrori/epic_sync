import json
import urllib2
import boto3
import botocore
from time import sleep
import datetime
import argparse
import logging


def main():
    BUCKET = 'epic-archive-mirror'
    API = 'http://epic.gsfc.nasa.gov/api'
    ARCHIVE = 'http://epic.gsfc.nasa.gov/epic-archive'
    RETRIES = 5
    DAYS_TRACK_CHANGES = 14

    def get_available_dates():
        data = urllib2.urlopen(
            '{endpoint}/images.php?available_dates'.format(
                endpoint=API))
        if data.code == 200:
            dates = json.loads(data.read())
            dates.reverse()
            return dates
        logging.info('Failed getting available dates')
        return None

    def get_list_by_date(date):
        data = urllib2.urlopen(
            '{endpoint}/images.php?date={date}'.format(
                endpoint=API, date=date))
        if data.code == 200:
            return json.loads(data.read())
        logging.info('Failed getting list by date for date: {}'.format(date))
        return None

    def get_mirror_list_by_date(date):
        client = boto3.client('s3')
        list_path = 'images/list/images_{date}.json'.format(date=date)
        mirror_list = ''
        try:
            mirror_list = client.get_object(
                Bucket=BUCKET,
                Key=list_path)['Body']
        except botocore.exceptions.ClientError as e:
            logging.info(
                'Failed getting list by date from mirror for date: {}'.format(date))
            return None
        return json.loads(mirror_list)

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
        logging.info(
            'Failed getting image data for image path: {}'.format(image_path))
        return None

    def date_synced(date):
        if args.full or not list_exists(date):
            logging.info(
                'Date: {} needs sync because running in full mode or list does not exist in mirror'.format(date))
            return False

        now = datetime.datetime.today()
        test_date = datetime.datetime.strptime(date, '%Y-%m-%d')
        delta = now - test_date

        if delta.days > DAYS_TRACK_CHANGES:
            logging.info(
                'No need to sync date: {}, too old for rescan'.format(date))
            return True

        original = get_list_by_date(date)
        mirror = get_mirror_list_by_date(date)

        if original is None or mirror is None or not list_up_to_date(
                original, mirror):
            logging.info(
                'Date: {} needs syncing because original list was changed'.format(date))
            return False

        return True

    def list_up_to_date(original, mirror):
        if len(original) != len(mirror):
            logging.info('List not up to date - different size')
            return False

        original_set = set()
        for image in original:
            original_set.add(image['image'])

        mirror_set = set()
        for image in mirror:
            mirror_set.add(image['image'])

        if original_set != mirror_set:
            logging.info('List not up to date - missing images')
            return False

        return True

    def list_exists(date):
        client = boto3.client('s3')
        try:
            client.head_object(
                Bucket=BUCKET,
                Key='images_{date}.json'.format(
                    date=date))
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                logging.info(
                    'List for date: {} does not exist on mirror'.format(date))
                return False
        return True

    def upload_file(data, key):
        client = boto3.client('s3')

        if isinstance(data, basestring):
            body = data
            content_type = 'text/html; charset=UTF-8'
        else:
            body = data.read()
            content_type = data.info()['Content-type']

        if not args.dryrun:
            client.put_object(
                Body=body,
                Bucket=BUCKET,
                Key=key,
                ContentType=content_type)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--full',
        help='Sync all dates',
        action='store_true')
    parser.add_argument(
        '--dryrun',
        help='Not writing to mirror',
        action='store_true')
    parser.add_argument(
        '--verbose',
        help='Print debug messages',
        action='store_true')
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    dates = get_available_dates()
    upload_file(json.dumps(dates), 'images/available_dates.json')
    first_iteration = True
    for date in dates:
        logging.info('date: {}'.format(date))
        if not date_synced(date):
            image_list = get_list_by_date(date)
            images = get_images_names_by_date(image_list)
            all_images_downloaded = True
            for image in images:
                logging.info('image: {}'.format(image))
                thumb = '{endpoint}/thumbs/{image}.jpg'.format(
                    endpoint=ARCHIVE, image=image)
                jpg = '{endpoint}/jpg/{image}.jpg'.format(
                    endpoint=ARCHIVE, image=image)
                png = '{endpoint}/png/{image}.png'.format(
                    endpoint=ARCHIVE, image=image)
                thumb_key = 'images/thumbs/{image}.jpg'.format(image=image)
                logging.info('thumb key: {}'.format(thumb_key))
                jpg_key = 'images/jpg/{image}.jpg'.format(image=image)
                logging.info('jpg key: {}'.format(jpg_key))
                png_key = 'images/png/{image}.png'.format(image=image)
                logging.info('png key: {}'.format(png_key))
                thumb_data = get_image_data(thumb)
                jpg_data = get_image_data(jpg)
                png_data = get_image_data(png)
                if thumb_data is None or jpg_data is None or png_data is None:
                    logging.info('Failed downloading one of the images')
                    all_images_downloaded = False
                    break
                upload_file(thumb_data, thumb_key)
                upload_file(jpg_data, jpg_key)
                upload_file(png_data, png_key)
            if all_images_downloaded:
                list_path = 'images/list/images_{date}.json'.format(date=date)
                logging.info(
                    'All images uploaded, writing list: {}'.format(list_path))
                upload_file(json.dumps(image_list), list_path)
                if first_iteration:
                    upload_file(
                        json.dumps(image_list),
                        'images/images_latest.json')
        first_iteration = False

if __name__ == '__main__':
    main()
