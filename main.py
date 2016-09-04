import json
import urllib2
import boto3
import botocore
from time import sleep, time
import datetime
import argparse
import logging
from operator import itemgetter
from sh import convert
import os


def main():

    def list_all_png_images():
        client = boto3.resource('s3')
        pngs = []
        for obj in client.Bucket(BUCKET).objects.filter(Prefix='images/png/'):
            pngs.append(obj.key)
        return pngs

    def list_all_images_mentioned_in_lists():
        client = boto3.resource('s3')
        lists = list(
            client.Bucket(BUCKET).objects.filter(
                Prefix='images/list/'))
        images = []
        for l in lists:
            data = get_json_file_from_mirror(l.key)
            images += get_images_names_from_list(data)
        return images

    def invalidate_files(files):
        client = boto3.client('cloudfront')
        response = client.create_invalidation(
            DistributionId=DISTRIBUTION_ID,
            InvalidationBatch={
                'Paths': {
                    'Quantity': len(files),
                    'Items': files
                },
                'CallerReference': str(int(time()))
            }
        )
        invalidation_id = response['Invalidation']['Id']
        logging.info(
            'Created invalidation for files {files} with ID {id}'.format(
                files=files, id=invalidation_id))

    def file_exists(file):
        client = boto3.client('s3')
        try:
            client.head_object(
                Bucket=BUCKET,
                Key=file)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return False
        return True

    def get_available_dates():
        try:
            data = urllib2.urlopen(
                '{endpoint}/images.php?available_dates'.format(
                    endpoint=API))
            if data.code == 200:
                dates = json.loads(data.read())
                dates.reverse()
                return dates
        except:
            pass
        logging.info('Failed getting available dates')
        return None

    def get_list_by_date(date):
        try:
            data = urllib2.urlopen(
                '{endpoint}/images.php?date={date}'.format(
                    endpoint=API, date=date))
            if data.code == 200:
                return json.loads(data.read())
        except:
            pass
        logging.info('Failed getting list by date for date: {}'.format(date))
        return None

    def get_json_file_from_mirror(list_path):
        client = boto3.client('s3')
        try:
            data = client.get_object(
                Bucket=BUCKET, Key=list_path)['Body'].read()
        except botocore.exceptions.ClientError as e:
            logging.info(
                'Failed getting json file {} from mirror'.format(file))
            return None
        return json.loads(data)

    def get_mirror_list_by_date(date):
        path = 'images/list/images_{date}.json'.format(date=date)
        return get_json_file_from_mirror(path)

    def get_images_names_from_list(image_list):
        images = []
        for l in image_list:
            images.append(l['image'])
        return images

    def get_image_data(image_path):
        for i in range(RETRIES):
            try:
                data = urllib2.urlopen(image_path)
                if data.code == 200:
                    return data
            except:
                sleep(1)
        logging.info(
            'Failed getting image data for image path: {}'.format(image_path))
        return None

    def too_old_to_sync(date):
        now = datetime.datetime.today()
        test_date = datetime.datetime.strptime(date, '%Y-%m-%d')
        delta = now - test_date

        if delta.days > DAYS_TRACK_CHANGES:
            logging.info('Date {} too old'.format(date))
            return True

        return False

    def upload_data(data, key):
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

    def upload_file(file_name, key):
        client = boto3.client('s3')

        if not args.dryrun:
            client.upload_file(file_name, BUCKET, key)

    def process_images_in_list(image_list):
        images = get_images_names_from_list(image_list)
        failed_images = []
        for image in images:
            logging.info('image: {}'.format(image))
            png = '{endpoint}/png/{image}.png'.format(
                endpoint=ARCHIVE, image=image)
            png_key = 'images/png/{image}.png'.format(image=image)
            logging.info('png key: {}'.format(png_key))
            png_data = get_image_data(png)
            if png_data is None:
                logging.info('Failed image download')
                failed_images.append(image)
                continue
            base_name = os.path.abspath(
                os.path.join(os.path.sep, 'tmp', image))
            local_png_file_name = base_name + '.png'
            local_jpg_file_name = base_name + '.jpg'
            with open(local_png_file_name, 'wb') as f:
                f.write(png_data.read())
            for res in ['2048', '1024', '512', '256', '120']:
                res_string = '{res}x{res}'.format(res=res)
                convert(
                    local_png_file_name,
                    '-resize',
                    res_string,
                    local_jpg_file_name)
                # compatibility hack
                if res == '2048':
                    jpg_key = 'images/jpg/{}.jpg'.format(image)
                # NASA thumbnail
                elif res == '120':
                    jpg_key = 'images/thumbs/{}.jpg'.format(image)
                else:
                    jpg_key = 'images/jpg/{}/{}.jpg'.format(res, image)
                logging.info('jpg key: {}'.format(jpg_key))
                upload_file(local_jpg_file_name, jpg_key)
                os.remove(local_jpg_file_name)

            upload_file(local_png_file_name, png_key)
            os.remove(local_png_file_name)

        return failed_images

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
    parser.add_argument(
        '--dev',
        help='Use dev bucket',
        action='store_true')
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    if args.dev:
        BUCKET = 'dev.blueturn.earth'
    else:
        BUCKET = 'epic-archive-mirror'

    DISTRIBUTION_ID = 'E2NGB7E5BXXA9J'
    API = 'http://epic.gsfc.nasa.gov/api'
    ARCHIVE = 'http://epic.gsfc.nasa.gov/epic-archive'
    AVAILABLE_DATES_PATH_ON_MIRROR = 'images/available_dates.json'
    LATEST_IMAGES_PATH_ON_MIRROR = 'images/images_latest.json'
    RETRIES = 5
    DAYS_TRACK_CHANGES = 14

    files_to_invalidate = []

    dates = get_available_dates()
    dates_on_mirror = get_json_file_from_mirror(AVAILABLE_DATES_PATH_ON_MIRROR)
    if dates_on_mirror != dates:
        upload_data(
            json.dumps(dates,
                       indent=4),
            AVAILABLE_DATES_PATH_ON_MIRROR)
        files_to_invalidate.append('/' + AVAILABLE_DATES_PATH_ON_MIRROR)

    first_iteration = True
    for date in dates:
        logging.info('date: {}'.format(date))
        if not args.full and too_old_to_sync(date):
            break

        daily_image_list_from_archive = get_mirror_list_by_date(date)
        daily_image_list_from_api = get_list_by_date(date)

        list_of_images_to_download = []
        daily_image_list_to_archive = daily_image_list_from_api
        if args.full or daily_image_list_from_archive is None:
            logging.info('New list')
            list_of_images_to_download = daily_image_list_from_api
        else:
            logging.info('Existing list')
            archive_names = set(
                get_images_names_from_list(daily_image_list_from_archive))
            api_names = set(
                get_images_names_from_list(daily_image_list_from_api))
            new_images = api_names - archive_names
            for new_image in new_images:
                new_image_key = next(
                    (item for item in daily_image_list_from_api if item['image'] == new_image), None)
                new_image_key['new'] = True
                list_of_images_to_download.append(new_image_key)
            daily_image_list_to_archive = daily_image_list_from_archive + \
                list_of_images_to_download

        if len(list_of_images_to_download) > 0:
            failed_images = process_images_in_list(list_of_images_to_download)
            for item in list(daily_image_list_to_archive):
                # remove failed images from list
                if item['image'] in failed_images:
                    daily_image_list_to_archive.remove(item)
                else:
                    # fix coords single to double quotes
                    item['coords'] = item['coords'].replace("'", '"')
            logging.info('New images')
            list_path = 'images/list/images_{date}.json'.format(date=date)
            list_content = sorted(
                daily_image_list_to_archive,
                key=itemgetter('date'))
            upload_data(json.dumps(list_content, indent=4), list_path)
            files_to_invalidate.append('/' + list_path)
            if first_iteration:
                upload_data(
                    json.dumps(list_content, indent=4), LATEST_IMAGES_PATH_ON_MIRROR)
                files_to_invalidate.append('/' + LATEST_IMAGES_PATH_ON_MIRROR)

        first_iteration = False

    if len(files_to_invalidate) > 0:
        invalidate_files(files_to_invalidate)


if __name__ == '__main__':
    main()
