import argparse
import urllib2
import logging
import json
import boto3
import botocore
import os
from tempfile import gettempdir
from subprocess import check_call
import numpy as np
import cv2
import math
import random
import sys
from time import time, sleep


class Epic:

    def __init__(self, args, config):
        self.args = args
        self.config = config
        self.s3 = boto3.client('s3')
        self.invalidate_paths = set()

    def invalidate(self):
        if not self.args.dryrun and len(self.invalidate_paths) > 0:
            paths = list(self.invalidate_paths)
            client = boto3.client('cloudfront')
            response = client.create_invalidation(
                DistributionId=self.config['distribution_id'],
                InvalidationBatch={
                    'Paths': {
                        'Quantity': len(paths),
                        'Items': paths
                    },
                    'CallerReference': str(int(time()))
                }
            )
            invalidation_id = response['Invalidation']['Id']
            logging.info(
                'Created invalidation for files {} with ID {}'.format(
                    paths, invalidation_id))

    def _read_file_from_mirror(self, bucket, key):
        try:
            return self.s3.get_object(
                Bucket=bucket, Key=key)['Body'].read()
        except botocore.exceptions.ClientError as e:
            logging.info(
                'error reading file from s3://{}/{}'.format(bucket, key))
        return None

    def _read_file_from_url(self, url):
        for i in range(self.config['retries']):
            try:
                data = urllib2.urlopen(url)
                if data.code == 200:
                    return data.read()
            except:
                sleep(1)
        logging.info('error reading file from ' + url)
        return None

    def _read_json(self, data):
        return json.loads(data)

    def dates_completed(self):
        ret = []
        suffix = '.json'
        kwargs = {
            'Bucket': self.config['bucket'],
            'Prefix': '{}/list/images_'.format(self.config['images_folder'])}
        continuation_token = ''
        while True:
            if continuation_token != '':
                kwargs['ContinuationToken'] = continuation_token
            response = self.s3.list_objects_v2(**kwargs)
            if 'Contents' in response:
                ret += [d['Key'][len(kwargs['Prefix']):-len(suffix)]
                        for d in response['Contents']]
            else:
                return ret
            if response['IsTruncated']:
                continuation_token = response['NextContinuationToken']
            else:
                return sorted(ret)

    def missing_dates(self):
        ret = []
        url = self.config['api_url'] + '/all'
        data = self._read_file_from_url(url)
        dates_from_api = []
        for d in self._read_json(data):
            dates_from_api.append(d['date'])
        if self.args.full:
            ret = dates_from_api
        else:
            dates_from_mirror = self.dates_completed()
            missing_dates = set(dates_from_api) - set(dates_from_mirror)
            common_dates = set(dates_from_api) & set(dates_from_mirror)
            for date in common_dates:
                logging.info('len for date: ' + date)
                num_images_api = len(self.image_list(date))
                num_images_archive = len(self.image_list_mirror(date))
                if num_images_api != num_images_archive:
                    logging.info(
                        'At date: {}, api: {}, arch: {}'.format(
                            date, num_images_api, num_images_archive))
                    missing_dates.add(date)
            ret = missing_dates
        return sorted(ret, reverse=True)

    def image_list(self, date):
        url = '{}/date/{}'.format(self.config['api_url'], date)
        data = self._read_file_from_url(url)
        return self._read_json(data)

    def image_list_mirror(self, date):
        bucket = self.config['bucket']
        key = '{}/list/images_{}.json'.format(
            self.config['images_folder'], date)
        data = self._read_file_from_mirror(bucket, key)
        return self._read_json(data)

    def _upload_file(self, path, bucket, key):
        if not self.args.dryrun:
            self.s3.upload_file(path, bucket, key)

    def _upload_data(self, body, bucket, key, content_type):
        if not self.args.dryrun:
            self.s3.put_object(
                Body=body,
                Bucket=bucket,
                Key=key,
                ContentType=content_type)

    def set_latest_date(self, date):
        source = {
            'Bucket': self.config['bucket'],
            'Key': '{}/list/images_{}.json'.format(
                self.config['images_folder'], date)
        }
        bucket = self.config['bucket']
        key = self.config['latest_images_path']
        if not self.args.dryrun:
            self.s3.copy_object(Bucket=bucket, Key=key, CopySource=source)

    def get_date_from_image_name(self, image_name):
        date_part_from_name = image_name.split('_')[2]
        year = date_part_from_name[:4]
        month = date_part_from_name[4:6]
        day = date_part_from_name[6:8]
        return year, month, day

    def png(self, image_name):
        year, month, day = self.get_date_from_image_name(image_name)
        url = '{archive_url}/{year}/{month}/{day}/png/{image}.png'.format(
            archive_url=self.config['archive_url'],
            year=year,
            month=month,
            day=day,
            image=image_name)
        logging.info('Downloading ' + url)
        data = self._read_file_from_url(url)
        filename = os.path.join(gettempdir(), image_name + '.png')
        with open(filename, 'wb') as f:
            f.write(data)
        key = '{}/png/{}.png'.format(self.config['images_folder'], image_name)
        logging.info(
            'Uploading to s3://{}/{}'.format(self.config['bucket'], key))
        self._upload_file(filename, self.config['bucket'], key)

    def jpgs(self, image_name):
        for res in self.config['res']:
            res_string = '{res}x{res}'.format(res=res)
            infile = os.path.join(gettempdir(), image_name + '.png')
            outfile = os.path.join(gettempdir(), image_name + '.jpg')
            cmd = 'convert {} -resize {} {}'.format(
                infile, res_string, outfile)
            check_call(cmd, shell=True)
            # compatibility hack
            if res == '2048':
                key = '{}/jpg/{}.jpg'.format(
                    self.config['images_folder'],
                    image_name)
            # NASA thumbnail
            elif res == '120':
                key = '{}/thumbs/{}.jpg'.format(
                    self.config['images_folder'],
                    image_name)
            else:
                key = '{}/jpg/{}/{}.jpg'.format(
                    self.config['images_folder'],
                    res,
                    image_name)
            logging.info(
                'Uploading to s3://{}/{}'.format(self.config['bucket'], key))
            self._upload_file(outfile, self.config['bucket'], key)
            os.remove(outfile)

    def bounding_shapes(self, image_name):
        filename = os.path.join(gettempdir(), image_name + '.png')
        im = cv2.imread(filename, 0)
        height, width = im.shape
        ret, thresh = cv2.threshold(im, 10, 255, cv2.THRESH_BINARY)
        contours, hierarchy = cv2.findContours(
            thresh, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        area = 0
        idx = 0
        max_area = math.pi * (height / 2)**2
        for index, item in enumerate(contours):
            a = cv2.contourArea(item)
            if a > area and a < max_area:
                area = a
                idx = index
        cnt = contours[idx]
        center, radius = cv2.minEnclosingCircle(cnt)
        cx = center[0] / 2048
        cy = center[1] / 2048
        r = radius / 2048
        logging.info('Circle center, radius: {}, {}'.format((cx, cy), r))
        (ex, ey), (MA, ma), angle = cv2.fitEllipse(cnt)

        if self.args.debug:
            im2 = cv2.imread(filename)
            cv2.circle(im2, (int(center[0]), int(center[1])), int(radius), (255, 255, 255), 4)
            cv2.ellipse(im2, ((ex, ey), (MA, ma), angle), (0, 0, 255), 4)
            cv2.drawContours(im2, contours, idx, (0, 255, 0), 4)
            cv2.imwrite(os.path.join(gettempdir(), '_debug_' + image_name + '.png'), im2)

        points = cv2.ellipse2Poly((int(ex), int(ey)), (int(
            MA / 2), int(ma / 2)), int(angle), 0, 360, 1)
        npoints = []
        for point in random.sample(points, 5):
            npoints.append((float(point[0]) / 2048, float(point[1]) / 2048))
        cache = {
            'jpg': {
                'earth_circle': {
                    'center': {'x': cx, 'y': cy},
                    'radius': r
                },
                'earth_ellipse': {'points': npoints}
            },
            'png': {
                'earth_circle': {
                    'center': {'x': cx, 'y': cy},
                    'radius': r
                },
                'earth_ellipse': {'points': npoints}
            }
        }
        return cache

    def run(self):
        dates = self.missing_dates()
        for date in dates:
            logging.info('Working on date: ' + date)
            images_json = self.image_list(date)
            logging.info('Read json with {} images'.format(len(images_json)))
            try:
                for image in images_json:
                    image_name = image['image']
                    logging.info('Working on image: ' + image_name)
                    self.png(image_name)
                    self.jpgs(image_name)
                    image['cache'] = self.bounding_shapes(image_name)
                    # delete png
                    os.remove(os.path.join(gettempdir(), image_name + '.png'))
                    # fix json coming from the api
                    image['coords'] = image['coords'].replace("'", '"')
                logging.info(
                    'Uploading json with {} images'.format(
                        len(images_json)))
                self._upload_data(
                    json.dumps(images_json, indent=4),
                    self.config['bucket'],
                    '{}/list/images_{}.json'.format(
                        self.config['images_folder'],
                        date),
                    'application/json')
                lists = self.dates_completed()
                self._upload_data(
                    json.dumps(lists, indent=4),
                    self.config['bucket'],
                    self.config['available_dates_path'],
                    'application/json')
                self.invalidate_paths.add(
                    '/' + self.config['available_dates_path'])
                self.set_latest_date(lists[-1])
                self.invalidate_paths.add(
                    '/' + self.config['latest_images_path'])
            except:
                logging.info(
                    'Skipped date: {} because of an error.'.format(date))
                continue
        self.invalidate()


def main():
    def _parse_arguments():
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--full',
            help='Full sync',
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
        parser.add_argument(
            '--enhanced',
            help='Sync enhanced images',
            action='store_true')
        parser.add_argument(
            '--debug',
            help='Save debug images',
            action='store_true')
        return parser.parse_args()

    def _config(args):
        if args.verbose:
            logging.basicConfig(level=logging.INFO)

        config = {}

        if args.dev:
            config['bucket'] = 'blueturn-content-dev'
        else:
            config['bucket'] = 'blueturn-content'

        config['distribution_id'] = 'E21HG4M80KUJI5'
        base_url = 'http://epic.gsfc.nasa.gov'
        if args.enhanced:
            config['api_url'] = base_url + '/api/enhanced'
            config['archive_url'] = base_url + '/archive/enhanced'
            config['images_folder'] = 'enhanced_images'
            config[
                'available_dates_path'] = 'enhanced_images/available_dates.json'
            config['latest_images_path'] = 'enhanced_images/images_latest.json'
        else:
            config['api_url'] = base_url + '/api/natural'
            config['archive_url'] = base_url + '/archive/natural'
            config['images_folder'] = 'images'
            config['available_dates_path'] = 'images/available_dates.json'
            config['latest_images_path'] = 'images/images_latest.json'

        config['retries'] = 5
        config['res'] = ['2048', '1024', '512', '256', '120']
        return config

    args = _parse_arguments()
    config = _config(args)

    epic = Epic(args, config)
    epic.run()


if __name__ == '__main__':
    main()
