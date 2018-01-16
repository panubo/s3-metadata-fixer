import os
import sys
import click
import boto3

import fixer

import mimetypes

from pprint import pprint, pformat

BUCKET_NAME_REGEX = '^[a-zA-Z0-9.\-_]{1,255}$'

UNKNOWN_MIMETYPE = 'application/octet-stream'

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('bucket')
@click.argument('prefix', required=False, default="")
@click.option('--update-content-type', is_flag=True, help="Update objects Content-Type based on file extension")
@click.option('--update-cache-control', help="Update objects Cache-Control, eg. 'max-age=86400'")
@click.option('--dry-run', is_flag=True)
@click.option('--debug/--no-debug', default=False)
def main(**kwargs):
    """Bulk update metadata for AWS S3 Objects"""

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(kwargs["bucket"])
    for obj in bucket.objects.filter(Prefix=kwargs["prefix"]):
        full_obj = obj.Object()
        click.secho("%s Cache-Control: %s, Content-Type: %s, " % (obj.key, full_obj.cache_control, full_obj.content_type), fg='green')

        new_mime, new_encoding = mimetypes.guess_type(obj.key, False)
        if new_mime == None:
            new_mime = UNKNOWN_MIMETYPE

        new = {"CopySource": {'Bucket': kwargs["bucket"], 'Key': obj.key},
            "MetadataDirective": "REPLACE"}

        if full_obj.cache_control:
            new['CacheControl'] = full_obj.cache_control
        if full_obj.content_disposition:
            new['ContentDisposition'] = full_obj.content_disposition
        if full_obj.content_encoding:
            new['ContentEncoding'] = full_obj.content_encoding
        if full_obj.content_language:
            new['ContentLanguage'] = full_obj.content_language
        if full_obj.content_type:
            new['ContentType'] = full_obj.content_type
        if full_obj.metadata:
            new['Metadata'] = full_obj.metadata


        if kwargs['update_cache_control']:
            new['CacheControl'] = kwargs['update_cache_control']
        if kwargs['update_content_type']:
            new['ContentType'] = new_mime
            if new_encoding != None:
                new['ContentEncoding'] = new_encoding

        click.secho("%s Cache-Control: %s, Content-Type: %s, " % (obj.key, dict.get(new, 'CacheControl', "None"), dict.get(new, 'ContentType', "None")), fg='yellow')

        if kwargs['debug']: click.secho(pformat(new))
        if not kwargs['dry_run']: full_obj.copy_from(**new)

if __name__ == '__main__':
    main()

