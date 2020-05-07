Sync folders with s3 or http server using a metadata file with md5 sums.

Install
=======

    gem install s3_meta_sync

Usage
=====

```Bash
# upload local files and remove everything that is not local
s3-meta-sync <local> <bucket:folder> --key <aws-access-key> --secret <aws-secret-key>

# download from a http server (for internal mirroring)
s3-meta-sync http://my-ftp.com/some-folder <local> # no credentials required

# download files and remove everything that is not remote
s3-meta-sync <bucket:folder> <local> --region us-west-2 # no credentials required

Key and secret can also be supplied using AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY
```

If a downloaded file is does not match it's md5 sum in .s3-meta-sync, the whole download is aborted and no change is made.

### Options

```
    -k, --key KEY                    AWS access key
    -s, --secret SECRET              AWS secret key
    -r, --region REGION              AWS region if not us-west-2
    -p, --parallel COUNT             Use COUNT threads for download/upload default: 10
        --ssl-none                   Do not verify ssl certs
    -z, --zip                        Zip when uploading to save bandwidth
        --no-local-changes           Do not md5 all the local files, they did not change
    -V, --verbose                    Verbose mode
    -h, --help                       Show this.
    -v, --version                    Show Version
```

Using the [ruby api](https://github.com/grosser/s3_meta_sync/pull/25/files) `credentials_path` and `acl` are also supported to allow up/download via private api buckets. (TODO: turn into cli options)

## Production setup example

Upload:
```Bash
s3-meta-sync company:translations translations # download current translations (will fail on corrupted translations but leave a log)
cp -R translations working # make a copy so corruption detection is used on next download
rake generate_translations
s3-meta-sync working company:translations
```

Download using [multi-timeout](https://github.com/grosser/multi_timeout):
```Bash
# download translations from s3
# - timeout after 60 minutes (INT so tempdirs get cleaned up)
# - use a lockfile to not run more than once
# - on failure: print output -> cron email is sent (downloaded files are discarded)
# - on success: amend to log
multi-timeout -INT 59m -KILL 60m /usr/bin/flock -n lock sh -c '(s3-meta-sync company:translations /data/translations > /tmp/downloader.log 2>&1 && date >> /tmp/downloader.log && cat /tmp/downloader.log >> /var/log/downloader.log) || cat /tmp/downloader.log'
```

Development
===========

 - `cp spec/credentials.yml{.example,}` 
 - fill it out
 - `bundle exec rake` 

Atm no travis tests since they would need aws credentials, which I cannot store on travis securely.

Author
======
[Michael Grosser](http://grosser.it)<br/>
michael@grosser.it<br/>
License: MIT
