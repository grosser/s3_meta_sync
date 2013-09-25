Sync folders with s3 using a metadata file with md5 sums.

Install
=======

    gem install s3_meta_sync

Usage
=====

```Bash
# upload local files and remove everything that is not local
s3-meta-sync <local> <bucket:folder> --key <aws-access-key> --secret <aws-secret-key>

# download files and remove everything that is not remote
s3-meta-sync <bucket:folder> <local> --region us-west-2 # no credentials required
```

### Options

```
    -k, --key KEY                    AWS access key
    -s, --secret SECRET              AWS secret key
    -r, --region REGION              AWS region if not us-standard
    -h, --help                       Show this.
    -v, --version                    Show Version
```

Author
======
[Michael Grosser](http://grosser.it)<br/>
michael@grosser.it<br/>
License: MIT<br/>
[![Build Status](https://travis-ci.org/grosser/s3_meta_sync.png)](https://travis-ci.org/grosser/s3_meta_sync)
