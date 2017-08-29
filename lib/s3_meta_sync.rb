require "optparse"
require "s3_meta_sync/version"
require "s3_meta_sync/syncer"

module S3MetaSync
  RemoteWithoutMeta = Class.new(StandardError)
  RemoteCorrupt = Class.new(StandardError)
  META_FILE = ".s3-meta-sync"
  CORRUPT_FILES_LOG = "s3-meta-sync-corrupted.log"

  class << self
    def run(argv)
      source, dest, options = parse_options(argv)
      Syncer.new(options).sync(source, dest)
      0
    end

    def parse_options(argv)
      options = {
        key: ENV["AWS_ACCESS_KEY_ID"],
        secret: ENV["AWS_SECRET_ACCESS_KEY"],
        zip: false,
      }
      OptionParser.new do |opts|
        opts.banner = <<-BANNER.gsub(/^ {10}/, "")
          Sync folders with s3 using a metadata file with md5 sums.

          # upload local files and remove everything that is not local
          s3-meta-sync <local> <bucket:folder> --key <aws-access-key> --secret <aws-secret-key>

          # download files and remove everything that is not remote
          s3-meta-sync <bucket:folder> <local> # no credentials required

          Key and secret can also be supplied using AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY

          Options:
        BANNER
        opts.on("-k", "--key KEY", "AWS access key") { |c| options[:key] = c }
        opts.on("-s", "--secret SECRET", "AWS secret key") { |c| options[:secret] = c }
        opts.on("-r", "--region REGION", "AWS region if not us-standard") { |c| options[:region] = c }
        opts.on("-p", "--parallel COUNT", Integer, "Use COUNT threads for download/upload default: 10") { |c| options[:parallel] = c }
        opts.on("-o", "--open-timeout TIMEOUT", Integer, "Net::HTTP open timeout in seconds default: 5") { |c| options[:open_timeout] = c }
        opts.on("-t", "--read-timeout TIMEOUT", Integer, "Net::HTTP read timeout in seconds default: 10") { |c| options[:read_timeout] = c }
        opts.on("--ssl-none", "Do not verify ssl certs") { options[:ssl_none] = true }
        opts.on("-z", "--zip", "Zip when uploading to save bandwidth") { options[:zip] = true }
        opts.on("--no-local-changes", "Do not md5 all the local files, they did not change") { options[:no_local_changes] = true }
        opts.on("--retries MAX", Integer, "MAX number of times retrying failed http requests default: 2") { |c| options[:max_retries] = c }
        opts.on("-V", "--verbose", "Verbose mode"){ options[:verbose] = true }
        opts.on("-h", "--help", "Show this.") { puts opts; exit }
        opts.on("-v", "--version", "Show Version") { puts VERSION; exit}
      end.parse!(argv)

      raise "need source and destination" unless argv.size == 2
      raise "need 1 local and 1 remote" unless argv.select { |a| a.include?(":") }.size == 1
      raise "need credentials --key + --secret" if argv.last.include?(":") and (not options[:key] or not options[:secret])

      [*argv, options]
    end
  end
end
