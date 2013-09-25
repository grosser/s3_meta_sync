require "s3_meta_sync/version"
require "open-uri"
require "yaml"
require "digest/md5"
require "aws/s3"
require "optparse"

module S3MetaSync
  RemoteWithoutMeta = Class.new(Exception)
  META_FILE = ".s3-meta-sync"

  class Syncer
    def initialize(config)
      @config = config
    end

    def sync(source, destination)
      raise if source.end_with?("/") or destination.end_with?("/")

      if destination.include?(":")
        @bucket, destination = destination.split(":")
        upload(source, destination)
      else
        @bucket, source = source.split(":")
        download(source, destination)
      end
    end

    private

    def upload(source, destination)
      remote_info = begin
        download_meta(destination)
      rescue RemoteWithoutMeta
        {}
      end
      generate_meta(source)
      local_info = read_meta(source)

      local_info.each do |path, md5|
        next if remote_info[path] == md5
        upload_file(source, path, destination)
      end

      (remote_info.keys - local_info.keys).each do |path|
        delete_remote_file(destination, path)
      end

      upload_file(source, META_FILE, destination)
    end

    def upload_file(source, path, destination)
      s3.objects["#{destination}/#{path}"].write File.read("#{source}/#{path}"), :acl => :public_read
    end

    def delete_remote_file(remote, path)
      s3.objects["#{remote}/#{path}"].delete
    end

    def delete_local_file(local, path)
      File.delete("#{local}/#{path}")
    end

    def s3
      @s3 ||= ::AWS::S3.new(:access_key_id => @config[:key], :secret_access_key => @config[:secret]).buckets[@bucket]
    end

    def generate_meta(source)
      meta = Hash[Dir["#{source}/**/*"].select { |f| File.file?(f) }.map do |file|
        [file.sub("#{source}/", ""), Digest::MD5.file(file).to_s]
      end]
      file = "#{source}/#{META_FILE}"
      FileUtils.mkdir_p(File.dirname(file))
      File.write(file, meta.to_yaml)
    end

    def read_meta(source)
      file = "#{source}/#{META_FILE}"
      File.exist?(file) ? YAML.load(File.read(file)) : {}
    end

    def download_meta(destination)
      content = download_content("#{destination}/#{META_FILE}")
      YAML.load(content)
    rescue
      raise RemoteWithoutMeta
    end

    def download_file(source, path, destination)
      content = download_content("#{source}/#{path}")
      file = "#{destination}/#{path}"
      FileUtils.mkdir_p(File.dirname(file))
      File.write(file, content)
    end

    def download_content(path)
      url = "https://s3#{"-#{region}" if region}.amazonaws.com/#{@bucket}/#{path}"
      open(url).read
    rescue OpenURI::HTTPError
      raise "Unable to download #{url} -- #{$!}"
    end

    def download(source, destination)
      remote_info = download_meta(source)
      generate_meta(destination)
      local_info = read_meta(destination) # TODO maybe generate !?

      remote_info.each do |path, md5|
        next if local_info[path] == md5
        download_file(source, path, destination)
      end

      (local_info.keys - remote_info.keys).each do |path|
        delete_local_file(destination, path)
      end

      download_file(source, META_FILE, destination)

      `find #{destination} -depth -empty -delete`
    end

    def region
      @config[:region] unless @config[:region].to_s.empty?
    end
  end

  class << self
    def run(argv)
      source, dest, options = parse_options(argv)
      Syncer.new(options).sync(source, dest)
      0
    end

    def parse_options(argv)
      options = {}
      OptionParser.new do |opts|
        opts.banner = <<-BANNER.gsub(/^ {10}/, "")
          Sync folders with s3 using a metadata file with md5 sums.

          # upload local files and remove everything that is not local
          s3-meta-sync <local> <bucket:folder> --key <aws-access-key> --secret <aws-secret-key>

          # download files and remove everything that is not remote
          s3-meta-sync <bucket:folder> <local> # no credentials required


          Options:
        BANNER
        opts.on("-k", "--key KEY", "AWS access key") { |c| options[:key] = c }
        opts.on("-s", "--secret SECRET", "AWS secret key") { |c| options[:secret] = c }
        opts.on("-r", "--region REGION", "AWS region if not us-standard") { |c| options[:region] = c }
        opts.on("-h", "--help", "Show this.") { puts opts; exit }
        opts.on("-v", "--version", "Show Version"){ puts VERSION; exit}
      end.parse!(argv)

      raise "need source and destination" unless argv.size == 2
      raise "need credentials --key + --secret" if argv.last.include?(":") and not options[:key] or not options[:secret]

      [*argv, options]
    end
  end
end
