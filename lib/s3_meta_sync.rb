require "s3_meta_sync/version"
require "open-uri"
require "yaml"
require "digest/md5"
require "optparse"
require "fileutils"
require "tmpdir"

require "aws/s3"

if RUBY_VERSION < "2.0.0"
  # need to require these or upload in multiple threads will fail on systems with high load
  require "aws/s3/s3_object"
  require "aws/core/response"
  require "aws/s3/object_collection"
end

module S3MetaSync
  RemoteWithoutMeta = Class.new(Exception)
  RemoteCorrupt = Class.new(Exception)
  META_FILE = ".s3-meta-sync"
  CORRUPT_FILES_LOG = "s3-meta-sync-corrupted.log"

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
      corrupted = consume_corrupted_files(source)
      remote_info = begin
        download_meta(destination)
      rescue RemoteWithoutMeta
        log "Remote has no .s3-meta-sync, uploading everything", true
        {}
      end
      generate_meta(source)
      local_info = read_meta(source)
      upload = local_info.select { |path, md5| remote_info[path] != md5 || corrupted.include?(path) }.map(&:first)
      delete = remote_info.keys - local_info.keys
      log "Uploading: #{upload.size} Deleting: #{delete.size}", true

      upload_files(source, destination, upload)
      delete_remote_files(destination, delete)
      upload_file(source, META_FILE, destination)
    end

    def download(source, destination)
      remote_info = download_meta(source)
      generate_meta(destination)
      local_info = read_meta(destination)
      download = remote_info.select { |path, md5| local_info[path] != md5 }.map(&:first)
      delete = local_info.keys - remote_info.keys

      log "Downloading: #{download.size} Deleting: #{delete.size}", true

      unless download.empty? && delete.empty?
        mktmpdir do |staging_area|
          copy_content(destination, staging_area)
          download_files(source, staging_area, download)
          delete_local_files(staging_area, delete)
          download_file(source, META_FILE, staging_area)
          verify_integrity!(staging_area, destination)
          delete_empty_folders(staging_area)
          swap_in_directory(destination, staging_area)
        end
      end
    end

    def copy_content(destination, dir)
      system "cp -R #{destination}/* #{dir} 2>/dev/null"
    end

    def swap_in_directory(destination, dir)
      mktmpdir { |landfill| FileUtils.mv(destination, landfill) }
      FileUtils.mv(dir, destination)
      FileUtils.mkdir(dir) # make ensure in outside mktmpdir not blow up
    end

    def verify_integrity!(staging_area, destination)
      file = "#{staging_area}/#{META_FILE}"
      remote = YAML.load_file(file)
      actual = meta_data(staging_area)

      if remote != actual
        corrupted = actual.select { |file, md5| remote[file] && remote[file] != md5 }.map(&:first)
        File.write("#{destination}/#{CORRUPT_FILES_LOG}", corrupted.join("\n"))
        log "corrupted files downloaded:\n#{corrupted.join("\n")}", true
        raise RemoteCorrupt
      end
    end

    def consume_corrupted_files(source)
      log = "#{source}/#{CORRUPT_FILES_LOG}"
      if File.exist?(log)
        corrupted = File.read(log).split("\n")
        log "force uploading #{corrupted.size} corrupted files", true
        File.unlink log
        corrupted
      else
        []
      end
    end

    def upload_file(source, path, destination)
      log "Uploading #{path}"
      s3.objects["#{destination}/#{path}"].write File.read("#{source}/#{path}"), :acl => :public_read
    end

    def delete_remote_files(remote, paths)
      paths.each { |path| log "Deleting #{@bucket}:#{remote}/#{path}" }
      s3.objects.delete paths.map { |path| "#{remote}/#{path}" }
    end

    def delete_local_files(local, paths)
      paths = paths.map { |path| "#{local}/#{path}" }
      paths.each { |path| log "Deleting #{path}" }
      File.delete(*paths)
    end

    def s3
      @s3 ||= ::AWS::S3.new(:access_key_id => @config[:key], :secret_access_key => @config[:secret]).buckets[@bucket]
    end

    def generate_meta(source)
      file = "#{source}/#{META_FILE}"
      FileUtils.mkdir_p(File.dirname(file))
      File.write(file, meta_data(source).to_yaml)
    end

    def meta_data(source)
      return {} unless File.directory?(source)
      Dir.chdir(source) do
        files = Dir["**/*"].select { |f| File.file?(f) }
        Hash[files.map { |file| [file, Digest::MD5.file(file).to_s] }]
      end
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
      File.write(file, content, :encoding => content.encoding)
    end

    def download_content(path)
      log "Downloading #{path}"
      url = "https://s3#{"-#{region}" if region}.amazonaws.com/#{@bucket}/#{path}"
      options = (@config[:ssl_none] ? {:ssl_verify_mode => OpenSSL::SSL::VERIFY_NONE} : {})
      open(url, options).read
    rescue OpenURI::HTTPError
      raise "Unable to download #{url} -- #{$!}"
    rescue OpenSSL::SSL::SSLError
      retries ||= 0
      retries += 1
      if retries == 1
        log "SSL error downloading #{path}, retrying"
        retry
      else
        raise
      end
    end

    # allow to keep tmpdir in a different place, so it can be on the same device or
    # a device that is bigger
    def mktmpdir(&block)
      Dir.mktmpdir(nil, ENV["TMPDIR"], &block)
    end

    def delete_empty_folders(destination)
      `find #{destination} -depth -empty -delete`
    end

    def download_files(source, destination, paths)
      in_multiple_threads(paths) { |path| download_file(source, path, destination) }
    end

    def upload_files(source, destination, paths)
      in_multiple_threads(paths) { |path| upload_file(source, path, destination) }
    end

    def region
      @config[:region] unless @config[:region].to_s.empty?
    end

    def in_multiple_threads(data)
      threads = [@config[:parallel] || 10, data.size].min
      data = data.dup
      (0...threads).to_a.map do
        Thread.new do
          while slice = data.shift
            yield slice
          end
        end
      end.each(&:join)
    end

    def log(text, important=false)
      $stderr.puts text if @config[:verbose] or important
    end
  end

  class << self
    def run(argv)
      source, dest, options = parse_options(argv)
      Syncer.new(options).sync(source, dest)
      0
    end

    def parse_options(argv)
      options = {
        :key => ENV["AWS_ACCESS_KEY_ID"],
        :secret => ENV["AWS_SECRET_ACCESS_KEY"]
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
        opts.on("--ssl-none", "Do not verify ssl certs") { |c| options[:ssl_none] = true }
        opts.on("-V", "--verbose", "Verbose mode"){ options[:verbose] = true }
        opts.on("-h", "--help", "Show this.") { puts opts; exit }
        opts.on("-v", "--version", "Show Version"){ puts VERSION; exit}
      end.parse!(argv)

      raise "need source and destination" unless argv.size == 2
      raise "need 1 local and 1 remote" unless argv.select { |a| a.include?(":") }.size == 1
      raise "need credentials --key + --secret" if argv.last.include?(":") and (not options[:key] or not options[:secret])

      [*argv, options]
    end
  end
end
