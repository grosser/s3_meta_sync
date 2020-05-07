# frozen_string_literal: true

require "net/http"
require "open-uri"
require "yaml"
require "digest/md5"
require "fileutils"
require "tmpdir"
require "openssl"
require "mime/types"

require "aws-sdk-s3"
require "s3_meta_sync/zip"

module S3MetaSync
  class Syncer
    DEFAULT_REGION = "us-east-1"
    STAGING_AREA_PREFIX = "s3ms_"

    AWS_PUBLIC_ACCESS = "public-read"
    AWS_PRIVATE_ACCESS = "private"

    def initialize(config)
      @config = {
        acl: AWS_PUBLIC_ACCESS,
        region: DEFAULT_REGION
      }.merge(config)
    end

    def sync(source, destination)
      raise ArgumentError if source.end_with?("/") or destination.end_with?("/")

      if destination.include?(":")
        @bucket, destination = destination.split(":")
        upload(source, destination)
      else
        if url?(source)
          @bucket = nil
          source = source
        else
          @bucket, source = source.split(":")
        end
        download(source, destination)
      end
    end

    private

    def upload(source, destination)
      corrupted = consume_corrupted_files(source)
      remote_meta = begin
        download_meta(destination)
      rescue RemoteWithoutMeta
        log "Remote has no #{META_FILE}, uploading everything", true
        {files: {}}
      end
      local_files = generate_meta(source)[:files]
      remote_files = remote_meta[:files]
      upload = if @config[:zip] == remote_meta[:zip]
        local_files.select { |path, md5| remote_files[path] != md5 || corrupted.include?(path) }
      else
        local_files
      end.map(&:first)
      delete = remote_files.keys - local_files.keys
      log "Uploading: #{upload.size} Deleting: #{delete.size}", true

      upload_files(source, destination, upload)
      delete_remote_files(destination, delete)
      upload_file(source, META_FILE, destination)
    end

    def download(source, destination)
      delete_old_temp_folders

      remote_meta = download_meta(source)
      local_files = ((@config[:no_local_changes] && read_meta(destination)) || meta_data(destination))[:files]

      download = remote_meta[:files].select { |path, md5| local_files[path] != md5 }.map(&:first)
      delete = local_files.keys - remote_meta[:files].keys

      log "Downloading: #{download.size} Deleting: #{delete.size}", true

      if download.any? || delete.any?
        Dir.mktmpdir(STAGING_AREA_PREFIX) do |staging_area|
          log "Staging area: #{staging_area}"
          FileUtils.mkdir_p(destination)
          copy_content(destination, staging_area)
          download_files(source, staging_area, download, remote_meta[:zip])
          delete_local_files(staging_area, delete)
          delete_empty_folders(staging_area)
          store_meta(staging_area, remote_meta)

          verify_integrity!(staging_area, destination, download, remote_meta[:files])
          log "Swapping in directories #{destination} and #{staging_area}"
          self.class.swap_in_directory(destination, staging_area)
          FileUtils.mkdir(staging_area) # mktmpdir tries to remove this directory
          log "Download finished"
        end
      end
    end

    # Sometimes SIGTERM causes Dir.mktmpdir to not properly delete the temp folder
    # Remove 1 day old folders
    def delete_old_temp_folders
      path = File.join(Dir.tmpdir, STAGING_AREA_PREFIX + "*")

      day = 24 * 60 * 60
      dirs = Dir.glob(path)
      dirs.select! { |dir| Time.now.utc - File.ctime(dir).utc > day } # only stale ones
      removed = dirs.each { |dir| FileUtils.rm_rf(dir) }

      log "Removed #{removed} old temp folder(s)" if removed.count > 0
    end

    def copy_content(destination, dir)
      log "Copying content from #{destination} to #{dir}"
      system "cp -R #{destination}/* #{dir} 2>/dev/null"
    end

    # almost atomic when destination and temp dir are not on the same device
    def self.swap_in_directory(destination, dir)
      next_dir = "#{destination}-next"
      delete = "#{destination}-delete"

      # clean up potential leftovers from last run
      FileUtils.remove_dir(next_dir) if File.exist?(next_dir)
      FileUtils.remove_dir(delete) if File.exist?(delete)

      # move onto the same device
      FileUtils.mv(dir, next_dir)

      # copy permissions
      FileUtils.chmod_R(File.stat(destination).mode, next_dir)

      # swap
      FileUtils.mv(destination, delete)
      FileUtils.mv(next_dir, destination)

      # cleanup old
      FileUtils.remove_dir(delete)
    end

    def verify_integrity!(staging_area, destination, changed, remote)
      log "Verifying integrity of #{changed.size} files" if changed.size > 0
      local = md5_hash(staging_area, changed)
      corrupted = local.select { |file, md5| remote[file] != md5 }.map(&:first)
      return if corrupted.empty?

      File.write("#{destination}/#{CORRUPT_FILES_LOG}", corrupted.join("\n"))
      message = "corrupted files downloaded:\n#{corrupted.join("\n")}"
      log message, true
      raise RemoteCorrupt, message
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
      content = File.read("#{source}/#{path}")
      content = Zip.zip(content) if @config[:zip] && path != META_FILE

      object = {
        acl: @config[:acl],
        bucket: @bucket,
        body: content,
        content_encoding: content.encoding.to_s,
        content_type: MIME::Types.of(path).first.to_s,
        key: "#{destination}/#{path}"
      }

      object[:server_side_encryption] = @config[:server_side_encryption] if @config[:server_side_encryption]

      s3.put_object(object)
    end

    def delete_remote_files(remote, paths)
      paths.each { |path| log "Deleting #{@bucket}:#{remote}/#{path}" }
      if paths.any?
        # keys are limited to 1000 per request: http://docs.aws.amazon.com/sdkforruby/api/Aws/S3/Bucket.html#delete_objects-instance_method
        paths.each_slice(1000) do |sliced_paths|
          log "Sending request for #{sliced_paths.size} keys"
          s3.delete_objects(
            delete: { objects: sliced_paths.map { |path| {key: "#{remote}/#{path}"} } },
            request_payer: "requester",
            bucket: @bucket
          )
        end
      end
    end

    def delete_local_files(local, paths)
      log "Delete #{paths.size} local files" if paths.size > 0
      paths = paths.map { |path| "#{local}/#{path}" }
      paths.each { |path| log "Deleting #{path}" }
      FileUtils.rm_f(paths)
    end

    def s3
      @s3 ||= begin
        config = { region: @config[:region] }

        if @config[:credentials_path]
          config[:credentials] = Aws::SharedCredentials.new(path: @config[:credentials_path], profile_name: "default")
        else
          config[:access_key_id] = @config[:key]
          config[:secret_access_key] = @config[:secret]
          config[:session_token] = @config[:session_token] if @config[:session_token]
        end

        Aws::S3::Client.new(config)
      end
    end

    def generate_meta(source)
      store_meta(source, meta_data(source))
    end

    def store_meta(source, meta)
      log "Storing meta file"
      file = "#{source}/#{META_FILE}"
      FileUtils.mkdir_p(File.dirname(file))
      File.write(file, meta.to_yaml)
      meta
    end

    def meta_data(source)
      return {files: {}} unless File.directory?(source)
      result = {files: md5_hash(source)}
      result[:zip] = @config[:zip] if @config[:zip]
      result
    end

    def md5_hash(source, files=nil)
      Dir.chdir(source) do
        files ||= Dir["**/*"].select { |f| File.file?(f) }
        Hash[files.map { |file| [file, Digest::MD5.file(file).to_s] }]
      end
    end

    def read_meta(source)
      file = "#{source}/#{META_FILE}"
      if File.exist?(file)
        content = File.read(file)
        parse_yaml_content(content) if content.size > 0
      end
    end

    def download_meta(destination)
      if private?
        private_access_download_meta(destination)
      else
        public_access_download_meta(destination)
      end
    end

    def private_access_download_meta(destination)
      content = private_content_download(destination, META_FILE).string

      raise S3MetaSync::RemoteWithoutMeta if content.empty? # if missing, upload everything

      parse_yaml_content(content)
    rescue Aws::S3::Errors::NoSuchKey, Aws::S3::Errors::AccessDenied # if requesting a file that doesn't exist AccessDenied is raised
      retries ||= 0

      raise S3MetaSync::RemoteWithoutMeta if retries >= 1

      retries += 1
      sleep 1 # maybe the remote meta was just updated ... give aws a second chance ...
      retry
    end

    def public_access_download_meta(destination)
      content = download_content("#{destination}/#{META_FILE}") { |io| io.read }

      raise OpenURI::HTTPError.new("Content is empty", nil) if content.size == 0

      parse_yaml_content(content)
    rescue OpenURI::HTTPError
      retries ||= 0

      raise S3MetaSync::RemoteWithoutMeta if retries >= 1

      retries += 1
      sleep 1 # maybe the remote meta was just updated ... give aws a second chance ...
      retry
    end

    def parse_yaml_content(content)
      result = YAML.load(content)
      result.key?(:files) ? result : {files: result} # support new and old format
    end

    def download_file(source, path, destination, zip)
      download = if private?
        private_content_download(source, path)
      else
        public_content_download(source, path)
      end

      download = S3MetaSync::Zip.unzip(download) if zip
      FileUtils.mkdir_p(File.dirname("#{destination}/#{path}"))

      # consumes less ram then File.write(path, content), possibly also faster
      File.open("#{destination}/#{path}", "wb") { |f| IO.copy_stream(download, f) }
      download.close
    end

    def private_content_download(source, path)
      obj = s3.get_object(bucket: @bucket, key: "#{source}/#{path}")
      obj.body
    end

    def public_content_download(source, path)
      download_content("#{source}/#{path}") # warning: using block form consumes more ram
    end

    def download_content(path)
      log "Downloading #{path}"
      url =
        if url?(path)
          path
        else
          "https://s3#{"-#{region}" if region}.amazonaws.com/#{@bucket}/#{path}"
        end
      options = (@config[:ssl_none] ? {:ssl_verify_mode => OpenSSL::SSL::VERIFY_NONE} : {})
      options[:open_timeout] = @config.fetch(:open_timeout, 5) # 5 seconds
      options[:read_timeout] = @config.fetch(:read_timeout, 10) # 10 seconds
      retry_downloads(url: url) { open(url, options) }
    end

    def retry_downloads(url:)
      yield
    rescue OpenURI::HTTPError, Errno::ECONNRESET, Errno::ETIMEDOUT, Net::OpenTimeout, Net::ReadTimeout => e
      max_retries = @config[:max_retries] || 2
      http_error_retries ||= 0
      http_error_retries += 1
      if http_error_retries <= max_retries
        log "#{e.class} error downloading #{url}, retrying #{http_error_retries}/#{max_retries}"
        retry
      else
        raise $!, "#{$!.message} -- while trying to download #{url}", $!.backtrace
      end
    rescue OpenSSL::SSL::SSLError
      ssl_error_retries ||= 0
      ssl_error_retries += 1
      if ssl_error_retries == 1
        log "SSL error downloading #{url}, retrying"
        retry
      else
        raise
      end
    end

    def delete_empty_folders(destination)
      log "Deleting empty folders"
      `find #{destination} -depth -empty -delete`
    end

    def download_files(source, destination, paths, zip)
      in_multiple_threads(paths) do |path|
        download_file(source, path, destination, zip)
      end
    end

    def upload_files(source, destination, paths)
      in_multiple_threads(paths) { |path| upload_file(source, path, destination) }
    end

    def region
      region = @config[:region]
      region if region != DEFAULT_REGION
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

    def url?(source)
      source.include?("://")
    end

    def log(text, important=false)
      $stderr.puts text if @config[:verbose] or important
    end

    def private?
      @config[:acl] == AWS_PRIVATE_ACCESS
    end
  end
end
