require "s3_meta_sync/version"
require "open-uri"
require "yaml"
require "digest/md5"
require "aws/s3"

module S3MetaSync
  class Syncer
    META_FILE = ".s3-meta-sync"

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
      remote_info = download_meta(destination)
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
      {}
    end

    def download_file(source, path, destination)
      content = download_content("#{source}/#{path}")
      file = "#{destination}/#{path}"
      FileUtils.mkdir_p(File.dirname(file))
      File.write(file, content)
    end

    def download_content(path)
      open("https://s3-us-west-2.amazonaws.com/#{@bucket}/#{path}").read
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
  end
end
