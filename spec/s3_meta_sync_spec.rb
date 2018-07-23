require "spec_helper"

SingleCov.covered! uncovered: 3 # .run is covered via CLI tests, but does not report coverage
(Dir['lib/**/*.rb'] - ['lib/s3_meta_sync.rb', 'lib/s3_meta_sync/version.rb']).each do |file|
  SingleCov.covered! file: file
end

describe S3MetaSync do
  def sh(command, options={})
    result = `#{command} #{"2>&1" unless options[:keep_output]}`
    raise "#{options[:fail] ? "SUCCESS" : "FAIL"} #{command}\n#{result}" if $?.success? == !!options[:fail]
    result
  end

  let(:key)    { config.fetch(:key) }
  let(:secret) { config.fetch(:secret) }

  let(:bucket) { config.fetch(:bucket) }
  let(:region) { config.fetch(:region) }

  let(:config) do
    YAML.load_file(File.expand_path("../credentials.yml", __FILE__))
  end

  let(:s3) do
    Aws::S3::Client.new(
      access_key_id: key,
      secret_access_key: secret,
      region: region
    )
  end

  let(:foo_md5) do |variable|
    if RUBY_VERSION < '2.4.0'
      "---\n:files:\n  xxx: 0976fb571ada412514fe67273780c510\n"
    else
      "---\n:files:\n  xxx: '0976fb571ada412514fe67273780c510'\n"
    end
  end
  let(:syncer) { S3MetaSync::Syncer.new(config) }

  def cleanup_s3
    keys = s3.list_objects(bucket: bucket).contents.map { |o| {key: o.key} }
    s3.delete_objects(bucket: bucket, delete: {objects: keys})
  end

  def upload_simple_structure
    sh "mkdir foo && echo yyy > foo/xxx"
    syncer.sync("foo", "#{bucket}:bar")
  end

  def download(file)
    region = if region && region != S3MetaSync::Syncer::DEFAULT_REGION
      "-#{region}"
    else
      nil
    end
    open("https://s3#{region}.amazonaws.com/#{bucket}/#{file}").read
  rescue
    nil
  end

  def with_utf8_encoding
    old = Encoding.default_external, Encoding.default_internal
    Encoding.default_external = Encoding.default_internal = Encoding::UTF_8
    yield
  ensure
    Encoding.default_external, Encoding.default_internal = old
  end

  def upload(file, content)
    File.write("foo/#{file}", content)
    syncer.send(:upload_file, "foo", file, "bar")
    expect(syncer.send(:download_content, "bar/#{file}").read).to eq(content)
  end

  around do |test|
    Dir.mktmpdir do |dir|
      Dir.chdir(dir, &test)
    end
  end

  it "has a VERSION" do
    expect(S3MetaSync::VERSION).to match(/^[\.\da-z]+$/)
  end

  describe "#parse_yaml_content" do
    let(:meta_yaml_content) { { files: { "xxx" => "0976fb571ada412514fe67273780c510" } } }
    let(:foo_md5_rails_2_2) { "---\n:files:\n  xxx: 0976fb571ada412514fe67273780c510\n" }
    let(:foo_md5_rails_2_4) { "---\n:files:\n  xxx: '0976fb571ada412514fe67273780c510'\n" }

    it 'parses yaml content on ruby < 2.4 format' do
      expect(syncer.send(:parse_yaml_content, foo_md5_rails_2_2)).to eq(meta_yaml_content)
    end

    it 'parses yaml content on ruby 2.4 format' do
      expect(syncer.send(:parse_yaml_content, foo_md5_rails_2_4)).to eq(meta_yaml_content)
    end
  end

  describe "#sync" do
    before do
      allow($stderr).to receive(:puts)
      upload_simple_structure
    end
    after { cleanup_s3 }

    context "sync local to remote" do
      it "uploads files" do
        expect(download("bar/xxx")).to eq("yyy\n")
        expect(download("bar/.s3-meta-sync")).to eq(foo_md5)
      end

      it "removes obsolete files" do
        sh "rm foo/xxx && echo yyy > foo/zzz"
        syncer.sync("foo", "#{bucket}:bar")

        expect(download("bar/xxx")).to be_nil
        expect(download("bar/zzz")).to eq("yyy\n")
      end

      it "does not upload/delete when nothing needs to be done" do
        expect(syncer).to receive(:upload_file).with("foo", ".s3-meta-sync", "bar")
        expect(syncer).not_to receive(:delete_remote_file)

        syncer.sync("foo", "#{bucket}:bar")
      end

      it "force uploads files that are corrupted" do
        old = File.read("foo/xxx")
        upload "xxx", "corrupt!"

        # log corrupted files while downloading
        File.write("foo/xxx", "changed") # trigger a download
        expect {
          syncer.sync("#{bucket}:bar", "foo")
        }.to raise_error(S3MetaSync::RemoteCorrupt)
        expect(File.read("foo/s3-meta-sync-corrupted.log")).to eq("xxx")

        # uploader should see the log and force a upload
        File.write("foo/xxx", old) # same md5 so normally would not upload
        syncer.sync("foo", "#{bucket}:bar")
        expect(syncer.send(:download_content, "bar/xxx").read).to eq(old)

        # log got consumed
        expect(File.exist?("foo/s3-meta-sync-corrupted.log")).to be false
      end

      it "does upload files that were corrupt but no longer exist" do
        File.write("foo/s3-meta-sync-corrupted.log", "something")
        syncer.sync("foo", "#{bucket}:bar")
      end

      it "has no server_side_encryption setting by default" do
        allow(syncer).to receive(:s3) { s3 }
        expect(s3).to receive(:put_object).with(hash_excluding(:server_side_encryption))

        syncer.sync("foo", "#{bucket}:bar")
      end

      describe "with zip enabled" do
        def config
          super.merge(zip: true)
        end

        it "uploads files zipped" do
          file = download("bar/xxx")
          expect(file).to include "tH{r"
          expect(S3MetaSync::Zip.unzip(StringIO.new(file)).read).to eq("yyy\n")
          expect(download("bar/.s3-meta-sync")).to eq(foo_md5.sub(/\n\z/, "\n:zip: true\n"))
        end
      end

      describe "server_side_encryption specified by the config" do
        def config
          super.merge(server_side_encryption: "AES256")
        end

        it "uses the server_side_encryption method set in the config" do
          allow(syncer).to receive(:s3) { s3 }
          expect(s3).to receive(:put_object).with(hash_including(server_side_encryption: "AES256"))

          syncer.sync("foo", "#{bucket}:bar")
        end
      end
    end

    context "sync remote to local" do
      def self.it_downloads_into_an_empty_folder
        it "downloads into an empty folder" do
          no_cred_syncer.sync("#{bucket}:bar", "foo2")
          expect(File.read("foo2/xxx")).to eq("yyy\n")
          expect(File.read("foo2/.s3-meta-sync")).to eq(foo_md5)
        end
      end

      let(:no_cred_syncer) { S3MetaSync::Syncer.new(region: region, no_local_changes: config[:no_local_changes]) }

      it "fails when trying to download an empty folder (which would remove everything)" do
        expect {
          no_cred_syncer.sync("#{bucket}:baz", "foo")
        }.to raise_error(S3MetaSync::RemoteWithoutMeta)
      end

      it "retries when trying to download an empty folder" do
        expect {
          expect(no_cred_syncer).to receive(:download_content).
            with(anything).exactly(2).
            and_raise(OpenURI::HTTPError.new(
              1111,
              "Unable to download https://#{region}.amazonaws.com/s3-meta-sync/bar/.s3-meta-sync -- 404 Not Found"
            ))
          no_cred_syncer.sync("#{bucket}:baz", "foo")
        }.to raise_error(S3MetaSync::RemoteWithoutMeta)
      end

      it_downloads_into_an_empty_folder

      it "downloads into an absolute folder" do
        no_cred_syncer.sync("#{bucket}:bar", "#{Dir.pwd}/foo2")
        expect(File.read("foo2/xxx")).to eq("yyy\n")
        expect(File.read("foo2/.s3-meta-sync")).to eq(foo_md5)
      end

      it "does not leave tempdirs behind" do
        dir = File.dirname(Dir.mktmpdir)
        before = Dir["#{dir}/*"].size

        no_cred_syncer.sync("#{bucket}:bar", "foo2")
        after = Dir["#{dir}/*"].size

        expect(after).to eq(before)
      end

      it "does not remove recent tempdirs left behind by SIGTERM exceptions" do
        Dir.mktmpdir(S3MetaSync::Syncer::STAGING_AREA_PREFIX)
        path = File.join(Dir.tmpdir, S3MetaSync::Syncer::STAGING_AREA_PREFIX + '*')
        before = Dir[path].size

        no_cred_syncer.sync("#{bucket}:bar", "foo2")
        after = Dir[path].size

        expect(after).to eq(before)
      end

      it "remove older (than a day) tempdirs left behind by SIGTERM exceptions" do
        Dir.mktmpdir(S3MetaSync::Syncer::STAGING_AREA_PREFIX)
        dir = Dir.mktmpdir(S3MetaSync::Syncer::STAGING_AREA_PREFIX)
        path = File.join(Dir.tmpdir, S3MetaSync::Syncer::STAGING_AREA_PREFIX + '*')
        before = Dir[path].size

        allow(File).to receive(:ctime).and_return(Time.now.utc)
        ctime = Time.at(Time.now.utc - 25 * 60 * 60)
        allow(File).to receive(:ctime).with(dir).and_return(ctime)

        no_cred_syncer.sync("#{bucket}:bar", "foo2")
        after = Dir[path].size

        expect(after).to eq(before - 1)
      end

      it "downloads nothing when everything is up to date" do
        expect(no_cred_syncer).not_to receive(:download_file)
        expect(no_cred_syncer).not_to receive(:delete_local_files)

        no_cred_syncer.sync("#{bucket}:bar", "foo")
      end

      it "deletes obsolete local files" do
        sh "echo yyy > foo/zzz"
        no_cred_syncer.sync("#{bucket}:bar", "foo")

        expect(File.exist?("foo/zzz")).to be false
      end

      it "removes empty folders" do
        sh "mkdir foo/baz"
        sh "echo dssdf > foo/baz/will_be_deleted"
        no_cred_syncer.sync("#{bucket}:bar", "foo")

        expect(File.exist?("foo/baz")).to be false
      end

      it "does not fail when local files that no longer exist are mentioned in local .s3-meta-sync" do
        File.open('foo/.s3-meta-sync', 'a+') { |f| f.puts "  gone: 1976fb571ada412514fe67273780c510\n  nested/gone: 2976fb571ada412514fe67273780c510" }
        no_cred_syncer.sync("#{bucket}:bar", "foo")
      end

      it "overwrites locally changed files" do
        sh "echo fff > foo/xxx"
        no_cred_syncer.sync("#{bucket}:bar", "foo")

        expect(File.read("foo/xxx")).to eq("yyy\n")
      end

      it "does not overwrite local files when md5 does not match" do
        # s3 is corrupted
        upload "xxx", "corrupt!"

        # directory exists with an old file
        FileUtils.mkdir("foo2")
        File.write("foo2/xxx", "old")

        # does not override
        expect {
          no_cred_syncer.sync("#{bucket}:bar", "foo2")
        }.to raise_error S3MetaSync::RemoteCorrupt

        expect(File.read("foo2/xxx")).to eq("old")
      end

      it "does not consider additional files on s3 as corrupted" do
        upload "yyy", "not-tracked"
        File.unlink("foo/yyy")
        syncer.sync("#{bucket}:bar", "foo")

        expect(File.exist?("foo/yyy")).to be false
      end

      it "does download from remote with old .s3-meta-sync format" do
        old_format = "---\nxxx: 0976fb571ada412514fe67273780c510\n"
        upload(".s3-meta-sync", old_format)
        no_cred_syncer.sync("#{bucket}:bar", "foo2")

        expect(File.read("foo2/xxx")).to eq("yyy\n")
        expect(File.read("foo2/.s3-meta-sync")).to eq(foo_md5)
      end

      describe "with changes and --no-local-changes set" do
        before do
          config[:no_local_changes] = true
          sh "echo fff > foo/xxx"
        end

        it "does not re-check local files for changes" do
          no_cred_syncer.sync("#{bucket}:bar", "foo")

          expect(File.read("foo/xxx")).to eq("fff\n")
        end

        it "does not fail when unneeded local files for do not exist" do
          File.open('foo/.s3-meta-sync', 'a+') { |f| f.puts "  gone: 1976fb571ada412514fe67273780c510\n  nested/gone: 2976fb571ada412514fe67273780c510" }
          no_cred_syncer.sync("#{bucket}:bar", "foo")

          expect(File.read("foo/xxx")).to eq("fff\n")
        end

        it "downloads with old .s3-meta-sync format" do
          File.write('foo/.s3-meta-sync', YAML.dump(YAML.load(foo_md5).fetch(:files))) # old format had just files in an array
          no_cred_syncer.sync("#{bucket}:bar", "foo")

          expect(File.read("foo/xxx")).to eq("fff\n")
        end
      end

      describe "when uploaded with zip" do
        def config
          super.merge(zip: true)
        end
        def foo_md5
          super.sub(/\n\z/, "\n:zip: true\n")
        end

        it_downloads_into_an_empty_folder
      end
    end
  end

  it "can download from a http server" do
    allow($stderr).to receive(:puts)
    port = 9000
    replies = {
      "/foo/.s3-meta-sync" => [200, {}, [{
        files: {
          "bar/baz.txt" => "eb61eead90e3b899c6bcbe27ac581660",
          "baz/bar.txt" => "5289492cf082446ca4a6eec9f72f1ec3"
        }}.to_yaml
      ]],
      "/foo/bar/baz.txt" => [200, {}, ["HELLO"]],
      "/foo/baz/bar.txt" => [200, {}, ["WORLD"]],
    }
    StubServer.open(port, replies) do |server|
      server.wait

      syncer = S3MetaSync::Syncer.new({})
      syncer.sync('http://localhost:9000/foo', 'local')
      expect(File.exist?('local/.s3-meta-sync')).to eq true
      expect(File.read('local/bar/baz.txt')).to eq 'HELLO'
      expect(File.read('local/baz/bar.txt')).to eq 'WORLD'
    end
  end

  it "can upload and download a utf-8 file" do
    with_utf8_encoding do
      syncer.instance_variable_set(:@bucket, bucket)
      expected = "…"
      sh "mkdir foo"
      File.write("foo/utf8", expected)
      syncer.send(:upload_file, "foo", "utf8", "bar")
      syncer.send(:download_file, "bar", "utf8", "baz", false)
      read = File.read("baz/utf8")

      expect(read).to eq(expected)
      expect(read.encoding).to eq(Encoding::UTF_8)
      expect(read[/…/]).to eq("…")
    end
  end

  describe ".parse_options" do
    let(:defaults) { {key: nil, secret: nil, zip: false} }

    def call(*args)
      S3MetaSync.send(:parse_options, *args)
    end

    after do
      ENV.delete "AWS_ACCESS_KEY_ID"
      ENV.delete "AWS_SECRET_ACCESS_KEY"
    end

    it "fails with empty" do
      expect { call([]) }.to raise_error(StandardError)
    end

    it "fails with 2 remotes" do
      expect { call(["x:z", "z:y", "--key", "k", "--secret", "s"]) }.to raise_error(StandardError)
    end

    it "fails with 2 locals" do
      expect { call(["x", "z"]) }.to raise_error(StandardError)
    end

    it "parses source + destination" do
      expect(call(["x:z", "y"])).to eq(["x:z", "y", defaults])
    end

    it "parses key + secret" do
      expect(call(["x", "y:z", "--key", "k", "--secret", "s"])).to eq(["x", "y:z", defaults.merge(key: "k", secret: "s")])
    end

    it "fails with missing key" do
      expect { call(["x", "y:z", "--secret", "s"]) }.to raise_error(StandardError)
    end

    it "fails with missing secret" do
      expect { call(["x", "y:z", "--key", "k"]) }.to raise_error(StandardError)
    end

    it "fails with missing key and secret" do
      expect { call(["x", "y:z"]) }.to raise_error(StandardError)
    end

    it "takes key and secret from the environment" do
      ENV["AWS_ACCESS_KEY_ID"] = "k"
      ENV["AWS_SECRET_ACCESS_KEY"] = "s"

      expect(call(["x", "y:z"])).to eq(["x", "y:z", defaults.merge(key: "k", secret: "s")])
    end

    it "take verbose mode" do
      expect(call(["x:z", "y", "-V"])).to eq(["x:z", "y", defaults.merge(verbose: true)])
      expect(call(["x:z", "y", "--verbose"])).to eq(["x:z", "y", defaults.merge(verbose: true)])
    end

    it "parses --ssl-none" do
      expect(call(["x:z", "y", "--ssl-none"])).to eq(["x:z", "y", defaults.merge(ssl_none: true)])
    end

    it "parses --zip" do
      expect(call(["x:z", "y", "--zip"])).to eq(["x:z", "y", defaults.merge(zip: true)])
    end

    it "parses --no-local-changes" do
      expect(call(["x:z", "y", "--no-local-changes"])).to eq(["x:z", "y", defaults.merge(no_local_changes: true)])
    end

    it "parses --retries" do
      expect(call(["x:z", "y", "--retries=5"])).to eq(["x:z", "y", defaults.merge(max_retries: 5)])
    end

    it "parses --open-timeout" do
      expect(call(["x:z", "y", "--open-timeout=3"])).to eq(["x:z", "y", defaults.merge(open_timeout: 3)])
    end

    it "parses --read-timeout" do
      expect(call(["x:z", "y", "--read-timeout=7"])).to eq(["x:z", "y", defaults.merge(read_timeout: 7)])
    end
  end

  describe ".download_content" do
    before do
      allow($stderr).to receive(:puts)
      upload_simple_structure
    end
    after { cleanup_s3 }

    it "downloads" do
      expect(syncer.send(:download_content, "bar/xxx").read).to eq("yyy\n")
    end

    it "retries once on ssl error" do
      expect(syncer).to receive(:open).and_raise OpenSSL::SSL::SSLError.new
      expect(syncer).to receive(:open).and_return double(read: "fff")
      expect(syncer.send(:download_content, "bar/xxx").read).to eq("fff")
    end

    it "retries once on net::http open timeout error" do
      expect(syncer).to receive(:open).and_raise Net::OpenTimeout.new
      expect(syncer).to receive(:open).and_return double(read: "fff")
      expect(syncer.send(:download_content, "bar/xxx").read).to eq("fff")
    end

    it "retries once on net::http read timeout error" do
      expect(syncer).to receive(:open).and_raise Net::ReadTimeout.new
      expect(syncer).to receive(:open).and_return double(read: "fff")
      expect(syncer.send(:download_content, "bar/xxx").read).to eq("fff")
    end

    it "does not retry multiple times on ssl error" do
      expect(syncer).to receive(:open).exactly(2).and_raise OpenSSL::SSL::SSLError.new
      expect { syncer.send(:download_content, "bar/xxx") }.to raise_error(OpenSSL::SSL::SSLError)
    end

    it "retries on a HTTP error" do
      expect(syncer).to receive(:open).and_raise OpenURI::HTTPError.new('http error', nil)
      expect(syncer).to receive(:open).and_raise OpenURI::HTTPError.new('http error', nil)
      expect(syncer).to receive(:open).and_return double(read: "fff")
      expect(syncer.send(:download_content, "bar/xxx").read).to eq("fff")
    end

    it "does not retry more than 3 times on a HTTP error" do
      expect(syncer).to receive(:open).exactly(3).and_raise OpenURI::HTTPError.new('http error', nil)
      expect { syncer.send(:download_content, "bar/xxx").read }.to raise_error(OpenURI::HTTPError)
    end

    it "does not retry more than 3 times on a Errno::ECONNRESET" do
      expect(syncer).to receive(:open).exactly(3).and_raise Errno::ECONNRESET
      expect { syncer.send(:download_content, "bar/xxx").read }.to raise_error(Errno::ECONNRESET)
    end

    describe "with retries option" do
      before { config[:max_retries] = 3 }

      it "retries more than 3 times on a HTTP error" do
        expect(syncer).to receive(:open).exactly(4).and_raise OpenURI::HTTPError.new('http error', nil)
        expect { syncer.send(:download_content, "bar/xxx").read }.to raise_error(OpenURI::HTTPError)
      end

      it "retries more than 3 times on a Errno::ECONNRESET" do
        expect(syncer).to receive(:open).exactly(4).and_raise Errno::ECONNRESET
        expect { syncer.send(:download_content, "bar/xxx").read }.to raise_error(Errno::ECONNRESET)
      end
    end
  end

  describe ".swap_in_directory" do
    # this test being green is no guarantee of being atomic
    it "does not leave gaps" do
      begin
        Dir.mkdir("foo")
        Dir.mkdir("bar")
        File.write("foo/bar", "1")
        File.write("bar/bar", "2")
        tester = Thread.new { loop { expect(File.exist?("foo/bar")).to be true } }
        sleep 0.1 # let tester get started

        expect(File.read("foo/bar")).to eq("1")
        S3MetaSync::Syncer.swap_in_directory("foo", "bar")
        expect(File.read("foo/bar")).to eq("2")
      ensure
        tester.kill
      end
    end
  end

  describe "CLI" do
    let(:params) { "--key #{key} --secret #{secret} --region #{region}" }
    def sync(command, options={})
      sh("#{Bundler.root}/bin/s3-meta-sync #{command}", options)
    end

    it "shows --version" do
      expect(sync("--version")).to include(S3MetaSync::VERSION)
    end

    it "shows --help" do
      expect(sync("--help")).to include("Sync folders with s3")
    end

    context "upload" do
      let(:new_bucket) { "#{bucket}:bar" }
      let(:bucket_url) do
        subdomain = (region == S3MetaSync::Syncer::DEFAULT_REGION ? "s3" : "s3-#{region}")

        "https://#{subdomain}.amazonaws.com/#{bucket}/bar/.s3-meta-sync"
      end

      around do |test|
        begin
          sh "mkdir foo && echo yyy > foo/xxx"
          test.call
        ensure
          cleanup_s3
        end
      end

      it "works" do
        expect(sync("foo #{new_bucket} #{params}")).to eq("Remote has no .s3-meta-sync, uploading everything\nUploading: 1 Deleting: 0\n")
        expect(download("bar/xxx")).to eq("yyy\n")
      end

      it "is verbose" do
        RSpec::Support::ObjectFormatter.default_instance.max_formatted_output_length = 1000
        result = sync("foo #{new_bucket} #{params} --verbose").strip
        expect(result).to eq <<-TXT.gsub(/^ {10}/, "").strip
          Downloading bar/.s3-meta-sync
          OpenURI::HTTPError error downloading #{bucket_url}, retrying 1/2
          OpenURI::HTTPError error downloading #{bucket_url}, retrying 2/2
          Downloading bar/.s3-meta-sync
          OpenURI::HTTPError error downloading #{bucket_url}, retrying 1/2
          OpenURI::HTTPError error downloading #{bucket_url}, retrying 2/2
          Remote has no .s3-meta-sync, uploading everything
          Storing meta file
          Uploading: 1 Deleting: 0
          Uploading xxx
          Uploading .s3-meta-sync
        TXT
      end
    end
  end
end
