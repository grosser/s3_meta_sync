require "spec_helper"

describe S3MetaSync do
  let(:config) { YAML.load_file(File.expand_path("../credentials.yml", __FILE__)) }
  let(:s3) { AWS::S3.new(:access_key_id => config[:key], :secret_access_key => config[:secret]).buckets[config[:bucket]] }
  let(:syncer) { S3MetaSync::Syncer.new(config) }
  let(:foo_md5) { "---\nxxx: 0976fb571ada412514fe67273780c510\n" }

  def upload_simple_structure
    `mkdir foo && echo yyy > foo/xxx`
    syncer.sync("foo", "#{config[:bucket]}:bar")
  end

  def download(file)
    open("https://s3-us-west-2.amazonaws.com/#{config[:bucket]}/#{file}").read
  rescue
    nil
  end

  it "has a VERSION" do
    S3MetaSync::VERSION.should =~ /^[\.\da-z]+$/
  end

  describe "#sync" do
    around do |test|
      Dir.mktmpdir do |dir|
        Dir.chdir(dir, &test)
      end
    end

    after do
      s3.objects.each { |o| o.delete }
    end

    context "sync local to remote" do
      before { upload_simple_structure }

      it "uploads files" do
        download("bar/xxx").should == "yyy\n"
        download("bar/.s3-meta-sync").should == foo_md5
      end

      it "removes obsolete files" do
        `rm foo/xxx && echo yyy > foo/zzz`
        syncer.sync("foo", "#{config[:bucket]}:bar")
        download("bar/xxx").should == nil
        download("bar/zzz").should == "yyy\n"
      end

      it "does not upload/delete when nothing needs to be done" do
        syncer.should_receive(:upload_file).with("foo", ".s3-meta-sync", "bar")
        syncer.should_not_receive(:delete_remote_file)
        syncer.sync("foo", "#{config[:bucket]}:bar")
      end
    end

    context "sync remote to local" do
      before do
        upload_simple_structure
      end

      after do
        s3.objects.each { |o| o.delete }
      end

      it "downloads into an empty folder" do
        syncer.sync("#{config[:bucket]}:bar", "foo2")
        File.read("foo2/xxx").should == "yyy\n"
        File.read("foo2/.s3-meta-sync").should == foo_md5
      end

      it "downloads nothing when everything is up to date" do
        syncer.should_receive(:download_file).with("bar", ".s3-meta-sync", "foo")
        syncer.should_not_receive(:delete_local_file)
        syncer.sync("#{config[:bucket]}:bar", "foo")
      end

      it "deletes obsolete local files" do
        `echo yyy > foo/zzz`
        syncer.sync("#{config[:bucket]}:bar", "foo")
        File.exist?("foo/zzz").should == false
      end

      it "removes empty folders" do
        `mkdir foo/baz`
        syncer.sync("#{config[:bucket]}:bar", "foo")
        File.exist?("foo/baz").should == false
      end

      it "overwrites locally changed files" do
        `echo fff > foo/xxx`
        syncer.sync("#{config[:bucket]}:bar", "foo")
        File.read("foo/xxx").should == "yyy\n"
      end
    end
  end
end
