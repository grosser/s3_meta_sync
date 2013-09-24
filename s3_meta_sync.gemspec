$LOAD_PATH.unshift File.expand_path("../lib", __FILE__)
name = "s3_meta_sync"
require "#{name.gsub("-","/")}/version"

Gem::Specification.new name, S3MetaSync::VERSION do |s|
  s.summary = "Sync folders with s3 using a metadata file and md5 diffs"
  s.authors = ["Michael Grosser"]
  s.email = "michael@grosser.it"
  s.homepage = "http://github.com/grosser/#{name}"
  s.files = `git ls-files lib/ bin/`.split("\n")
  s.license = "MIT"
  cert = File.expand_path("~/.ssh/gem-private-key-grosser.pem")
  if File.exist?(cert)
    s.signing_key = cert
    s.cert_chain = ["gem-public_cert.pem"]
  end
  s.add_runtime_dependency "aws-sdk"
end
