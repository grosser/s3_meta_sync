name = "s3_meta_sync"
require "./lib/#{name}/version"

Gem::Specification.new name, S3MetaSync::VERSION do |s|
  s.summary = "Sync folders with s3 using a metadata file and md5 diffs"
  s.authors = ["Michael Grosser"]
  s.email = "michael@grosser.it"
  s.homepage = "https://github.com/grosser/#{name}"
  s.files = `git ls-files lib/ bin/`.split("\n")
  s.license = "MIT"
  s.add_runtime_dependency "aws-sdk-core", '~> 2.0'
  s.executables = ["s3-meta-sync"]
end
