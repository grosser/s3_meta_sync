require "bundler/setup"

require "single_cov"
SingleCov.setup :rspec

require "s3_meta_sync"
require "tmpdir"
require "stub_server"
