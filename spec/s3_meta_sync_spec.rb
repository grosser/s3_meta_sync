require "spec_helper"

describe S3MetaSync do
  it "has a VERSION" do
    S3MetaSync::VERSION.should =~ /^[\.\da-z]+$/
  end
end
