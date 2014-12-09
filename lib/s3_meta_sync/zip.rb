require "zlib"
require "stringio"

module S3MetaSync
  module Zip
    class << self
      def zip(string)
        io = StringIO.new("w")
        w_gz = Zlib::GzipWriter.new(io)
        w_gz.write(string)
        w_gz.close
        io.string
      end

      def unzip(string)
        Zlib::GzipReader.new(StringIO.new(string, "rb")).read
      end
    end
  end
end
