# frozen_string_literal: true

require "zlib"
require "stringio"

module S3MetaSync
  module Zip
    class << self
      def zip(string)
        io = StringIO.new("w".dup)
        w_gz = Zlib::GzipWriter.new(io)
        w_gz.write(string)
        w_gz.close
        io.string
      end

      def unzip(io)
        Zlib::GzipReader.new(io)
      end
    end
  end
end
