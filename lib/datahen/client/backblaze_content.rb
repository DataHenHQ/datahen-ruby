require 'zlib'
require 'httparty'

module Datahen
  module Client
    class BackblazeContent
      include HTTParty

      def get_content(url)
        self.class.get(url, format: :plain)
      end

      def get_gunzipped_content(url)
        # Zlib.gunzip(get_content(url))
        gunzip(get_content(url))
      end

      def gunzip(string)
        sio = StringIO.new(string)
        gz = Zlib::GzipReader.new(sio, encoding: Encoding::ASCII_8BIT)
        _content = ""
        begin
          _content = gz.read
        rescue => e
          # if unexpected eof error, then readchar until error, and ignore it
          if e.to_s == 'unexpected end of file'
            # heavily improve content read recovery by using "String#<<",
            #  reading all "good" lines and then concat the remaining chars
            begin
              gz.each_line{|line| content << line}
            rescue => e
              begin
                content << gz.readchar while !gz.eof
              rescue => e
                puts "Ignored Zlib error: #{e.to_s}"
              end
            end
          else
            raise e
          end
        end

        return _content
      ensure
        gz.close if gz.respond_to?(:close)
      end
    end
  end
end
