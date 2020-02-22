module Datahen
  module Client
    class GlobalPage < Datahen::Client::Base
      def find(gid)
        self.class.get("/global_pages/#{gid}", @options)
      end

      def find_content(gid)
        self.class.get("/global_pages/#{gid}/content", @options)
      end
    end
  end
end
