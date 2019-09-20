module Datahen
  module Client
    class Export < Datahen::Client::Base
      def all(opts={})
        params = @options.merge(opts)
        self.class.get("/scrapers/exports", params)
      end
    end
  end
end
