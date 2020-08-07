module Datahen
  module Client
    class Account < Datahen::Client::Base

      def profile(opts={})
        params = @options.merge(opts)

        self.class.get("/profile", params)
      end

    end
  end
end
