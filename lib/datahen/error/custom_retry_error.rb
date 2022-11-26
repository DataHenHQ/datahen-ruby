module Datahen
  module Error
    class CustomRetryError < Exception
      attr_accessor :error, :delay

      def initialize delay, error = nil
        self.error = error
        self.delay = delay
      end
    end
  end
end
