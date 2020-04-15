module Datahen
  module Client
    class JobFinisher < Datahen::Client::Base
      # Reset finisher on a scraper's current job.
      #
      # @param [Integer] job_id Job ID
      # @param [Hash] opts ({}) API custom parameters.
      #
      # @return [HTTParty::Response]
      def reset(job_id, opts={})
        params = @options.merge(opts)
        self.class.put("/jobs/#{job_id}/finisher/reset", params)
      end
    end
  end
end
