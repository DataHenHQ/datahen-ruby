module Datahen
  module Client
    class JobResource < Datahen::Client::Base
      def all(job_id, opts={})
        params = @options.merge(opts)
        self.class.get("/jobs/#{job_id}/resources", params)
      end
    end

  end
end
