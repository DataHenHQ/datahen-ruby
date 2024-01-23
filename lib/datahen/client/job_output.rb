module Datahen
  module Client
    class JobOutput < Datahen::Client::Base
      def find(job_id, collection, id)
        self.class.get("/jobs/#{job_id}/output/collections/#{collection}/records/#{id}", @options)
      end

      def all(job_id, collection = 'default', opts = {})
        limit = opts.has_key?(:retry_limit) ? opts.fetch(:retry_limit) : 0
        self.retry(limit, 10, "Error while updating the seeder.", true, CHECK_EMPTY_BODY) do
          self.class.get("/jobs/#{job_id}/output/collections/#{collection}/records", @options)
        end
      end

      def collections(job_id)
        self.class.get("/jobs/#{job_id}/output/collections", @options)
      end
    end
  end
end
