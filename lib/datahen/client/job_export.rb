module Datahen
  module Client
    class JobExport < Datahen::Client::Base
      def create(job_id, exporter_name)
        self.class.post("/jobs/#{job_id}/exports/#{exporter_name}", @options)
      end
    end
  end
end

