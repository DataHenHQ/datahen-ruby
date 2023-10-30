module Datahen
  module Client
    class JobTask < Datahen::Client::Base
      def all(job_id, opts={})
        params = @options.merge(opts)
        self.class.get("/jobs/#{job_id}/tasks", params)
      end

      def find(job_id, task_id, opts={})
        params = @options.merge(opts)
        self.class.get("/jobs/#{job_id}/tasks/#{task_id}", params)
      end

    end

  end
end
