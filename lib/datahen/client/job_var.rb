module Datahen
  module Client
    class JobVar < Datahen::Client::Base

      def find(job_id, var_name)
        self.class.get("/jobs/#{job_id}/vars/#{var_name}", @options)
      end

      def all(job_id, opts={})
        params = @options.merge opts
        self.class.get("/jobs/#{job_id}/vars", params)
      end

      def set(job_id, var_name, value, opts={})
        body = {}
        body[:value] = value
        body[:secret] = opts[:secret] if opts[:secret]
        params = @options.merge({body: body.to_json})
        self.class.put("/jobs/#{job_id}/vars/#{var_name}", params)
      end

      def unset(job_id, var_name, opts={})
        params = @options.merge(opts)
        self.class.delete("/jobs/#{job_id}/vars/#{var_name}", params)
      end
    end
  end
end
