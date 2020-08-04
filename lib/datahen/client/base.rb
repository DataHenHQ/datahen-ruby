require 'httparty'

module Datahen
  module Client
    class Base
      include HTTParty

      default_timeout 60

      def self.env_auth_token
        ENV['DATAHEN_TOKEN']
      end

      def self.env_ignore_ssl
        ENV['DATAHEN_IGNORE_SSL'].to_s.strip == '1'
      end

      def env_api_url
        ENV['DATAHEN_API_URL'].nil? ? 'https://app.datahen.com/api/v1' : ENV['DATAHEN_API_URL']
      end

      def ignore_ssl
        return @ignore_ssl unless @ignore_ssl.nil?
        @ignore_ssl = self.class.env_ignore_ssl
        @ignore_ssl
      end

      def auth_token
        @auth_token ||= self.class.env_auth_token
      end

      def auth_token= value
        @auth_token = value
      end

      def initialize(opts={})
        @ignore_ssl = opts[:ignore_ssl]
        self.class.base_uri(env_api_url)
        self.auth_token = opts[:auth_token] unless opts[:auth_token].nil?
        @options = {
          headers: {
            "Authorization" => "Bearer #{auth_token}",
            "Content-Type" => "application/json",
          },
          verify: !ignore_ssl
        }

        query = {}
        query[:p] = opts[:page] if opts[:page]
        query[:pp] = opts[:per_page] if opts[:per_page]
        query[:fetchfail] = opts[:fetch_fail] if opts[:fetch_fail]
        query[:parsefail] = opts[:parse_fail] if opts[:parse_fail]
        query[:status] = opts[:status] if opts[:status]
        query[:page_type] = opts[:page_type] if opts[:page_type]
        query[:gid] = opts[:gid] if opts[:gid]
        query[:"min-timestamp"] = opts[:"min-timestamp"] if opts[:"min-timestamp"]
        query[:"max-timestamp"] = opts[:"max-timestamp"] if opts[:"max-timestamp"]
        query[:limit] = opts[:limit] if opts[:limit]
        query[:order] = opts[:order] if opts[:order]
        query[:filter] = opts[:filter] if opts[:filter]

        if opts[:query]
          if opts[:query].is_a?(Hash)
            query[:q] = opts[:query].to_json
          elsif opts[:query].is_a?(String)
            query[:q] = JSON.parse(opts[:query]).to_json
          end
        end

        unless query.empty?
          @options.merge!(query: query)
        end
      end
    end
  end
end
