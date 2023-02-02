module Datahen
  module Scraper
    class RubyParserExecutor < Executor
      attr_accessor :save
      # Refetch self page flag.
      # @return [Boollean]
      # @note It is stronger than #reparse_self flag.
      attr_accessor :refetch_self
      # Reparse self page flag.
      # @return [Boollean]
      # @note It is stronger than #limbo_self flag.
      attr_accessor :reparse_self
      # Limbo self page flag.
      # @return [Boollean]
      attr_accessor :limbo_self

      FIND_OUTPUTS_RETRY_LIMIT = nil

      def initialize(options={})
        @filename = options.fetch(:filename) { raise "Filename is required"}
        @page = options.fetch(:page) { nil }
        @gid = (self.page || {})['gid'] || options.fetch(:gid) { raise "GID or a page with a GID is required"}
        @job_id = options.fetch(:job_id)
        @page_vars = options.fetch(:vars) { {} }
        @keep_outputs = !!(options.fetch(:keep_outputs) { false })
      end

      def self.exposed_methods
        [
          :get_content,
          :get_failed_content,
          :content,
          :failed_content,
          :outputs,
          :pages,
          :page,
          :save_pages,
          :save_outputs,
          :find_output,
          :find_outputs,
          :refetch,
          :reparse,
          :limbo,
          :finish
        ].freeze
      end

      def exec_parser(save=false)
        @save = save
        if save
          puts "Executing parser script"
        else
          puts "Trying parser script"
        end

        eval_parser_script(save)
      end

      def init_page_vars(page)
        return self.page unless self.page.nil?

        if !@page_vars.nil? && !@page_vars.empty?
          page['vars'] = @page_vars
        end
        page
      end

      def update_to_server(opts = {})
        parsing_update(
          job_id: opts[:job_id],
          gid: opts[:gid],
          pages: opts[:pages],
          outputs: opts[:outputs],
          parsing_status: opts[:status])
      end

      def update_parsing_starting_status
        return unless save

        response = parsing_update(
          job_id: job_id,
          gid: gid,
          parsing_status: :starting,
          keep_outputs: @keep_outputs
        )

        if response.code == 200
          puts "Page Parsing Status Updated."
        else
          puts "Error: Unable to save Page Parsing Status to server: #{response.body}"
          raise "Unable to save Page Parsing Status to server: #{response.body}"
        end
      end

      def update_parsing_done_status
        return unless save

        response = parsing_update(
          job_id: job_id,
          gid: gid,
          parsing_status: :done)

        if response.code == 200
          puts "Page Parsing Done."
        else
          puts "Error: Unable to save Page Parsing Done Status to server: #{response.body}"
          raise "Unable to save Page Parsing Done Status to server: #{response.body}"
        end
      end

      def update_parsing_status page_gid, status
        return unless save

        response = parsing_update(
          job_id: job_id,
          gid: page_gid,
          parsing_status: status)

        if response.code == 200
          puts "Page #{page_gid} status changed to #{status}."
        else
          puts "Error: Unable to change page  #{page_gid} status: #{response.body} to #{status}"
          raise "Unable to change page  #{page_gid} status: #{response.body} to #{status}"
        end
      end

      def save_type
        :parsing
      end

      def refetch_page page_gid
        if save
          update_parsing_status page_gid, :to_refetch
          puts "Refetch page #{page_gid}"
        else
          puts "Would have refetch page #{page_gid}"
        end
      end

      def refetch page_gid
        raise ArgumentError.new("page_gid needs to be a String.") unless page_gid.is_a?(String)
        if page_gid == gid
          self.refetch_self = true
          raise Error::SafeTerminateError
        end
        refetch_page page_gid
      end

      def reparse_page page_gid
        if save
          update_parsing_status page_gid, :to_reparse
          puts "Reparse page #{page_gid}"
        else
          puts "Would have reparse page #{page_gid}"
        end
      end

      def reparse page_gid
        raise ArgumentError.new("page_gid needs to be a String.") unless page_gid.is_a?(String)
        if page_gid == gid
          self.reparse_self = true
          raise Error::SafeTerminateError
        end
        reparse_page page_gid
      end

      def limbo_page page_gid
        if save
          update_parsing_status page_gid, :limbo
          puts "Limbo page #{page_gid}"
        else
          puts "Would have limbo page #{page_gid}"
        end
      end

      def limbo page_gid
        raise ArgumentError.new("page_gid needs to be a String.") unless page_gid.is_a?(String)
        if page_gid == gid
          self.limbo_self = true
          raise Error::SafeTerminateError
        end
        limbo_page page_gid
      end

      def eval_parser_script(save=false)
        update_parsing_starting_status

        proc = Proc.new do
          page = init_page
          outputs = []
          pages = []
          page = init_page_vars(page)
          self.refetch_self = false
          self.reparse_self = false
          self.limbo_self = false

          begin
            context = isolated_binding({
              outputs: outputs,
              pages: pages,
              page: page
            })
            eval_with_context filename, context
          rescue Error::SafeTerminateError => e
            # do nothing, this is fine
          rescue SyntaxError => e
            handle_error(e) if save
            raise e
          rescue => e
            handle_error(e) if save
            raise e
          end

          puts "=========== Parsing Executed ==========="
          begin
            save_pages_and_outputs(pages, outputs, :parsing) unless refetch_self
          rescue => e
            handle_error(e) if save
            raise e
          end

          if refetch_self
            update_parsing_status gid, :to_refetch
          elsif reparse_self
            update_parsing_status gid, :to_reparse
          elsif limbo_self
            update_parsing_status gid, :limbo
          else
            update_parsing_status gid, :done
          end
        end
        proc.call
      end

      def content
        @content ||= get_content(job_id, gid)
      end

      def failed_content
        @failed_content ||= get_failed_content(job_id, gid)
      end

      def handle_error(e)
        error = ["Parsing #{e.class}: #{e.to_s} (Job:#{job_id} GID:#{gid})",clean_backtrace(e.backtrace)].join("\n")

        parsing_update(
          job_id: job_id,
          gid: gid,
          parsing_status: :failed,
          log_error: error)
      end

    end
  end
end
