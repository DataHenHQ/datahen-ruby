module Datahen
  module Scraper
    class Parser
      def self.exec_parser_page(filename, gid, job_id=nil, save=false, vars = {}, keep_outputs=false, content_file=nil, page_stub=nil, local_copy_prefix=nil, server_validation=nil)
        extname = File.extname(filename)
        case extname
        when '.rb'
          executor = RubyParserExecutor.new(
            filename: filename,
            gid: gid,
            job_id: job_id,
            vars: vars,
            keep_outputs: keep_outputs,
            content_file: content_file,
            page_stub: page_stub,
            local_copy_prefix: local_copy_prefix,
            server_validation: server_validation
          )
          executor.exec_parser(save)
        else
          puts "Unable to find a parser executor for file type \"#{extname}\""
        end
      end


    end
  end
end
