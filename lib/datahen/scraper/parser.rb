module Datahen
  module Scraper
    class Parser
      def self.exec_parser_page(filename, gid, job_id=nil, save=false, vars = {}, keep_outputs=false)
        extname = File.extname(filename)
        case extname
        when '.rb'
          executor = RubyParserExecutor.new(
            filename: filename,
            gid: gid,
            job_id: job_id,
            vars: vars,
            keep_outputs: keep_outputs
          )
          executor.exec_parser(save)
        else
          puts "Unable to find a parser executor for file type \"#{extname}\""
        end
      end

      def self.exec_parser_by_page(filename, page, job_id=nil, save=false, vars = {}, keep_outputs=false)
        extname = File.extname(filename)
        case extname
        when '.rb'
          executor = RubyParserExecutor.new(
            filename: filename,
            page: page,
            job_id: job_id,
            vars: vars,
            keep_outputs: keep_outputs
          )
          executor.exec_parser(save)
        else
          puts "Unable to find a parser executor for file type \"#{extname}\""
        end
      end

    end
  end
end
