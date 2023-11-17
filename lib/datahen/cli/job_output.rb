module Datahen
  class CLI < Thor
    class JobOutput < Thor

      package_name "scraper output"
      def self.banner(command, namespace = nil, subcommand = false)
        "#{basename} #{@package_name} #{command.usage}"
      end

      desc "list <scraper_name>", "List output records in a collection that is in the current job"
      long_desc <<-LONGDESC
        List all output records in a collection that is in the current job of a scraper\n
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :page, :aliases => :p, type: :numeric, desc: 'Get the next set of records by page.'
      option :per_page, :aliases => :P, type: :numeric, desc: 'Number of records per page. Max 500 per page.'
      option :collection, :aliases => :c, desc: "Shows outputs from a specific collection.(defaults to 'default' collection)"
      option :query, :aliases => :q, type: :string, banner: :JSON, desc: 'Set search query. Must be in json format. i.e: {"Foo":"bar"} '
      def list(scraper_name)
        collection = options.fetch(:collection) { 'default' }
        if options[:job]
          client = Client::JobOutput.new(options)
          json = JSON.parse(client.all(options[:job], collection).body)
          if json['error'] == ""
            puts "#{JSON.pretty_generate(json['data'])}"
          else 
            puts "#{JSON.pretty_generate(json['error'])}"
          end
        else
          client = Client::ScraperJobOutput.new(options)
          json = JSON.parse(client.all(scraper_name, collection).body)
          if json['error'] == ""
            puts "#{JSON.pretty_generate(json['data'])}"
          else 
            puts "#{JSON.pretty_generate(json['error'])}"
          end
        end
      end

      desc "show <scraper_name> <record_id>", "Show one output record in a collection that is in the current job of a scraper"
      long_desc <<-LONGDESC
        Shows an output record in a collection that is in the current job of a scraper\n
        <record_id>: ID of the output record.\x5
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :collection, :aliases => :c, desc: "Shows output from a specific collection.(defaults to 'default' collection)"
      def show(scraper_name, id)
        collection = options.fetch(:collection) { 'default' }
        if options[:job]
          client = Client::JobOutput.new(options)
          json = JSON.parse(client.find(options[:job], collection, id).body)
          if json['error'] == ""
            puts "#{JSON.pretty_generate(json['data'])}"
          else 
            puts "#{JSON.pretty_generate(json['error'])}"
          end
        else
          client = Client::ScraperJobOutput.new(options)
          json = JSON.parse(client.find(scraper_name, collection, id).body)
          if json['error'] == ""
            puts "#{JSON.pretty_generate(json['data'])}"
          else 
            puts "#{JSON.pretty_generate(json['error'])}"
          end
        end
      end

      desc "collections <scraper_name>", "list job output collections that are inside a current job of a scraper."
      long_desc <<-LONGDESC
        List job output collections that are inside a current job of a scraper.\x5
      LONGDESC
      option :job, :aliases => :j, type: :numeric, desc: 'Set a specific job ID'
      option :page, :aliases => :p, type: :numeric, desc: 'Get the next set of records by page.'
      option :per_page, :aliases => :P, type: :numeric, desc: 'Number of records per page. Max 500 per page.'
      def collections(scraper_name)

        if options[:job]
          client = Client::JobOutput.new(options)
          json = JSON.parse(client.collections(options[:job]).body)
          if json['error'] == ""
            puts "#{JSON.pretty_generate(json['data'])}"
          else 
            puts "#{JSON.pretty_generate(json['error'])}"
          end
        else
          client = Client::ScraperJobOutput.new(options)
          json = JSON.parse(client.collections(scraper_name).body)
          if json['error'] == ""
            puts "#{JSON.pretty_generate(json['data'])}"
          else 
            puts "#{JSON.pretty_generate(json['error'])}"
          end
        end
      end

    end
  end

end
