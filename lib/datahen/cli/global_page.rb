module Datahen
  class CLI < Thor
    class GlobalPage < Thor

      desc "show <gid>", "Show a global page"
      def show(gid)
        client = Client::GlobalPage.new(options)
        puts "#{client.find(gid)}"
      end

      desc "content <gid>", "Show content of a globalpage"
      def content(gid)
        client = Client::GlobalPage.new(options)
        result = JSON.parse(client.find_content(gid).to_s)

        if result['available'] == true
          puts "Preview content url: \"#{result['preview_url']}\""
          `open "#{result['preview_url']}"`
        else
          puts "Content does not exist"
        end
      end

    end
  end
end
