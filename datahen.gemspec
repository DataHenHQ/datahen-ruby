
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "datahen/version"

Gem::Specification.new do |spec|
  spec.name          = "datahen"
  spec.version       = Datahen::VERSION
  spec.authors       = ["Parama Danoesubroto"]
  spec.email         = ["parama@datahen.com"]

  spec.summary       = %q{DataHen toolbelt for developers}
  spec.description   = %q{DataHen toolbelt to develop scrapers and other scripts}
  spec.homepage      = "https://datahen.com"
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata["allowed_push_host"] = "https://rubygems.org"
    spec.metadata["homepage_uri"] = spec.homepage
    spec.metadata["source_code_uri"] = "https://github.com/DataHenOfficial/datahen-ruby"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.required_ruby_version = '>= 2.2.2'
  spec.add_dependency "thor", "~> 0.20.3"
  spec.add_dependency 'httparty', '~> 0.16.2'
  spec.add_dependency 'nokogiri', '~> 1.6'
  spec.add_development_dependency 'bundler', '>= 1.16'
  spec.add_development_dependency 'rake', '>= 10.0'
  spec.add_development_dependency 'minitest', '>= 5.11'
  spec.add_development_dependency 'simplecov', '>= 0.16.1'
  spec.add_development_dependency 'simplecov-console', '>= 0.4.2'
  spec.add_development_dependency 'timecop', '>= 0.9.1'
  spec.add_development_dependency 'byebug', '>= 0'
end
