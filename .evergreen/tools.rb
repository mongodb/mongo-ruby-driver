require 'json'
require 'open-uri'

class ServerVersionRegistry
  def initialize(desired_version, arch)
    @desired_version, @arch = desired_version, arch
  end

  attr_reader :desired_version, :arch

  def download_url
    @download_url ||= begin
      info = JSON.load(uri_open('http://downloads.mongodb.org/current.json').read)
      version = info['versions'].detect { |version| version['version'].start_with?(desired_version) }
      if version.nil?
        info = JSON.load(URI.open('http://downloads.mongodb.org/full.json').read)
        versions = info['versions'].select { |version| version['version'].start_with?(desired_version) }
        # Get rid of rc, beta etc. versions.
        versions.delete_if { |version| version['version'].include?('-') }
        # Versions are ordered with newest first, take the first one i.e. the most
        # recent one.
        version = versions.first
        if version.nil?
          STDERR.puts "Error: no version #{desired_version}"
          exit 2
        end
      end
      dl = version['downloads'].detect { |dl| dl['archive']['url'].index("enterprise-#{arch}") }
      unless dl
        STDERR.puts "Error: no download for #{arch} for #{version['version']}"
        exit 2
      end
      url = dl['archive']['url']
    end
  end

  def uri_open(*args)
    if RUBY_VERSION < '2.5'
      open(*args)
    else
      URI.open(*args)
    end
  end
end
