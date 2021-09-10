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
      version = info['versions'].detect do |version|
        version['version'].start_with?(desired_version) &&
        !version['version'].include?('-') &&
        # Sometimes the download situation is borked and there is a release
        # with no downloads... skip those.
        !version['downloads'].empty?
      end
      # Allow RC releases if there isn't a GA release.
      version ||= info['versions'].detect do |version|
        version['version'].start_with?(desired_version) &&
        # Sometimes the download situation is borked and there is a release
        # with no downloads... skip those.
        !version['downloads'].empty?
      end
      if version.nil?
        info = JSON.load(URI.parse('http://downloads.mongodb.org/full.json').open.read)
        versions = info['versions'].select do |version|
          version['version'].start_with?(desired_version) &&
          !version['downloads'].empty?
        end
        # Get rid of rc, beta etc. versions if there is a GA release.
        if versions.any? { |version| !version.include?('-') }
          versions.delete_if do |version|
            version['version'].include?('-')
          end
        end
        # Versions are ordered with newest first, take the first one i.e. the most
        # recent one.
        version = versions.first
        if version.nil?
          STDERR.puts "Error: no version #{desired_version}"
          exit 2
        end
      end
      dl = version['downloads'].detect do |dl|
        dl['archive']['url'].index("enterprise-#{arch}") &&
        dl['arch'] == 'x86_64'
      end
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
