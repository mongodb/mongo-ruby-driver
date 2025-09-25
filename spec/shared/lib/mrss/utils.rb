# frozen_string_literal: true
# encoding: utf-8

module Mrss
  module Utils
    extend self

    def print_backtrace(dest=STDERR)
      raise
    rescue => e
      dest.puts e.backtrace.join("\n")
    end

    # Parses the given version string, accounting for suffix information that
    # Gem::Version cannot successfully parse.
    #
    # @param [ String ] version the version to parse
    #
    # @return [ Gem::Version ] the parsed version
    #
    # @raise [ ArgumentError ] if the string cannot be parsed.
    def parse_version(version)
      Gem::Version.new(version)
    rescue ArgumentError
      match = version.match(/\A(?<major>\d+)\.(?<minor>\d+)\.(?<patch>\d+)?(-[A-Za-z\+\d]+)?\z/)
      raise ArgumentError.new("Malformed version number string #{version}") if match.nil?

      Gem::Version.new(
        [
          match[:major],
          match[:minor],
          match[:patch]
        ].join('.')
      )
    end
  end
end
