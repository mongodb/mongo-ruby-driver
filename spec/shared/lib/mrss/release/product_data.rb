# frozen_string_literal: true

require 'yaml'

module Mrss
  module Release
    class ProductData
      FILE_PATH = 'product.yml'

      def self.init!
        if File.exist?(FILE_PATH)
          raise "#{FILE_PATH} already exists; refusing to overwrite it"
        end

        initial_data = {
          'name' => 'Product Name',
          'description' => 'a very short description of the product',
          'package' => 'product_package',
          'jira' => 'https://url.to.jira/project',
          'version' => { 'number' => '1.0.0',
                         'file' => 'path/to/version.rb' }
        }

        File.write(FILE_PATH, initial_data.to_yaml)
      end

      def initialize
        @hash = YAML.load_file(FILE_PATH)
      end

      def save_product_file!
        File.write(FILE_PATH, @hash.to_yaml)
      end

      def rewrite_version_file!
        version_module = File.read(version_file)
        new_module = version_module.
          sub(/^(\s*)(VERSION\s*=\s*).*$/) { "#{$1}#{$2}#{quoted_version}" }
        File.write(version_file, new_module)
      end

      def version
        @hash['version']['number']
      end

      def quoted_version
        if version.include?("'")
          version.inspect
        else
          "'#{version}'"
        end
      end

      def version=(number)
        @hash['version']['number'] = number
      end

      # returns an array of [ major, minor, patch, suffix ].
      #
      # each element will be returned as a String.
      def version_parts
        version.split(/\./, 4)
      end

      # bump the version according to the given release type:
      #
      #  'major' -> increment major component, zero the others
      #  'minor' -> increment minor component, zero the patch
      #  'patch' -> increment the patch component
      def bump_version(release)
        major, minor, patch, suffix = version_parts

        case release
        when 'major' then
          major = major.to_i + 1
          minor = patch = 0
        when 'minor'
          minor = minor.to_i + 1
          patch = 0
        when 'patch'
          patch = patch.to_i + 1
        else
          raise ArgumentError, "invalid release type: #{release.inspect}"
        end

        self.version = [ major, minor, patch ].join('.')
      end

      # Invokes `#bump_version`, and then saves the new version to the
      # product.yml file and to the version.rb file.
      def bump_version!(release)
        bump_version(release)
        save_product_file!
        rewrite_version_file!
      end

      def version_file
        @hash['version']['file']
      end

      def name
        @hash['name']
      end

      # The description is intended to be used in places where it can be
      # appended to the end of a sentence, e.g.
      #
      #   "We just released #{product.name} - #{product.description}!"
      #
      # Markdown formatting is allowed (even expected).
      def description
        @hash['description']
      end

      def package
        @hash['package']
      end

      def jira_project_url
        @hash['jira']
      end

      def tag_name
        "v#{version}"
      end

      def tag_exists?(tag = tag_name)
        `git tag -l #{tag}`.strip == tag
      end

      def branch_exists?(branch)
        `git branch -l #{branch}`.strip == branch
      end

      def base_branch
        @base_branch ||= begin
                           major, minor, = version_parts
                           branch = "#{major}.#{minor}-stable"
                           branch_exists?(branch) ? branch : 'master'
                         end
      end
    end
  end
end
