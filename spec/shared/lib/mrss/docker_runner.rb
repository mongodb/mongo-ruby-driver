# frozen_string_literal: true
# encoding: utf-8

require 'optparse'
require 'erb'
autoload :Dotenv, 'dotenv'

module Mrss
  autoload :ServerVersionRegistry, 'mrss/server_version_registry'

  class DockerRunner
    def initialize(**opts)
      # These options are required:
      opts.fetch(:image_tag)
      opts.fetch(:dockerfile_path)
      opts.fetch(:default_script)
      opts.fetch(:project_lib_subdir)

      @options = opts.merge(preload: true)
    end

    attr_reader :options

    def run
      process_arguments
      unless @options[:exec_only]
        create_dockerfile
        create_image
      end
      if @options[:mongo_only]
        run_deployment
      else
        run_tests
      end
    end

    private

    def process_arguments
      #@options = {}
      OptionParser.new do |opts|
        opts.banner = "Usage: test-on-docker [-d distro] [evergreen_key=value ...]"

        opts.on("-a", "--add-env=PATH", "Load environment variables from PATH in .env format") do |path|
          @options[:extra_env] ||= {}
          unless File.exist?(path)
            raise "-a option references nonexistent file #{path}"
          end
          Dotenv.parse(path).each do |k, v|
            @options[:extra_env][k] = v
          end
        end

        opts.on("-d", "--distro=DISTRO", "Distro to use") do |v|
          @options[:distro] = v
        end

        opts.on('-e', '--exec-only', 'Execute tests using existing Dockerfile (for offline user)') do |v|
          @options[:exec_only] = v
        end

        opts.on('-m', '--mongo-only=PORT', 'Start the MongoDB deployment and expose it to host on ports starting with PORT') do |v|
          @options[:mongo_only] = v.to_i
        end

        opts.on('-p', '--preload', 'Preload Ruby toolchain and server binaries in docker (default)') do |v|
          @options[:preload] = v
        end

        opts.on('-P', '--no-preload', 'Do not preload Ruby toolchain and server binaries in docker') do
          @options[:preload] = false
        end

        opts.on('-s', '--script=SCRIPT', 'Test script to invoke') do |v|
          @options[:script] = v
        end

        opts.on('-i', '--interactive', 'Interactive mode - disable per-test timeouts') do |v|
          @options[:interactive] = v
        end
      end.parse!

      @env = Hash[ARGV.map do |arg|
        arg.split('=', 2)
      end]

      @env['RVM_RUBY'] ||= 'ruby-2.7'
      unless ruby =~ /^j?ruby-/
        raise "RVM_RUBY option is not in expected format: #{ruby}"
      end

      @env['MONGODB_VERSION'] ||= '4.4'
    end

    def create_dockerfile
      template_path = File.join(File.dirname(__FILE__), '../../share/Dockerfile.erb')
      result = ERB.new(File.read(template_path)).result(binding)
      File.open(dockerfile_path, 'w') do |f|
        f << result
      end
    end

    def image_tag
      options.fetch(:image_tag)
    end

    def dockerfile_path
      options.fetch(:dockerfile_path)
    end

    def create_image
      run_command(['docker', 'build',
        '-t', image_tag,
        '-f', dockerfile_path,
        '.'])
    end

    BASE_TEST_COMMAND = %w(docker run --rm -i --tmpfs /tmpfs:exec).freeze

    def run_tests
      run_command(BASE_TEST_COMMAND + tty_arg + extra_env + [image_tag] +
        script.split(/\s+/))
    end

    def run_deployment
      run_command(BASE_TEST_COMMAND + tty_arg + extra_env + [
        '-e', %q`TEST_CMD=watch -x bash -c "ps awwxu |egrep 'mongo|ocsp'"`,
        '-e', 'BIND_ALL=true',
      ] + port_forwards + [image_tag] + script.split(/\s+/))
    end

    def tty_arg
      tty = File.open('/dev/stdin') do |f|
        f.isatty
      end
      if tty
        %w(-t --init)
      else
        []
      end
    end

    def extra_env
      if @options[:extra_env]
        @options[:extra_env].map do |k, v|
          # Here the value must not be escaped
          ['-e', "#{k}=#{v}"]
        end.flatten
      else
        []
      end
    end

    def port_forwards
      args = (0...num_exposed_ports).map do |i|
        host_port = @options[:mongo_only] + i
        container_port = 27017 + i
        ['-p', "#{host_port}:#{container_port}"]
      end.flatten

      if @env['OCSP_ALGORITHM'] && !@env['OCSP_VERIFIER']
        args += %w(-p 8100:8100)
      end

      args
    end

    def run_command(cmd)
      if pid = fork
        Process.wait(pid)
        unless $?.exitstatus == 0
          raise "Process exited with code #{$?.exitstatus}"
        end
      else
        exec(*cmd)
      end
    end

    def distro
      @options[:distro] || if app_tests?
        'ubuntu2004'
        else
          case server_version
          when '3.6'
            'debian9'
          when '4.0', '4.2'
            'ubuntu1804'
          else
            'ubuntu2004'
          end
        end
    end

    BASE_IMAGES = {
      'debian81' => 'debian:jessie',
      'debian92' => 'debian:stretch',
      'debian10' => 'debian:buster',
      'debian11' => 'debian:bullseye',
      'ubuntu1404' => 'ubuntu:trusty',
      'ubuntu1604' => 'ubuntu:xenial',
      'ubuntu1804' => 'ubuntu:bionic',
      'ubuntu2004' => 'ubuntu:focal',
      'ubuntu2204' => 'ubuntu:jammy',
      'rhel62' => 'centos:6',
      'rhel70' => 'centos:7',
      'rhel80' => 'rockylinux:8',
    }.freeze

    def base_image
      BASE_IMAGES[distro] or raise "Unknown distro: #{distro}"
    end

    def ruby
      @env['RVM_RUBY']
    end

    def ruby_head?
      ruby == 'ruby-head'
    end

    def system_ruby?
      %w(1 true yes).include?(@env['SYSTEM_RUBY']&.downcase)
    end

    def server_version
      @env['MONGODB_VERSION']
    end

    def script
      @options[:script] || options.fetch(:default_script)
    end

    def debian?
      distro =~ /debian|ubuntu/
    end

    def ubuntu?
      distro=~ /ubuntu/
    end

    def preload?
      !!@options[:preload]
    end

    def interactive?
      !!@options[:interactive]
    end

    def project_lib_subdir
      options.fetch(:project_lib_subdir)
    end

    def server_download_url
      @server_download_url ||= ServerVersionRegistry.new(server_version, distro).download_url
    end

    def libmongocrypt_path
      case distro
      when /ubuntu1604/
        "./ubuntu1604/nocrypto/lib64/libmongocrypt.so"
      when /ubuntu1804/
        "./ubuntu1804-64/nocrypto/lib64/libmongocrypt.so"
      when /debian92/
        "./debian92/nocrypto/lib64/libmongocrypt.so"
      else
        raise "This script does not support running FLE tests on #{distro}. Use ubuntu1604, ubuntu1804 or debian92 instead"
      end
    end

    def expose?
      !!@options[:mongo_only]
    end

    def fle?
      %w(1 true yes).include?(@env['FLE']&.downcase)
    end

    # Mongoid
    def app_tests?
      %w(1 true yes).include?(@env['APP_TESTS']&.downcase)
    end

    def num_exposed_ports
      case @env['TOPOLOGY'] || 'standalone'
      when 'standalone', 'replica-set-single-node'
        1
      when 'replica-set'
        3
      when 'sharded-cluster'
        if @env['SINGLE_MONGOS']
          1
        else
          2
        end
      end
    end
  end
end
