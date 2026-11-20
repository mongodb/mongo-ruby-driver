# frozen_string_literal: true

require 'lite_spec_helper'
require 'open3'
require 'rbconfig'

describe 'Utils loading' do
  it 'loads ec2_setup without loading Mongo' do
    root = File.expand_path('..', CURRENT_PATH)
    spec_dir = File.join(root, 'spec')
    lib_dir = File.join(root, 'lib')
    ec2_setup_dir = File.join(root, '.evergreen', 'lib')

    script = <<~RUBY
      require 'ec2_setup'
      abort 'Utils is not defined' unless defined?(Utils)
      abort 'Mongo was loaded with Utils' if defined?(Mongo::URI)
      abort 'Utils.ec2_instance_id is missing' unless Utils.respond_to?(:ec2_instance_id)
      abort 'Mongo was loaded with Utils' if defined?(Mongo::URI)
      puts 'ok'
    RUBY

    stdout, stderr, status = Open3.capture3(
      RbConfig.ruby,
      '-I', spec_dir,
      '-I', lib_dir,
      '-I', ec2_setup_dir,
      '-e', script
    )

    expect(status).to be_success, stderr
    expect(stdout).to include('ok')
    expect(stderr).not_to include('NameError')
  end
end
