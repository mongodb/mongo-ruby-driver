require 'bundler/inline'

gemfile true do
  source 'https://rubygems.org'
  gem 'nokogiri'
  gem 'aws-sdk-s3'
  gem 'mimemagic'
end

require 'aws-sdk-s3'
require 'mimemagic'
require_relative 'lib/mongo/version'

def upload_files(local_folder_path, s3, bucket_name, s3_folder_path)
  Dir.glob("#{local_folder_path}/**/*").each do |file|
    next if File.directory?(file)

    key = File.join(s3_folder_path, file.gsub("#{local_folder_path}/",''))

    mime_type = MimeMagic.by_path(file)

    puts "Mime type for #{file} is #{mime_type}"

    if mime_type.nil?
      puts "Unable to determine mime type for #{file}"
      s3.put_object(bucket: bucket_name, key: key, body: File.read(file))
    else
      s3.put_object(bucket: bucket_name, key: key, body: File.read(file), content_type: mime_type.type)
    end

    print '.'
    $stdout.flush
  end
end

ACCESS_KEY = ENV['DOCS_AWS_ACCESS_KEY_ID']
SECRET_KEY = ENV['DOCS_AWS_SECRET_ACCESS_KEY']
S3_BUCKET = ENV['DOCS_AWS_BUCKET']
S3_PREFIX = "docs/ruby-driver/#{Mongo::VERSION}/api"


Aws.config.update({
  region: 'us-east-2',
  credentials: Aws::Credentials.new(ACCESS_KEY, SECRET_KEY)
})
Aws.use_bundled_cert!

s3 = Aws::S3::Client.new 

MimeMagic.add('text/html', extensions: ['html'])

upload_files('build/public/master/api', s3, S3_BUCKET, S3_PREFIX)
puts "\nDone!"

