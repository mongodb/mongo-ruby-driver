# -*- mode: ruby; -*-

require 'rubygems'

begin
  require 'bundler'
rescue LoadError
  raise '[FAIL] Bundler not found! Install it with `gem install bundler; bundle install`.'
end

if ENV.has_key?('TEST') || ENV.has_key?('TRAVIS_TEST')
  Bundler.require(:default, :testing)
else
  Bundler.require(:default, :testing, :deploy, :development)
end

Dir.glob(File.join('tasks', '**', '*.rake')).sort.each { |rake| load File.expand_path(rake) }