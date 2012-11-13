# -*- mode: ruby; -*-

require 'rubygems'

begin
  require 'bundler'
rescue LoadError
  raise '[FAIL] Bundler not found! Install it with `gem install bundler; bundle install`.'
end

if ENV['TEST']
  Bundler.require(:default, :testing)
else
  Bundler.require(:default, :deploy, :testing)
end

Dir.glob(File.join('tasks', '**', '*.rake')).sort.each { |rake| load File.expand_path(rake) }