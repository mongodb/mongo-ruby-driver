# -*- mode: ruby; -*-

require 'rubygems'

begin
  require 'bundler'
  Bundler.require(:default, :deployment, :testing)
  # Bundler::GemHelper.install_tasks
rescue LoadError
  raise '[FAIL] Bundler not found! Install it with `gem install bundler; bundle install`.'
end

Dir.glob(File.join("tasks", "**", "*.rake")).sort.each { |rake| load File.expand_path(rake) }