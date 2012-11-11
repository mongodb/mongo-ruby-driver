# -*- mode: ruby; -*-

require 'rubygems'

begin
  require 'bundler'
  if ENV['TEST']
    Bundler.require(:default, :testing)
  else
    Bundler.require(:default, :deployment, :testing)
  end
  # Bundler::GemHelper.install_tasks
rescue LoadError
  raise '[FAIL] Bundler not found! Install it with `gem install bundler; bundle install`.'
end

Dir.glob(File.join('tasks', '**', '*.rake')).sort.each { |rake| load File.expand_path(rake) }