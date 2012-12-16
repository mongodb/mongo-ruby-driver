desc "Starts an IRB session pre-loaded with the 'mongo' gem"
task :console do
  system 'irb -rubygems -I lib -r mongo.rb'
end