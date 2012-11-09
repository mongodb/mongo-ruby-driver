# -*- mode: ruby; -*-

desc "Generate all documentation"
task :docs => ['docs:yard', 'docs:rdoc']

namespace :docs do

  desc "Generate yard documention"
  task :yard do
    version = bumper_version.to_s
    out = File.join('docs', 'yard', version)
    FileUtils.rm_rf(out)
    system "yardoc -o #{out} --title mongo-#{version}"
  end

  desc "Generate rdoc documention"
  task :rdoc do
    out = File.join('docs', 'rdoc', bumper_version.to_s)
    FileUtils.rm_rf(out)
    system "rdoc --main README.md --op #{out} --inline-source --quiet README.md `find lib -name '*.rb'`"
  end

end