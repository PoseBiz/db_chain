# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run 'rake gemspec'
# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "db_chain"
  s.version = ""

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Andrew Cholakian"]
  s.date = "2012-09-25"
  s.description = "Pose Internal DB Conf"
  s.email = "andrew@andrewvc.com"
  s.executables = ["db_chain_dump"]
  s.extra_rdoc_files = [
    "LICENSE.txt",
    "README.rdoc"
  ]
  s.files = [
    ".document",
    "Gemfile",
    "Gemfile.lock",
    "LICENSE.txt",
    "README.rdoc",
    "Rakefile",
    "VERSION",
    "bin/db_chain_dump",
    "db_chain.gemspec",
    "lib/db_chain.rb",
    "test/helper.rb",
    "test/test_db_chain.rb"
  ]
  s.homepage = "http://github.com/andrewvc/db_chain"
  s.licenses = ["MIT"]
  s.require_paths = ["lib"]
  s.rubygems_version = "1.8.24"
  s.summary = "TODO: one-line summary of your gem"

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<pg>, [">= 0"])
      s.add_runtime_dependency(%q<fog>, [">= 0"])
      s.add_runtime_dependency(%q<heroku-api>, [">= 0"])
      s.add_runtime_dependency(%q<heroku>, [">= 0"])
      s.add_development_dependency(%q<shoulda>, [">= 0"])
      s.add_development_dependency(%q<rdoc>, ["~> 3.12"])
      s.add_development_dependency(%q<bundler>, ["> 1.0.0"])
      s.add_development_dependency(%q<jeweler>, ["~> 1.8.4"])
    else
      s.add_dependency(%q<pg>, [">= 0"])
      s.add_dependency(%q<fog>, [">= 0"])
      s.add_dependency(%q<heroku-api>, [">= 0"])
      s.add_dependency(%q<heroku>, [">= 0"])
      s.add_dependency(%q<shoulda>, [">= 0"])
      s.add_dependency(%q<rdoc>, ["~> 3.12"])
      s.add_dependency(%q<bundler>, ["> 1.0.0"])
      s.add_dependency(%q<jeweler>, ["~> 1.8.4"])
    end
  else
    s.add_dependency(%q<pg>, [">= 0"])
    s.add_dependency(%q<fog>, [">= 0"])
    s.add_dependency(%q<heroku-api>, [">= 0"])
    s.add_dependency(%q<heroku>, [">= 0"])
    s.add_dependency(%q<shoulda>, [">= 0"])
    s.add_dependency(%q<rdoc>, ["~> 3.12"])
    s.add_dependency(%q<bundler>, ["> 1.0.0"])
    s.add_dependency(%q<jeweler>, ["~> 1.8.4"])
  end
end
