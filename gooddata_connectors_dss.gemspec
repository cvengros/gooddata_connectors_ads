# coding: utf-8
$:.push File.expand_path("../lib", __FILE__)
#lib = File.expand_path('../lib', __FILE__)
#$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'gooddata_connectors_dss/version'

Gem::Specification.new do |spec|
  spec.name          = "gooddata_connectors_dss"
  spec.version       = Gooddata::Connectors::Storage::VERSION
  spec.authors       = ["Adrian Toman"]
  spec.email         = ["adrian.toman@gooddata.com"]
  spec.summary       = %q{A ruby interface to DSS}
  spec.description   = %q{It's awesome.}
  spec.homepage      = ""
  spec.license       = "MIT"
  spec.platform      = 'java'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency 'rspec', '~>2.14'
  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
  spec.add_dependency "gooddata"
  spec.add_dependency "gooddata-dss-jdbc"
  spec.add_dependency "sequel"
  spec.add_dependency "gooddata_connectors_base"

end
