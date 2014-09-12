# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'gooddata_connectors_ads/version'

Gem::Specification.new do |spec|
  spec.name          = "gooddata_connectors_ads"
  spec.version       = GoodData::Connectors::Ads::VERSION
  spec.authors       = ["Adrian Toman"]
  spec.email         = ["adrian.toman@gooddata.com"]
  spec.summary       = %q{Gem for integration with Gooddata ADS, written for connectors framework}
  spec.description   = %q{Gem for integration with Gooddata ADS, written for connectors framework}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
  spec.add_dependency "erubis"
  spec.add_dependency "sequel"
  spec.add_dependency "gooddata-dss-jdbc"
end
