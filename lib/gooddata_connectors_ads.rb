require "gooddata_connectors_ads/version"
require "gooddata_connectors_ads/ads_storage"
require "gooddata_connectors_ads/type_converter"
require "gooddata_connectors_ads/connection"
require "gooddata_connectors_ads/helper/helper"
require "erubis"
require "gooddata_connectors_ads/composite/tree/node"
require "gooddata_connectors_ads/composite/tree/task"
Dir["#{File.dirname(__FILE__)}/gooddata_connectors_ads/composite/**/*.rb"].each { |f| require(f) }

module GoodData
  module Connectors
    module Ads
      class AdsMiddleware < GoodData::Bricks::Middleware

        def call(params)
          $log = params["GDC_LOGGER"]
          $log.info "Initializing AdsMiddleware"
          ads_storage = AdsStorage.new(params["metadata_wrapper"],params)
          @app.call(params.merge('ads_storage_wrapper' => ads_storage))
        end
      end
    end
  end
end