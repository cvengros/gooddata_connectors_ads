require "gooddata_connectors_ads/version"
require "gooddata_connectors_ads/ads_storage"
require "gooddata_connectors_ads/type_converter"
require "gooddata_connectors_ads/connection"
require "erubis"

module GoodDataConnectorsAds


  class AdsMiddleware < GoodData::Bricks::Middleware

    def call(params)
      $log = params["GDC_LOGGER"]
      $log.info "Initializing AdsMiddleware"
      ads_storage = AdsStorage.new(params["metadata_wrapper"],params)
      @app.call(params.merge('ads_storage_wrapper' => ads_storage))
    end


  end

end
