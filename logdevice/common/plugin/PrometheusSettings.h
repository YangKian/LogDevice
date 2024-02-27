#include <logdevice/common/settings/UpdateableSettings.h>

namespace facebook { namespace logdevice {

class PrometheusSettings : public SettingsBundle {
 public:
  virtual const char* getName() const override {
    return "Prometheus";
  }

  virtual void defineSettings(SettingEasyInit& init) {
    using namespace SettingFlag;

    // TODO add support for push model for clients
    init("prometheus-listen-addr",
         &prometheus_listen_addr,
         "6300",
         nullptr,
         "The address that the prometheus exposer will listen on",
         SERVER | CLIENT | REQUIRES_RESTART,
         SettingsCategory::Monitoring);
    init("disable-prometheus-publisher",
         &disable_prometheus,
         "false",
         nullptr,
         "Disable the prometheus exposer",
         SERVER | CLIENT | REQUIRES_RESTART,
         SettingsCategory::Monitoring);
  }

  virtual ~PrometheusSettings() override {}

  std::string prometheus_listen_addr;
  bool disable_prometheus;
};

}} // namespace facebook::logdevice