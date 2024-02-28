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
         "0.0.0.0:6300",
         nullptr,
         "The address that the prometheus exposer will listen on",
         SERVER | CLIENT | REQUIRES_RESTART,
         SettingsCategory::Monitoring);
    init("enable-prometheus",
         &enable_prometheus,
         "false",
         nullptr,
         "Enable the prometheus exposer",
         SERVER | CLIENT | REQUIRES_RESTART,
         SettingsCategory::Monitoring);
  }

  virtual ~PrometheusSettings() override {}

  std::string prometheus_listen_addr;
  bool enable_prometheus;
};

}} // namespace facebook::logdevice