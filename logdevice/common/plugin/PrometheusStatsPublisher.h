#include <unordered_map>

#include <logdevice/common/StatsPublisher.h>
#include <prometheus/exposer.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/counter.h>
#include <prometheus/registry.h>

namespace facebook { namespace logdevice {

class PrometheusStatsPublisher : public StatsPublisher {
 public:
  PrometheusStatsPublisher(const std::string& listen_addr);

  // Used for tests
  PrometheusStatsPublisher(std::shared_ptr<prometheus::Registry> registry);

  virtual ~PrometheusStatsPublisher() = default;

  void publish(const std::vector<const Stats*>& current,
               const std::vector<const Stats*>& previous,
               std::chrono::milliseconds elapsed) override;

  void addRollupEntity(std::string entity) override;

  prometheus::Family<prometheus::Gauge>&
  getGaugeFamily(const std::string& name, const std::string& stats_name, const std::string& help);

  prometheus::Family<prometheus::Counter>&
  getCounterFamily(const std::string& name, const std::string& stats_name, const std::string& help);

 public:
  struct StatsInfo {
    bool is_counter;
    const char* help;
  };

  std::unordered_map<std::string, StatsInfo> stats_info_;

 private:
  std::unique_ptr<prometheus::Exposer> exposer_;
  std::shared_ptr<prometheus::Registry> registry_;
  std::unordered_map<std::string, prometheus::Family<prometheus::Gauge>&>
      gauge_famililes_;
  std::unordered_map<std::string, prometheus::Family<prometheus::Counter>&>
      counter_famililes_;
};

}} // namespace facebook::logdevice
