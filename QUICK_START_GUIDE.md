# Quick Start Guide - 1000 TPS DB Write Service

## 🚀 Quick Start (5 Minutes)

### 1. Start Dependencies

```bash
# Start Oracle Database
docker run -d --name oracle-db \
  -p 1522:1521 \
  -e ORACLE_PASSWORD=admin \
  gvenzl/oracle-xe:21-slim

# Start Kafka + Zookeeper
docker-compose up -d kafka zookeeper
```

### 2. Build & Run

```bash
# Build the application
./mvnw clean package -DskipTests

# Run the application
java -jar target/quarkus-app/quarkus-run.jar
```

### 3. Verify Service is Running

```bash
# Check health
curl http://localhost:8085/q/health

# Check metrics endpoint
curl http://localhost:8085/api/monitoring/metrics

# Check Prometheus metrics
curl http://localhost:8085/q/metrics
```

---

## 📊 Monitor Performance

### Real-time Throughput Monitoring

```bash
# Watch current TPS
watch -n 2 'curl -s http://localhost:8085/api/monitoring/metrics | jq -r .currentThroughputTPS'

# Get performance summary
curl http://localhost:8085/api/monitoring/summary | jq
```

### Expected Output at 1000 TPS

```json
{
  "currentThroughputTPS": "1024.50",
  "totalUpdates": "250000",
  "failureRatePercent": "0.02",
  "circuitBreakerState": "CLOSED",
  "circuitBreakerFailureCount": 0
}
```

---

## 🧪 Load Testing

### Generate 1000 TPS Load

```bash
# Install Kafka tools
brew install kafka  # macOS
apt-get install kafka  # Ubuntu

# Produce test messages
kafka-producer-perf-test \
  --topic DC-DR \
  --num-records 100000 \
  --record-size 500 \
  --throughput 1000 \
  --producer-props bootstrap.servers=localhost:9092 \
  key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

### Monitor Consumer Lag

```bash
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group accounting-dc-group \
  --describe
```

---

## 🔧 Configuration Reference

### Key Performance Settings

| Setting | Value | Purpose |
|---------|-------|---------|
| `max-size` | 150 | Connection pool size |
| `event-loop-size` | 32 | Parallel processing threads |
| `max.poll.records` | 500 | Kafka batch size |
| `failure-strategy` | dead-letter-queue | Error handling |

### File Locations

- **Configuration**: `src/main/resources/application.yml`
- **Repository**: `src/main/java/com/csg/airtel/aaa4j/external/repository/DBWriteRepository.java`
- **Consumer**: `src/main/java/com/csg/airtel/aaa4j/application/listner/DBWriteConsumer.java`

---

## 🏥 Health Checks

### Application Health

```bash
# Overall health
curl http://localhost:8085/q/health | jq

# Liveness probe
curl http://localhost:8085/q/health/live

# Readiness probe
curl http://localhost:8085/q/health/ready
```

### Circuit Breaker Status

```bash
# Check circuit breaker
curl http://localhost:8085/api/monitoring/circuit-breaker | jq

# Reset if needed
curl -X POST http://localhost:8085/api/monitoring/circuit-breaker/reset
```

---

## 🐛 Troubleshooting

### Low Throughput (< 1000 TPS)

```bash
# Check connection pool utilization
curl http://localhost:8085/q/metrics | grep hikari

# Check consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group accounting-dc-group --describe

# Increase connection pool if needed
# Edit: src/main/resources/application.yml
# Change: max-size: 150 -> 200
```

### Circuit Breaker Open

```bash
# Check circuit breaker state
curl http://localhost:8085/api/monitoring/circuit-breaker

# View recent errors
tail -n 100 logs/application.log | grep ERROR

# Reset after fixing issue
curl -X POST http://localhost:8085/api/monitoring/circuit-breaker/reset
```

### High Error Rate

```bash
# Check Dead Letter Queue
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic DC-DR-DLQ \
  --from-beginning

# View failure metrics
curl http://localhost:8085/api/monitoring/metrics | jq .failureRatePercent
```

---

## 📈 Prometheus Integration

### Add to `prometheus.yml`

```yaml
scrape_configs:
  - job_name: 'db-write-service'
    metrics_path: '/q/metrics'
    static_configs:
      - targets: ['localhost:8085']
```

### Key Metrics to Monitor

- `db_updates_total` - Total database updates
- `db_updates_failures` - Failed updates
- `db_update_duration_seconds` - Operation duration
- `db_batch_updates_total` - Batch operations

---

## 🎯 Performance Targets

| Metric | Target | Alert If |
|--------|--------|----------|
| Throughput | 1000 TPS | < 800 TPS |
| Failure Rate | < 0.1% | > 1% |
| P95 Latency | < 50ms | > 100ms |
| Circuit Breaker | CLOSED | OPEN |
| Consumer Lag | < 1000 | > 5000 |

---

## 🔒 Security Notes

- Default credentials in `application.yml` are for **development only**
- Change password before production: `password: Aaa!89nky78D`
- Use environment variables in production:
  ```bash
  export QUARKUS_DATASOURCE_PASSWORD=your_secure_password
  export QUARKUS_KAFKA_BOOTSTRAP_SERVERS=your_kafka_cluster
  ```

---

## 📚 Additional Documentation

- **Full Optimization Details**: [PERFORMANCE_OPTIMIZATION.md](PERFORMANCE_OPTIMIZATION.md)
- **Architecture Overview**: See "Architecture Improvements" section in PERFORMANCE_OPTIMIZATION.md
- **API Documentation**: Access Swagger UI at `http://localhost:8085/q/swagger-ui`

---

## ✅ Production Readiness Checklist

Before deploying to production:

- [ ] Database credentials secured via environment variables
- [ ] Connection pool sized for expected load (150+ connections)
- [ ] Kafka cluster has 10+ partitions for parallelism
- [ ] Dead Letter Queue topic created: `DC-DR-DLQ`
- [ ] Monitoring alerts configured for key metrics
- [ ] Load testing completed at 1000 TPS for 1+ hour
- [ ] Circuit breaker tested and validated
- [ ] Database has sufficient capacity (CPU, memory, I/O)

---

## 🆘 Support

For issues or questions:
1. Check troubleshooting section above
2. Review logs: `tail -f logs/application.log`
3. Check metrics: `curl http://localhost:8085/api/monitoring/summary`
4. Consult full documentation: `PERFORMANCE_OPTIMIZATION.md`

---

**Ready to handle 1000+ TPS! 🚀**
