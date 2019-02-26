@echo off
REM -Djava.security.auth.login.config=conf/server-client-jaas.conf
java -cp KafkaOffsetMonitor-assembly-0.4.6-SNAPSHOT.jar com.quantifind.kafka.offsetapp.OffsetGetterWeb --offsetStorage kafka --kafkaBrokers localhost:9092,localhost:9093  --kafkaSecurityProtocol PLAINTEXT  --zk localhost  --port 8081  --refresh 10.seconds --retain 1.days  --dbName offsetapp_kafka
