package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
import java.util.UUID;

public class FlinkReadKafkaSASL {
        public static void main(String[] args) throws Exception {
            // Read parameters from command line
            final ParameterTool params = ParameterTool.fromArgs(args);

            if(params.getNumberOfParameters() < 6) {
                System.out.println("\nUsage: FlinkReadKafkaSASL --read-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid> --username <username> --password <password> --truststore.password <password>");
                return;
            }

            // This job expects to be able to find your truststore in the resources/ directory, named
            // "eventador_truststore.jks".  To create the trust store, simply download the Deployment
            // CA certificate from your deployment (click on the Apache Kafka link for your deployment,
            // then "SASL Users", then "Download Certificate".  You will end up with a filename that looks
            // like "xxxx-ca-cert.pem".  Use keytool to generate a truststore using this file:
            // keytool -keystore eventador_truststore.jks -alias CARoot -import -file xxxx-ca-cert.pem -storepass YOUR_PASSWORD -noprompt
            // Be sure to note the password you protect your truststore with, and pass it to your
            // job in the --truststore.password argument.
            ClassLoader classLoader = FlinkReadKafkaSASL.class.getClassLoader();
            String truststore_path = classLoader.getResource("eventador_truststore.jks").getPath();

            // Configure ScramLogin via jaas
            String module = "org.apache.kafka.common.security.scram.ScramLoginModule";
            String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", module, params.getRequired("username"), params.getRequired("password"));

            // Flink and Kafka parameters
            Properties kparams = params.getProperties();
            kparams.setProperty("auto.offset.reset", "earliest");
            kparams.setProperty("flink.starting-position", "earliest");
            kparams.setProperty("group.id", params.getRequired("group.id"));
            kparams.setProperty("bootstrap.servers", params.getRequired("bootstrap.servers"));;

            // SASL parameters
            kparams.setProperty("security.protocol", "SASL_SSL");
            kparams.setProperty("sasl.mechanism", "SCRAM-SHA-256");
            kparams.setProperty("sasl.jaas.config", jaasConfig);
            kparams.setProperty("ssl.truststore.type", "jks");
            kparams.setProperty("ssl.truststore.location", truststore_path);
            kparams.setProperty("ssl.truststore.password", params.getRequired("truststore.password"));

            // Setup streaming environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
            env.enableCheckpointing(300000); // 300 seconds
            env.getConfig().setGlobalJobParameters(params);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.setParallelism(1);

            DataStream<String> messageStream = env
                    .addSource(new FlinkKafkaConsumer011<>(
                            params.getRequired("read-topic"),
                            new SimpleStringSchema(),
                            kparams));

            // Print Kafka messages to stdout - will be visible in logs
            messageStream.print();

            env.execute("FlinkReadKafkaSASL");
        }
}
