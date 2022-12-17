from __future__ import print_function
if __name__ == '__main__':
  from pyspark.sql import SparkSession

  spark = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
  print(spark.range(5000).where("id > 500").selectExpr("sum(id)").collect())




activation-1.1.1.jar                                   kubernetes-model-autoscaling-5.12.2.jar
aircompressor-0.21.jar                                 kubernetes-model-batch-5.12.2.jar
algebra_2.12-2.0.1.jar                                 kubernetes-model-certificates-5.12.2.jar
annotations-17.0.0.jar                                 kubernetes-model-common-5.12.2.jar
antlr4-runtime-4.8.jar                                 kubernetes-model-coordination-5.12.2.jar
antlr-runtime-3.5.2.jar                                kubernetes-model-core-5.12.2.jar
aopalliance-repackaged-2.6.1.jar                       kubernetes-model-discovery-5.12.2.jar
arpack-2.2.1.jar                                       kubernetes-model-events-5.12.2.jar
arpack_combined_all-0.1.jar                            kubernetes-model-extensions-5.12.2.jar
arrow-format-7.0.0.jar                                 kubernetes-model-flowcontrol-5.12.2.jar
arrow-memory-core-7.0.0.jar                            kubernetes-model-metrics-5.12.2.jar
arrow-memory-netty-7.0.0.jar                           kubernetes-model-networking-5.12.2.jar
arrow-vector-7.0.0.jar                                 kubernetes-model-node-5.12.2.jar
audience-annotations-0.5.0.jar                         kubernetes-model-policy-5.12.2.jar
automaton-1.11-8.jar                                   kubernetes-model-rbac-5.12.2.jar
avro-1.11.0.jar                                        kubernetes-model-scheduling-5.12.2.jar
avro-ipc-1.11.0.jar                                    kubernetes-model-storageclass-5.12.2.jar
avro-mapred-1.11.0.jar                                 lapack-2.2.1.jar
blas-2.2.1.jar                                         leveldbjni-all-1.8.jar
bonecp-0.8.0.RELEASE.jar                               libfb303-0.9.3.jar
breeze_2.12-1.2.jar                                    libthrift-0.12.0.jar
breeze-macros_2.12-1.2.jar                             log4j-1.2-api-2.17.2.jar
cats-kernel_2.12-2.1.1.jar                             log4j-api-2.17.2.jar
chill_2.12-0.10.0.jar                                  log4j-core-2.17.2.jar
chill-java-0.10.0.jar                                  log4j-slf4j-impl-2.17.2.jar
commons-cli-1.5.0.jar                                  logging-interceptor-3.12.12.jar
commons-collections-3.2.2.jar                          lz4-java-1.8.0.jar
commons-collections4-4.4.jar                           mesos-1.4.3-shaded-protobuf.jar
commons-compiler-3.0.16.jar                            META-INF
commons-compress-1.21.jar                              metrics-core-4.2.7.jar
commons-crypto-1.1.0.jar                               metrics-graphite-4.2.7.jar
commons-dbcp-1.4.jar                                   metrics-jmx-4.2.7.jar
commons-io-2.11.0.jar                                  metrics-json-4.2.7.jar
commons-lang-2.6.jar                                   metrics-jvm-4.2.7.jar
commons-lang3-3.12.0.jar                               minlog-1.3.0.jar
commons-logging-1.1.3.jar                              netty-all-4.1.74.Final.jar
commons-math3-3.6.1.jar                                netty-buffer-4.1.74.Final.jar
commons-pool-1.5.4.jar                                 netty-codec-4.1.74.Final.jar
commons-text-1.9.jar                                   netty-common-4.1.74.Final.jar
compress-lzf-1.1.jar                                   netty-handler-4.1.74.Final.jar
core-1.1.2.jar                                         netty-resolver-4.1.74.Final.jar
curator-client-2.13.0.jar                              netty-tcnative-classes-2.0.48.Final.jar
curator-framework-2.13.0.jar                           netty-transport-4.1.74.Final.jar
curator-recipes-2.13.0.jar                             netty-transport-classes-epoll-4.1.74.Final.jar
datanucleus-api-jdo-4.2.4.jar                          netty-transport-classes-kqueue-4.1.74.Final.jar
datanucleus-core-4.1.17.jar                            netty-transport-native-epoll-4.1.74.Final-linux-aarch_64.jar
datanucleus-rdbms-4.1.19.jar                           netty-transport-native-epoll-4.1.74.Final-linux-x86_64.jar
derby-10.14.2.0.jar                                    netty-transport-native-kqueue-4.1.74.Final-osx-aarch_64.jar
dropwizard-metrics-hadoop-metrics2-reporter-0.1.2.jar  netty-transport-native-kqueue-4.1.74.Final-osx-x86_64.jar
flatbuffers-java-1.12.0.jar                            netty-transport-native-unix-common-4.1.74.Final.jar
generex-1.0.2.jar                                      objenesis-3.2.jar
gson-2.2.4.jar                                         okhttp-3.12.12.jar
guava-14.0.1.jar                                       okio-1.14.0.jar
hadoop-client-api-3.3.2.jar                            opencsv-2.3.jar
hadoop-client-runtime-3.3.2.jar                        orc-core-1.7.6.jar
hadoop-shaded-guava-1.1.1.jar                          orc-mapreduce-1.7.6.jar
hadoop-yarn-server-web-proxy-3.3.2.jar                 orc-shims-1.7.6.jar
HikariCP-2.5.1.jar                                     oro-2.0.8.jar
hive-beeline-2.3.9.jar                                 osgi-resource-locator-1.0.3.jar
hive-cli-2.3.9.jar                                     paranamer-2.8.jar
hive-common-2.3.9.jar                                  parquet-column-1.12.2.jar
hive-exec-2.3.9-core.jar                               parquet-common-1.12.2.jar
hive-jdbc-2.3.9.jar                                    parquet-encoding-1.12.2.jar
hive-llap-common-2.3.9.jar                             parquet-format-structures-1.12.2.jar
hive-metastore-2.3.9.jar                               parquet-hadoop-1.12.2.jar
hive-serde-2.3.9.jar                                   parquet-jackson-1.12.2.jar
hive-service-rpc-3.1.2.jar                             pickle-1.2.jar
hive-shims-0.23-2.3.9.jar                              postgresql-42.5.0.jar
hive-shims-2.3.9.jar                                   protobuf-java-2.5.0.jar
hive-shims-common-2.3.9.jar                            py4j-0.10.9.5.jar
hive-shims-scheduler-2.3.9.jar                         redshift-jdbc42-2.1.0.9.jar
hive-storage-api-2.7.2.jar                             RoaringBitmap-0.9.25.jar
hive-vector-code-gen-2.3.9.jar                         rocksdbjni-6.20.3.jar
hk2-api-2.6.1.jar                                      scala-collection-compat_2.12-2.1.1.jar
hk2-locator-2.6.1.jar                                  scala-compiler-2.12.15.jar
hk2-utils-2.6.1.jar                                    scala-library-2.12.15.jar
httpcore-4.4.14.jar                                    scala-parser-combinators_2.12-1.1.2.jar
istack-commons-runtime-3.0.8.jar                       scala-reflect-2.12.15.jar
ivy-2.5.0.jar                                          scala-xml_2.12-1.2.0.jar
jackson-annotations-2.13.4.jar                         shapeless_2.12-2.3.7.jar
jackson-core-2.13.4.jar                                shims-0.9.25.jar
jackson-core-asl-1.9.13.jar                            slf4j-api-1.7.32.jar
jackson-databind-2.13.4.1.jar                          snakeyaml-1.31.jar
jackson-dataformat-yaml-2.13.4.jar                     snappy-java-1.1.8.4.jar
jackson-datatype-jsr310-2.13.4.jar                     spark-catalyst_2.12-3.3.1.jar
jackson-mapper-asl-1.9.13.jar                          spark-core_2.12-3.3.1.jar
jackson-module-scala_2.12-2.13.4.jar                   spark-graphx_2.12-3.3.1.jar
jakarta.annotation-api-1.3.5.jar                       spark-hive_2.12-3.3.1.jar
jakarta.inject-2.6.1.jar                               spark-hive-thriftserver_2.12-3.3.1.jar
jakarta.servlet-api-4.0.3.jar                          spark-kubernetes_2.12-3.3.1.jar
jakarta.validation-api-2.0.2.jar                       spark-kvstore_2.12-3.3.1.jar
jakarta.ws.rs-api-2.1.6.jar                            spark-launcher_2.12-3.3.1.jar
jakarta.xml.bind-api-2.3.2.jar                         spark-mesos_2.12-3.3.1.jar
janino-3.0.16.jar                                      spark-mllib_2.12-3.3.1.jar
javassist-3.25.0-GA.jar                                spark-mllib-local_2.12-3.3.1.jar
javax.jdo-3.2.0-m3.jar                                 spark-network-common_2.12-3.3.1.jar
javolution-5.5.1.jar                                   spark-network-shuffle_2.12-3.3.1.jar
jaxb-runtime-2.3.2.jar                                 spark-repl_2.12-3.3.1.jar
jcl-over-slf4j-1.7.32.jar                              spark-sketch_2.12-3.3.1.jar
jdo-api-3.0.1.jar                                      spark-sql_2.12-3.3.1.jar
jersey-client-2.36.jar                                 spark-streaming_2.12-3.3.1.jar
jersey-common-2.36.jar                                 spark-tags_2.12-3.3.1.jar
jersey-container-servlet-2.36.jar                      spark-tags_2.12-3.3.1-tests.jar
jersey-container-servlet-core-2.36.jar                 spark-unsafe_2.12-3.3.1.jar
jersey-hk2-2.36.jar                                    spark-yarn_2.12-3.3.1.jar
jersey-server-2.36.jar                                 spire_2.12-0.17.0.jar
JLargeArrays-1.5.jar                                   spire-macros_2.12-0.17.0.jar
jline-2.14.6.jar                                       spire-platform_2.12-0.17.0.jar
joda-time-2.10.13.jar                                  spire-util_2.12-0.17.0.jar
jodd-core-3.5.2.jar                                    ST4-4.0.4.jar
jpam-1.1.jar                                           stax-api-1.0.1.jar
json-1.8.jar                                           stream-2.9.6.jar
json4s-ast_2.12-3.7.0-M11.jar                          super-csv-2.2.0.jar
json4s-core_2.12-3.7.0-M11.jar                         threeten-extra-1.5.0.jar
json4s-jackson_2.12-3.7.0-M11.jar                      tink-1.6.1.jar
json4s-scalap_2.12-3.7.0-M11.jar                       transaction-api-1.1.jar
jsr305-3.0.0.jar                                       univocity-parsers-2.9.1.jar
jta-1.1.jar                                            velocity-1.5.jar
JTransforms-3.1.jar                                    xbean-asm9-shaded-4.20.jar
jul-to-slf4j-1.7.32.jar                                xz-1.8.jar
kryo-shaded-4.0.2.jar                                  zjsonpatch-0.3.0.jar
kubernetes-client-5.12.2.jar                           zookeeper-3.6.2.jar
kubernetes-model-admissionregistration-5.12.2.jar      zookeeper-jute-3.6.2.jar
kubernetes-model-apiextensions-5.12.2.jar              zstd-jni-1.5.2-1.jar
kubernetes-model-apps-5.12.2.jar


