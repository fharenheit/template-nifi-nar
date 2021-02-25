# NiFi NAR Sample

본 프로젝트는 Apache NiFi의 NAR(Nifi Archive)의 샘플 프로젝트입니다.

## Build

```bash
# mvn clean package
```

## NiFi Processor

`nifi-sample-processor-processors/src/main/resources/META-INF/services/org.apache.nifi.processor.Processor` 파일에 개발한 Processor를 등록합니다.

```
io.datadynamics.bigdata.nifi.processors.sample.UpdateRecord
io.datadynamics.bigdata.nifi.processors.sample.csv.CSVReader
```

NiFi Processor의 명칭은 Class명으로 결정이 되며 NiFi에 동일한 클래스명이 있는 경우 다음과 같이 `@Tags({"example"})`에 식별자를 추가하도록 합니다.

```java
@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"example"})
@SeeAlso({})
public class UpdateRecord extends AbstractRecordProcessor {
```

## Deployment

`*.nar` 파일을 배포하기 위해서 다음과 같이 `<NIFI_HOME>/lib`에 복사합니다.

```bash
# cp target/*.nar <NIFI_HOME>/lib
```

## Start

NiFi를 실행하기 위해서 다음의 커맨드를 실행합니다.

```
# cd <NIFI_HOME>/bin
# ./nifi.sh run
Java home: /usr/local/java/jdk1.8.0_251 
NiFi home: /root/nifi-1.13.0

Bootstrap Config File: /root/nifi-1.13.0/conf/bootstrap.conf

2021-02-24 15:06:51,678 INFO [main] org.apache.nifi.bootstrap.Command Starting Apache NiFi...
2021-02-24 15:06:51,678 INFO [main] org.apache.nifi.bootstrap.Command Working Directory: /root/nifi-1.13.0
2021-02-24 15:06:51,678 INFO [main] org.apache.nifi.bootstrap.Command Command: java -classpath /root/nifi-1.13.0/./conf:/root/nifi-1.13.0/./lib/javax.servlet-api-3.1.0.jar:/root/nifi-1.13.0/./lib/jetty-schemas-3.1.jar:/root/nifi-1.13.0/./lib/logback-classic-1.2.3.jar:/root/nifi-1.13.0/./lib/logback-core-1.2.3.jar:/root/nifi-1.13.0/./lib/jcl-over-slf4j-1.7.30.jar:/root/nifi-1.13.0/./lib/jul-to-slf4j-1.7.30.jar:/root/nifi-1.13.0/./lib/log4j-over-slf4j-1.7.30.jar:/root/nifi-1.13.0/./lib/slf4j-api-1.7.30.jar:/root/nifi-1.13.0/./lib/nifi-api-1.13.0.jar:/root/nifi-1.13.0/./lib/nifi-framework-api-1.13.0.jar:/root/nifi-1.13.0/./lib/nifi-server-api-1.13.0.jar:/root/nifi-1.13.0/./lib/nifi-runtime-1.13.0.jar:/root/nifi-1.13.0/./lib/nifi-nar-utils-1.13.0.jar:/root/nifi-1.13.0/./lib/nifi-properties-1.13.0.jar:/root/nifi-1.13.0/./lib/nifi-stateless-bootstrap-1.13.0.jar:/root/nifi-1.13.0/./lib/nifi-stateless-api-1.13.0.jar -Dorg.apache.jasper.compiler.disablejsr199=true -Xmx512m -Xms512m -Dcurator-log-only-first-connection-issue-as-error-level=true -Djavax.security.auth.useSubjectCredsOnly=true -Djava.security.egd=file:/dev/urandom -Dzookeeper.admin.enableServer=false -Dsun.net.http.allowRestrictedHeaders=true -Djava.net.preferIPv4Stack=true -Djava.awt.headless=true -Djava.protocol.handler.pkgs=sun.net.www.protocol -Dnifi.properties.file.path=/root/nifi-1.13.0/./conf/nifi.properties -Dnifi.bootstrap.listen.port=36853 -Dapp=NiFi -Dorg.apache.nifi.bootstrap.config.log.dir=/root/nifi-1.13.0/logs org.apache.nifi.NiFi 
2021-02-24 15:06:51,693 INFO [main] org.apache.nifi.bootstrap.Command Launched Apache NiFi with Process ID 8126
```