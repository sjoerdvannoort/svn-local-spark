# Hive 

## Enabling hive support

To successfully enable hive support on windows, make sure hadoop.dll and winutils.dll are in a folder that is in the PATH environment variable. 

## Start Hive Thrift Server from python

With below code snippet the thrift server can be started from python code
```
# Start Hive Thrift Server
java_args = sc._gateway.new_array(sc._gateway.jvm.java.lang.String,2)
java_args[0]="dummy"    
java_args[1]="false"

sc._jvm.org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.main(java_args)
```

See the example in the jupyter/Hive.ipynb notebook

## Hive Thrift Server configuration

This configuration example should be saved as hive_site.xml to the conf folder of the spark installation, for instance C:\Program Files\spark-3.5.4-bin-hadoop3-scala2.13\conf


```
<configuration>
<property>
  <name>hive.server2.authentication</name>
  <value>NOSASL</value>
  <description>Authentication mode, default NONE. Options are NONE (uses plain SASL), NOSASL, KERBEROS, LDAP, PAM and CUSTOM</description>
</property>
<property>
  <name>hive.server2.thrift.port</name>
  <value>10001</value>
  <description>Port number for HiveServer2 Thrift interface. defaults to 10000</description>
</property>
<property>
  <name>hive.server2.thrift.http.port</name>
  <value>10001</value>
  <description>Http Port number. defaults to 10001</description>
</property>
<property>
  <name>hive.server2.transport.mode</name>
  <value>http</value>
  <description>binary (default) or http</description>
</property>
<property>
  <name>hive.metastore.local</name>
  <value>true</value>
</property>
<property>
  <name>hive.execution.engine</name>
  <value>spark</value>
</property>
</configuration>
```

## Connecting to spark from Power BI Desktop

With the above configuration, and a hive enabled spark session running, it is possible to connect PBI Desktop.
As data source connector, choose "spark"
In the Spark dialog: 
Server: http://localhost:10001/cliservice
Protocol: HTTP
In the authentication popup, choose the Username/Password tab and enter a random user name and password.
