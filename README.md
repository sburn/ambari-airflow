```
  ____________       _____________
 ____    |__( )_________  __/__  /________      __
____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
 _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/  v1.10.5
```
# Apache Airflow management pack for Apache Ambari

[![Airflow version](https://img.shields.io/badge/Airflow-1.10.5-brightgreen.svg)](https://github.com/sburn/ambari-airflow)
[![Python version](https://img.shields.io/badge/Python-3-brightgreen.svg)](https://github.com/sburn/ambari-airflow)
[![Executor type](https://img.shields.io/badge/Executor-Celery-brightgreen.svg)](https://github.com/sburn/ambari-airflow)
[![HDP version](https://img.shields.io/badge/HDP-3.1-brightgreen.svg)](https://github.com/sburn/ambari-airflow)
[![License](http://img.shields.io/:license-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

Mpack allows you to install/configure Airflow directly from Ambari.

Installing Apache Aiflow Mpack
------------------------------
1. Stop Ambari server.
2. Install the Apache Airflow Mpack on Ambari server.
3. Start Ambari server.

```
ambari-server stop
ambari-server install-mpack --mpack=airflow-mpack.tar.gz
ambari-server start
```

Upgrading Apache Aiflow Mpack
-----------------------------
1. Stop Ambari server.
2. Upgrade the Apache Airflow Mpack on Ambari server.
3. Start Ambari server.

```
ambari-server stop
ambari-server upgrade-mpack --mpack=airflow-mpack.tar.gz
ambari-server start
```

Installing Apache Airflow from Ambari
-------------------------------------
1. Action - Add service.
2. Select Apache Airflow service.
3. Choose destination server.
4. You may configure Apache Airflow, change home folder.
5. Deploy!

![Add service](https://github.com/sburn/ambari-airflow/blob/master/Screenshots/1.PNG)
![Select Apache Airflow service](https://github.com/sburn/ambari-airflow/blob/master/Screenshots/2.PNG)
![Choose destination server](https://github.com/sburn/ambari-airflow/blob/master/Screenshots/3.PNG)
![Choose destination server](https://github.com/sburn/ambari-airflow/blob/master/Screenshots/3-1.PNG)
![configure Apache Airflow](https://github.com/sburn/ambari-airflow/blob/master/Screenshots/4.PNG)
![Deploy](https://github.com/sburn/ambari-airflow/blob/master/Screenshots/5.PNG)
![Deploy](https://github.com/sburn/ambari-airflow/blob/master/Screenshots/6.PNG)
![Deploy](https://github.com/sburn/ambari-airflow/blob/master/Screenshots/8.PNG)
![Deploy](https://github.com/sburn/ambari-airflow/blob/master/Screenshots/10.PNG)


### Enjoy!
