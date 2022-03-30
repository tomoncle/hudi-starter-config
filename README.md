# hudi 操作配置示例

## 环境依赖

需要安装的软件包

* java: `1.8`
```bash
# java
export JAVA_HOME=/usr/local/jdk8
export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
export PATH=$JAVA_HOME/bin:$PATH
```

* maven: `3.6+`
```bash
export M2_HOME=/usr/local/maven
export PATH=$PATH:$M2_HOME/bin
```

* scala: `2.12.10`
```bash
export SCALA_HOME=/usr/local/scala-2.12.10
export PATH=$PATH:$SCALA_HOME/bin
```

* hadoop: `2.7.3`
```bash
export HADOOP_HOME=/usr/local/hadoop-2.7.3
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export PATH=$PATH:$HADOOP_HOME/bin
```

## 软件依赖

* spark : `2.4.8 (spark-2.4.8-bin-hadoop2.7)`
* hudi : `0.10.1`