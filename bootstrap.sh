#!/usr/bin/env bash
fab remove-user create-user clean-host add-host clean-key add-key install-jdk install-scala set-ntp install-hadoop configure-hadoop format-hadoop start-hadoop install-spark configure-spark start-spark

./bin/spark-submit --class org.apache.spark.examples.SparkPi --master spark://sparkmaster:7077 examples/jars/spark-examples_2.11-2.4.4.jar 100

