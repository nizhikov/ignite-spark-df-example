/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.scalar.examples.spark

import org.apache.ignite.spark.IgniteRelationProvider._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Example App to work with Ignite and Spark clusters.
  * Instructions to run:
  * <p>
  * # make current ignite release with: `mvn clean package -Pall-java,release -Dmaven.javadoc.skip=true`, `mvn initialize -Prelease`
  * # unpack apache-ignite-fabric into tmp folder and run 2 server nodes
  * # run `LoadCSVDataToIgnite` from IDE - IDEA spark plugin, right click on class name and Run
  * # download and unpack spark 2.2 distribution http://spark.apache.org/downloads.html
  * ## run ./sbin/start-master.sh (check http://localhost:8080 for spark GUI)
  * ## run ./sbin/start-slave.sh <SPARK-URL> (for me it a `spark://info-dep-564:7077` - printed in a master log at start)
  * # run `StandaloneClustersExample` from IDE
  * # ???
  * # PROFIT!!!
  */
object StandaloneClustersExample extends App {
    private val CONFIG = "ignite-config.xml"

    private val MAVEN_HOME = "/home/dragon/.m2/repository"

    implicit val spark = SparkSession.builder()
        .appName("Spark Ignite data sources example")
        .master("spark://172.17.0.2:7077")
        .getOrCreate()

    spark.sparkContext.addJar(MAVEN_HOME + "/org/apache/ignite/ignite-core/2.3.0-SNAPSHOT/ignite-core-2.3.0-SNAPSHOT.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/apache/ignite/ignite-spring/2.3.0-SNAPSHOT/ignite-spring-2.3.0-SNAPSHOT.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/apache/ignite/ignite-log4j/2.3.0-SNAPSHOT/ignite-log4j-2.3.0-SNAPSHOT.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/apache/ignite/ignite-spark/2.3.0-SNAPSHOT/ignite-spark-2.3.0-SNAPSHOT.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/apache/ignite/ignite-indexing/2.3.0-SNAPSHOT/ignite-indexing-2.3.0-SNAPSHOT.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/springframework/spring-beans/4.3.7.RELEASE/spring-beans-4.3.7.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/springframework/spring-core/4.3.7.RELEASE/spring-core-4.3.7.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/springframework/spring-context/4.3.7.RELEASE/spring-context-4.3.7.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/springframework/spring-expression/4.3.7.RELEASE/spring-expression-4.3.7.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/javax/cache/cache-api/1.0.0/cache-api-1.0.0.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/com/h2database/h2/1.4.195/h2-1.4.195.jar")

    //searchRussianPlayers(spark)

    //println("Search russian players...[DONE]")

    //searchTopPlayedMatchesInKeyValue(spark)

    searchTopPlayedMatchesInKeyValue(spark)

    println("Search players that played max matches...[DONE]")

    spark.close()

    def searchRussianPlayers(spark: SparkSession) = {
        val russianPlayers = spark.read
            .format(IGNITE)
            .option(TCP_IP_ADDRESSES, "172.17.0.1:47500..47509;127.17.0.2:47500..47509;127.17.0.3:47500..47509;127.17.0.4:47500..47509; 127.17.0.5:47500..47509;")
            .option(PEER_CLASS_LOADING, "true")
            .option(TABLE, "player")
            .load().filter(col("country") === "RUS")

        russianPlayers.printSchema()
        russianPlayers.show(10000)
    }

    def searchTopPlayedMatches(spark: SparkSession) = {
        spark.read
            .format(IGNITE)
            .option(TCP_IP_ADDRESSES, "172.17.0.1:47500..47509;127.17.0.2:47500..47509;127.17.0.3:47500..47509;127.17.0.4:47500..47509; 127.17.0.5:47500..47509;")
            .option(PEER_CLASS_LOADING, "true")
            .option(TABLE, "player")
            .load().createOrReplaceTempView("player")

        spark.read
            .format(IGNITE)
            .option(TCP_IP_ADDRESSES, "172.17.0.1:47500..47509;127.17.0.2:47500..47509;127.17.0.3:47500..47509;127.17.0.4:47500..47509; 127.17.0.5:47500..47509;")
            .option(PEER_CLASS_LOADING, "true")
            .option(TABLE, "match")
            .load().createOrReplaceTempView("match")

        val countMatches = spark.sql(
            """
              |  SELECT
              |    p.NAME,
              |    count(*) cnt
              |  FROM
              |      player p join
              |      match m on (p.id = m.winner_id) or (p.id = m.LOSER_ID)
              |  WHERE p.country = "RUS"
              |  GROUP BY p.NAME
              |  ORDER BY cnt DESC
              |  LIMIT 10
            """.stripMargin)

        countMatches.printSchema()
        countMatches.show()
    }

    def searchTopPlayedMatchesInKeyValue(spark: SparkSession) = {
        println(classOf[Player].getName)
        println(classOf[Match].getName)
        spark.read
            .format(IGNITE)
            .option(TCP_IP_ADDRESSES, "172.17.0.1:47500..47509;127.17.0.2:47500..47509;127.17.0.3:47500..47509;127.17.0.4:47500..47509; 127.17.0.5:47500..47509;")
            .option(PEER_CLASS_LOADING, "true")
            .option(CACHE, "player_kv")
            .option(KEY_CLASS, "java.lang.Long")
            .option(VALUE_CLASS, classOf[Player].getName)
            .option(KEEP_BINARY, "true")
            .load().createOrReplaceTempView("player_kv")

        spark.read
            .format(IGNITE)
            .option(TCP_IP_ADDRESSES, "172.17.0.1:47500..47509;127.17.0.2:47500..47509;127.17.0.3:47500..47509;127.17.0.4:47500..47509; 127.17.0.5:47500..47509;")
            .option(PEER_CLASS_LOADING, "true")
            .option(CACHE, "match_kv")
            .option(KEY_CLASS, "java.lang.Long")
            .option(VALUE_CLASS, classOf[Match].getName)
            .option(KEEP_BINARY, "true")
            .load().createOrReplaceTempView("match_kv")

        val countMatches = spark.sql(
            """
              |  SELECT
              |    p.`value.name`,
              |    count(*) cnt
              |  FROM
              |      player_kv p join
              |      match_kv m on (p.key = m.`value.winner_id`) or (p.key = m.`value.loser_id`)
              |  WHERE p.`value.country` = "RUS"
              |  GROUP BY p.`value.name`
              |  ORDER BY cnt DESC
              |  LIMIT 10
            """.stripMargin)

        countMatches.printSchema()
        countMatches.show()
    }
}


