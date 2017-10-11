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

import java.io.File
import java.text.SimpleDateFormat

import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.utils.closeAfter

import scala.io.Source

/**
  */
object LoadCSVDataToIgnite extends App {
    private val CONFIG = "ignite-config.xml"

    private val CACHE_NAME = "cache1"

    private val DATA_DIR_NAME = "/home/dragon/src/tennis_atp/"

    closeAfter(setupClient) { client ⇒
        createTables(client)

        insertMatches(client)
    }

    def insertMatches(client: Ignite): Unit = {
        val c = cache(client)

        val matchCache = createMatchCache(client)

        val playerCache = createPlayerCache(client)

        implicit val insertPlayerQuery = new SqlFieldsQuery("INSERT INTO player(id, name, country) VALUES(?, ?, ?)")

        implicit val insertMatchQuery = new SqlFieldsQuery(
            s"""
               | INSERT INTO match(id, tourney_id, tourney_name, surface, draw_size, tourney_level, tourney_date,
               | match_num, winner_id, winner_seed, winner_entry, winner_name, winner_hand, winner_ht, winner_ioc,
               | winner_age, winner_rank, winner_rank_points, loser_id, loser_seed, loser_entry, loser_name, loser_hand,
               | loser_ht, loser_ioc, loser_age, loser_rank, loser_rank_points, score, best_of, round, minutes)
               | VALUES (${Stream.continually("?").take(32).mkString(",")})
               | """.stripMargin)

        val idIterator = Iterator.from(1)

        val playersIDs = scala.collection.mutable.Set[String]()

        filesList(DATA_DIR_NAME).foreach { file ⇒
            val name = file.getName

            if (name.startsWith("atp_matches") && name.endsWith(".csv")) {
                print(s"Inserting matches from ${name}...")

                val matches = new CSVReader(file.getAbsolutePath)

                for (m ← matches) {
                    val matchID = idIterator.next
                    insertMatch(m, c, matchID, insertMatchQuery)

                    insertKVMatch(matchID, m, matchCache)


                    val winnerID = m("winner_id").toLong
                    if (!playersIDs.contains(m("winner_id"))) {
                        insertPlayer(m("winner_id"), m("winner_name"), m("winner_ioc"), c, insertPlayerQuery)

                        playerCache.put(winnerID, Player(m("winner_name"), m("winner_ioc")))

                        playersIDs += m("winner_id")
                    }

                    val loserID = m("loser_id").toLong
                    if (!playersIDs.contains(m("loser_id"))) {
                        insertPlayer(m("loser_id"), m("loser_name"), m("loser_ioc"), c, insertPlayerQuery)

                        playerCache.put(loserID, Player(m("loser_name"), m("loser_ioc")))

                        playersIDs += m("loser_id")
                    }
                }

                println(s"${idIterator.next()}, ${playersIDs.size} [DONE]")
            }
        }
    }

    def insertKVMatch(id: Long, m: Map[String, String], matchCache: IgniteCache[java.lang.Long, Match]): Unit =
        matchCache.put(id, Match(
            m.getOrNull("tourney_id"),
            m.getOrNull("tourney_name"),
            m.getOrNull("surface"),
            m.getOrNull("draw_size"),
            m.getOrNull("tourney_level"),
            toDate(m.getOrNull("tourney_date")),
            m.getIntOrNull("match_num"),
            m.getLongOrNull("winner_id"),
            m.getIntOrNull("winner_seed"),
            m.getOrNull("winner_entry"),
            m.getOrNull("winner_name"),
            m.getOrNull("winner_hand"),
            m.getOrNull("winner_ht"),
            m.getOrNull("winner_ioc"),
            m.getDoubleOrNull("winner_age"),
            m.getIntOrNull("winner_rank"),
            m.getLongOrNull("winner_rank_points"),
            m.getLongOrNull("loser_id"),
            m.getIntOrNull("loser_seed"),
            m.getOrNull("loser_entry"),
            m.getOrNull("loser_name"),
            m.getOrNull("loser_hand"),
            m.getOrNull("loser_ht"),
            m.getOrNull("loser_ioc"),
            m.getDoubleOrNull("loser_age"),
            m.getIntOrNull("loser_rank"),
            m.getLongOrNull("loser_rank_points"),
            m.getOrNull("score"),
            m.getIntOrNull("best_of"),
            m.getOrNull("round"),
            m.getIntOrNull("minutes"))
        )

    def insertPlayer(playerID: String, playerName: String, playerCountry: String, c: IgniteCache[_, _], insertPlayerQuery: SqlFieldsQuery) = {
        c.query(insertPlayerQuery.setArgs(playerID, playerName, playerCountry)).getAll
    }

    def insertMatch(m: Map[String, String], c: IgniteCache[_, _], id: Int, insertMatchQuery: SqlFieldsQuery) = {
        val matchArgs = List(
            id,
            m.getOrNull("tourney_id"),
            m.getOrNull("tourney_name"),
            m.getOrNull("surface"),
            m.getOrNull("draw_size"),
            m.getOrNull("tourney_level"),
            toDate(m.getOrNull("tourney_date")),
            m.getOrNull("match_num"),
            m.getOrNull("winner_id"),
            m.getOrNull("winner_seed"),
            m.getOrNull("winner_entry"),
            m.getOrNull("winner_name"),
            m.getOrNull("winner_hand"),
            m.getOrNull("winner_ht"),
            m.getOrNull("winner_ioc"),
            m.getOrNull("winner_age"),
            m.getOrNull("winner_rank"),
            m.getOrNull("winner_rank_points"),
            m.getOrNull("loser_id"),
            m.getOrNull("loser_seed"),
            m.getOrNull("loser_entry"),
            m.getOrNull("loser_name"),
            m.getOrNull("loser_hand"),
            m.getOrNull("loser_ht"),
            m.getOrNull("loser_ioc"),
            m.getOrNull("loser_age"),
            m.getOrNull("loser_rank"),
            m.getOrNull("loser_rank_points"),
            m.getOrNull("score"),
            m.getOrNull("best_of"),
            m.getOrNull("round"),
            m.getOrNull("minutes"))

        c.query(insertMatchQuery.setArgs(matchArgs.map(_.asInstanceOf[AnyRef]): _*)).getAll
    }

    def createTables(client: Ignite) = {
        print("Creating table...")
        if (!client.cacheNames().contains("SQL_PUBLIC_MATCH")) {
            val c = cache(client)

            c.query(new SqlFieldsQuery(
                """
                  | CREATE TABLE match (
                  |     id LONG PRIMARY KEY,
                  |     tourney_id VARCHAR(10),
                  |     tourney_name VARCHAR(50),
                  |     surface VARCHAR(10),
                  |     draw_size VARCHAR(5),
                  |     tourney_level VARCHAR(1),
                  |     tourney_date DATE,
                  |     match_num INT,
                  |     winner_id LONG,
                  |     winner_seed INT,
                  |     winner_entry VARCHAR(2),
                  |     winner_name VARCHAR(50),
                  |     winner_hand VARCHAR(1),
                  |     winner_ht VARCHAR(10),
                  |     winner_ioc VARCHAR(10),
                  |     winner_age DOUBLE,
                  |     winner_rank INT,
                  |     winner_rank_points LONG,
                  |     loser_id LONG,
                  |     loser_seed INT,
                  |     loser_entry VARCHAR(2),
                  |     loser_name VARCHAR(50),
                  |     loser_hand VARCHAR(1),
                  |     loser_ht VARCHAR(10),
                  |     loser_ioc VARCHAR(10),
                  |     loser_age DOUBLE,
                  |     loser_rank INT,
                  |     loser_rank_points LONG,
                  |     score VARCHAR(20),
                  |     best_of INT,
                  |     round VARCHAR(5),
                  |     minutes INT
                  | )
                """.stripMargin)).getAll

            c.query(new SqlFieldsQuery(
                """
                  | CREATE TABLE player (
                  |     id LONG PRIMARY KEY,
                  |     name VARCHAR(50),
                  |     country VARCHAR(10)
                  | )
                """.stripMargin)).getAll
        }
        println("[DONE]")
    }

    def createMatchCache(client: Ignite): IgniteCache[java.lang.Long, Match] = {
        print("Creating match cache...")

        val ccfg = new CacheConfiguration[java.lang.Long, Match]("match_kv")

        val cache = client.getOrCreateCache(ccfg)

        println("[DONE]")

        cache
    }

    def createPlayerCache(client: Ignite): IgniteCache[java.lang.Long, Player] = {
        print("Creating player cache...")

        val ccfg = new CacheConfiguration[java.lang.Long, Player]("player_kv")

        val cache = client.getOrCreateCache(ccfg)

        println("[DONE]")

        cache
    }

    def setupClient: Ignite = {
        print("Starting Ignite client...")
        val cfg = new IgniteConfiguration
        cfg.setClientMode(true)

        val ignite = Ignition.start(cfg)

        println("[DONE]")

        ignite
    }

    def cache(ignite: Ignite): IgniteCache[Any, Any] = {
        val ccfg = new CacheConfiguration[Any, Any](CACHE_NAME).setSqlSchema("PUBLIC")

        ignite.getOrCreateCache(ccfg)
    }

    def filesList(dir: String): Seq[File] = {
        val d = new File(dir)
        if (d.exists() && d.isDirectory)
            d.listFiles.filter(_.isFile).sorted
        else
            Seq.empty
    }

    def toDate(str: String) =
        if (str != null)
            new SimpleDateFormat("yyyyMMdd").parse(str)
        else
            null

    class CSVReader(file: String) extends Iterable[Map[String, String]] {
        private val lines = Source.fromFile(new File(file), "ISO-8859-1").getLines

        private val names = lines.next().split(",")

        override def iterator: Iterator[Map[String, String]] = new Iterator[Map[String, String]] {
            override def hasNext: Boolean = lines.hasNext

            override def next(): Map[String, String] = {
                val line = lines.next()
                val values = line.split(",", -1)

                names.zip(values).toMap
            }
        }
    }

    implicit class ExtendedMap(map: Map[String, String]) {
        def getOrNull(key: String) = {
            val value = map.getOrElse(key, null)
            if (value != "")
                value
            else
                null
        }

        def getIntOrNull(key: String): java.lang.Integer = {
            val value = map.getOrElse(key, null)
            if (value != "")
                value.toInt
            else
                null
        }

        def getLongOrNull(key: String): java.lang.Long = {
            val value = map.getOrElse(key, null)
            if (value != "")
                value.toLong
            else
                null
        }

        def getDoubleOrNull(key: String): java.lang.Double = {
            val value = map.getOrElse(key, null)
            if (value != "")
                value.toDouble
            else
                null
        }
    }
}
