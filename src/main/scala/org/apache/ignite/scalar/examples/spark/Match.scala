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

/**
  */
case class Match(
    tourney_id: java.lang.String,
    tourney_name: java.lang.String,
    surface: java.lang.String,
    draw_size: java.lang.String,
    tourney_level: java.lang.String,
    tourney_date: java.util.Date,
    match_num: java.lang.Integer,
    winner_id: java.lang.Long,
    winner_seed: java.lang.Integer,
    winner_entry: java.lang.String,
    winner_name: java.lang.String,
    winner_hand: java.lang.String,
    winner_ht: java.lang.String,
    winner_ioc: java.lang.String,
    winner_age: java.lang.Double,
    winner_rank: java.lang.Integer,
    winner_rank_points: java.lang.Long,
    loser_id: java.lang.Long,
    loser_seed: java.lang.Integer,
    loser_entry: java.lang.String,
    loser_name: java.lang.String,
    loser_hand: java.lang.String,
    loser_ht: java.lang.String,
    loser_ioc: java.lang.String,
    loser_age: java.lang.Double,
    loser_rank: java.lang.Integer,
    loser_rank_points: java.lang.Long,
    score: java.lang.String,
    best_of: java.lang.Integer,
    round: java.lang.String,
    minutes: java.lang.Integer);
