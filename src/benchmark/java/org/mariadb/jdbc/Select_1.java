/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.jdbc;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.sql.ResultSet;
import java.sql.Statement;

public class Select_1 extends Common {

  @Benchmark
  public void testJdbc(MyState state, Blackhole blackhole) throws Throwable {
    Statement st = state.jdbc.createStatement();
    int rnd = (int) (Math.random() * 1000);
    ResultSet rs = st.executeQuery("select " + rnd);
    rs.next();
    Integer val = rs.getInt(1);
    if (rnd != val) throw new IllegalStateException("ERROR");
    blackhole.consume(val);
  }

}
