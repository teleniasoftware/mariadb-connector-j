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

public class Select_1_user extends Common {

  final int numberOfUserCol = 46;


  @Benchmark
  public void testJdbc(MyState state, Blackhole blackhole) throws Throwable {
    Statement st = state.jdbc.createStatement();
    ResultSet rs = st.executeQuery("select * FROM mysql.user LIMIT 1");
    rs.next();
    Object[] objs = new Object[numberOfUserCol];
    for (int i = 0; i < numberOfUserCol; i++) {
      objs[i] = rs.getObject(i + 1);
    }
    blackhole.consume(objs);
  }

}
