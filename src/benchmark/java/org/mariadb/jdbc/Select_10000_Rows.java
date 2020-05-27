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

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Select_10000_Rows extends Common {
  private static final String sql =
      "SELECT lpad(conv(floor(rand()*pow(36,8)), 10, 36), 8, 0) as rnd_str_8 FROM seq_1_to_10000";

  @Benchmark
  public void testJdbc(MyState state, Blackhole blackhole) throws Throwable {
    PreparedStatement st = state.jdbc.prepareStatement(sql);

    ResultSet rs = st.executeQuery();
    String[] res = new String[10000];
    int i = 0;
    while (rs.next()) {
      res[i++] = rs.getString(1);
    }
    blackhole.consume(res);
  }

}
