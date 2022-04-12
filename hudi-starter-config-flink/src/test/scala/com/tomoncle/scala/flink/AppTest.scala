/*
 * Copyright 2018 tomoncle
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tomoncle.scala.flink

import java.net.URI

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.junit._

@Test
class AppTest {
  @Test
  def validatorS3AAuth(): Unit = {
    val property = ConfigFactory.load()
    val s3a: S3AFileSystem = new S3AFileSystem()
    val uri: URI = new URI("s3a://test-apache-hudi")
    val config: Configuration = new Configuration()
    config.set("fs.s3a.access.key", property.getString("hudi.storage.s3.accessKey"))
    config.set("fs.s3a.secret.key", property.getString("hudi.storage.s3.secretKey"))
    config.set("fs.s3a.endpoint", property.getString("hudi.storage.s3.endpoint"))
    config.set("fs.s3a.connection.ssl.enabled", property.getString("hudi.storage.s3.enableSSL"))
    //    config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    config.set("fs.s3a.path.style.access", "true")
    s3a.initialize(uri, config)
  }

}


