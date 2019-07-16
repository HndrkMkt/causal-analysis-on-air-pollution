/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.dima.bdapro.jobs;

import de.tuberlin.dima.bdapro.sensor.SensorReadingCsvInputFormat;
import de.tuberlin.dima.bdapro.sensor.Type;
import de.tuberlin.dima.bdapro.sensor.UnifiedSensorReading;
import org.apache.flink.api.common.io.GlobFilePathFilter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;

import java.util.*;

abstract public class UnifiedSensorJob {
    protected static final Map<Type, String> sensorPatterns = new HashMap<Type, String>() {{
        put(Type.BME280, "bme280");
        put(Type.BMP180, "bmp180");
        put(Type.DHT22, "dht22");
        put(Type.DS18B20, "ds18b20");
        put(Type.HPM, "hpm");
        put(Type.HTU21D, "htu21d");
        put(Type.PMS3003, "pms3003");
        put(Type.PMS5003, "pms5003");
        put(Type.PMS7003, "pms7003");
        put(Type.PPD42NS, "ppd42ns");
        put(Type.SDS011, "sds011");
        put(Type.UNIFIED, "unified");
    }};

    protected static String getSensorPattern(Type sensorType) {
        return sensorPatterns.get(sensorType);
    }

    protected static DataSet<UnifiedSensorReading> readSensor(Type sensorType, String sensorDataBasePath, ExecutionEnvironment env, boolean compressed) {
        String suffix = compressed ? ".csv.gz" : ".csv";
        String sensorPattern = String.format("**/*%s" + suffix, getSensorPattern(sensorType));
        SensorReadingCsvInputFormat fileFormat = new SensorReadingCsvInputFormat(new Path(sensorDataBasePath), sensorType);
        fileFormat.setNestedFileEnumeration(true);
        fileFormat.setFilesFilter(new GlobFilePathFilter(Arrays.asList("**/", sensorPattern), new ArrayList<>()));
        return env.createInput(fileFormat, TypeInformation.of(UnifiedSensorReading.class)).setParallelism(1)
                .filter((UnifiedSensorReading reading) -> reading.sensorId != null);
    }

    protected static DataSet<UnifiedSensorReading> readSensor(Type sensorType, String sensorDataBasePath, ExecutionEnvironment env) {
        return readSensor(sensorType, sensorDataBasePath, env, true);
    }

    protected static DataSet<UnifiedSensorReading> readAllSensors(String sensorDataBasePath, ExecutionEnvironment env, boolean compressed) {
        ArrayList<DataSet<UnifiedSensorReading>> sensorReadingsList = new ArrayList<>();
        for (Type sensorType : Type.values()) {
            sensorReadingsList.add(readSensor(sensorType, sensorDataBasePath, env, compressed));
        }
        return unionAll(sensorReadingsList);
    }

    protected static <T> DataSet<T> unionAll(List<DataSet<T>> list) {
        DataSet<T> unionDataset = null;
        for (DataSet<T> dataset: list) {
            if (unionDataset == null) {
                unionDataset = dataset;
            } else {
                unionDataset = unionDataset.union(dataset);
            }
        }
        return unionDataset;
    }
}
