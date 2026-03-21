{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "8c114adc-3a4b-434a-b98b-07936ec9f439",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "✅ Spark running: 3.5.0\n",
      "✅ Hadoop home set!\n"
     ]
    }
   ],
   "source": [
    "import os\n",
    "os.environ[\"HADOOP_HOME\"] = \"C:\\\\hadoop\"\n",
    "os.environ[\"PATH\"] = os.environ[\"PATH\"] + \";C:\\\\hadoop\\\\bin\"\n",
    "\n",
    "import findspark\n",
    "findspark.init()\n",
    "\n",
    "from pyspark.sql import SparkSession\n",
    "\n",
    "spark = SparkSession.builder \\\n",
    "    .appName(\"SingaporeWeatherETL\") \\\n",
    "    .master(\"local[*]\") \\\n",
    "    .getOrCreate()\n",
    "\n",
    "print(\"✅ Spark running:\", spark.version)\n",
    "print(\"✅ Hadoop home set!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "0c8f395b-96c8-4d2c-a24e-170f3bf6b0ae",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "✅ Loaded air_temperature\n",
      "✅ Loaded relative_humidity\n",
      "✅ Loaded rainfall\n",
      "✅ Loaded wind_speed\n",
      "✅ Loaded wind_direction\n",
      "\n",
      "✅ All raw data loaded!\n"
     ]
    }
   ],
   "source": [
    "import json\n",
    "\n",
    "raw_data = {}\n",
    "\n",
    "metrics = [\n",
    "    \"air_temperature\",\n",
    "    \"relative_humidity\", \n",
    "    \"rainfall\",\n",
    "    \"wind_speed\",\n",
    "    \"wind_direction\"\n",
    "]\n",
    "\n",
    "for metric in metrics:\n",
    "    filename = f\"raw_data/{metric}_raw.json\"\n",
    "    with open(filename, \"r\") as f:\n",
    "        raw_data[metric] = json.load(f)\n",
    "    print(f\"✅ Loaded {metric}\")\n",
    "\n",
    "print(\"\\n✅ All raw data loaded!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "fdc1e1bb-929b-4644-a924-0b2d7d704c97",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "✅ Temperature rows: 2612\n",
      "✅ Humidity rows: 2612\n",
      "✅ Rainfall rows: 10818\n",
      "✅ Wind speed rows: 2611\n",
      "✅ Wind direction rows: 2606\n"
     ]
    }
   ],
   "source": [
    "import pandas as pd\n",
    "\n",
    "def flatten_weather(raw_records, metric_field):\n",
    "    rows = []\n",
    "    for day_data in raw_records:\n",
    "        try:\n",
    "            readings = day_data[\"data\"][\"readings\"]\n",
    "            stations = {s[\"id\"]: s for s in day_data[\"data\"][\"stations\"]}\n",
    "            for reading in readings:\n",
    "                timestamp = reading[\"timestamp\"]\n",
    "                for item in reading[\"data\"]:\n",
    "                    station_id = item[\"stationId\"]\n",
    "                    value = float(item[\"value\"])\n",
    "                    station_info = stations.get(station_id, {})\n",
    "                    rows.append({\n",
    "                        \"timestamp\": timestamp,\n",
    "                        \"station_id\": station_id,\n",
    "                        \"station_name\": station_info.get(\"name\", \"\"),\n",
    "                        \"latitude\": float(station_info.get(\"location\", {}).get(\"latitude\", 0)),\n",
    "                        \"longitude\": float(station_info.get(\"location\", {}).get(\"longitude\", 0)),\n",
    "                        metric_field: value\n",
    "                    })\n",
    "        except Exception as e:\n",
    "            pass\n",
    "    return rows\n",
    "\n",
    "# Convert to Spark DataFrames via Pandas\n",
    "df_temp   = spark.createDataFrame(pd.DataFrame(flatten_weather(raw_data[\"air_temperature\"], \"temperature_c\")))\n",
    "df_humid  = spark.createDataFrame(pd.DataFrame(flatten_weather(raw_data[\"relative_humidity\"], \"humidity_pct\")))\n",
    "df_rain   = spark.createDataFrame(pd.DataFrame(flatten_weather(raw_data[\"rainfall\"], \"rainfall_mm\")))\n",
    "df_wspeed = spark.createDataFrame(pd.DataFrame(flatten_weather(raw_data[\"wind_speed\"], \"wind_speed_kmh\")))\n",
    "df_wdir   = spark.createDataFrame(pd.DataFrame(flatten_weather(raw_data[\"wind_direction\"], \"wind_direction_deg\")))\n",
    "\n",
    "print(\"✅ Temperature rows:\", df_temp.count())\n",
    "print(\"✅ Humidity rows:\", df_humid.count())\n",
    "print(\"✅ Rainfall rows:\", df_rain.count())\n",
    "print(\"✅ Wind speed rows:\", df_wspeed.count())\n",
    "print(\"✅ Wind direction rows:\", df_wdir.count())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "62275e7e-f38c-4b56-a9e3-85e045ccb273",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "✅ Transformations done!\n",
      "+--------------------+----------+----------+\n",
      "|        station_name|avg_temp_c|max_temp_c|\n",
      "+--------------------+----------+----------+\n",
      "|             Sentosa|     28.23|      33.7|\n",
      "|Semakau Island La...|     28.16|      31.0|\n",
      "|  West Coast Highway|     28.01|      32.4|\n",
      "|  Paya Lebar Airport|     27.96|      32.6|\n",
      "| Tuas South Avenue 3|     27.87|      31.9|\n",
      "+--------------------+----------+----------+\n",
      "only showing top 5 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# Register all as temp views\n",
    "df_temp.createOrReplaceTempView(\"temperature\")\n",
    "df_humid.createOrReplaceTempView(\"humidity\")\n",
    "df_rain.createOrReplaceTempView(\"rainfall\")\n",
    "df_wspeed.createOrReplaceTempView(\"wind_speed\")\n",
    "\n",
    "# 1. Daily average temperature per station\n",
    "daily_temp = spark.sql(\"\"\"\n",
    "    SELECT DATE(timestamp) as date,\n",
    "           station_name,\n",
    "           ROUND(AVG(temperature_c), 2) as avg_temp_c,\n",
    "           ROUND(MAX(temperature_c), 2) as max_temp_c\n",
    "    FROM temperature\n",
    "    GROUP BY DATE(timestamp), station_name\n",
    "    ORDER BY date, avg_temp_c DESC\n",
    "\"\"\")\n",
    "\n",
    "# 2. Hottest stations overall\n",
    "hottest_stations = spark.sql(\"\"\"\n",
    "    SELECT station_name,\n",
    "           ROUND(AVG(temperature_c), 2) as avg_temp_c,\n",
    "           ROUND(MAX(temperature_c), 2) as max_temp_c\n",
    "    FROM temperature\n",
    "    GROUP BY station_name\n",
    "    ORDER BY avg_temp_c DESC\n",
    "    LIMIT 10\n",
    "\"\"\")\n",
    "\n",
    "# 3. Rainiest days\n",
    "rainiest_days = spark.sql(\"\"\"\n",
    "    SELECT DATE(timestamp) as date,\n",
    "           ROUND(SUM(rainfall_mm), 2) as total_rainfall_mm\n",
    "    FROM rainfall\n",
    "    GROUP BY DATE(timestamp)\n",
    "    ORDER BY total_rainfall_mm DESC\n",
    "\"\"\")\n",
    "\n",
    "# 4. Daily humidity summary\n",
    "daily_humidity = spark.sql(\"\"\"\n",
    "    SELECT DATE(timestamp) as date,\n",
    "           ROUND(AVG(humidity_pct), 2) as avg_humidity_pct,\n",
    "           ROUND(MAX(humidity_pct), 2) as max_humidity_pct\n",
    "    FROM humidity\n",
    "    GROUP BY DATE(timestamp)\n",
    "    ORDER BY date\n",
    "\"\"\")\n",
    "\n",
    "print(\"✅ Transformations done!\")\n",
    "hottest_stations.show(5)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "453059fc-051c-4745-8ff9-f00c501c2c24",
   "metadata": {},
   "outputs": [
    {
     "ename": "Py4JJavaError",
     "evalue": "An error occurred while calling o149.parquet.\n: java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'\r\n\tat org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Native Method)\r\n\tat org.apache.hadoop.io.nativeio.NativeIO$Windows.access(NativeIO.java:793)\r\n\tat org.apache.hadoop.fs.FileUtil.canRead(FileUtil.java:1249)\r\n\tat org.apache.hadoop.fs.FileUtil.list(FileUtil.java:1454)\r\n\tat org.apache.hadoop.fs.RawLocalFileSystem.listStatus(RawLocalFileSystem.java:601)\r\n\tat org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:1972)\r\n\tat org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:2014)\r\n\tat org.apache.hadoop.fs.ChecksumFileSystem.listStatus(ChecksumFileSystem.java:761)\r\n\tat org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:1972)\r\n\tat org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:2014)\r\n\tat org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.getAllCommittedTaskPaths(FileOutputCommitter.java:334)\r\n\tat org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.commitJobInternal(FileOutputCommitter.java:404)\r\n\tat org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.commitJob(FileOutputCommitter.java:377)\r\n\tat org.apache.parquet.hadoop.ParquetOutputCommitter.commitJob(ParquetOutputCommitter.java:48)\r\n\tat org.apache.spark.internal.io.HadoopMapReduceCommitProtocol.commitJob(HadoopMapReduceCommitProtocol.scala:192)\r\n\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$writeAndCommit$3(FileFormatWriter.scala:275)\r\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)\r\n\tat org.apache.spark.util.Utils$.timeTakenMs(Utils.scala:552)\r\n\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.writeAndCommit(FileFormatWriter.scala:275)\r\n\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeWrite(FileFormatWriter.scala:304)\r\n\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:190)\r\n\tat org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:190)\r\n\tat org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:113)\r\n\tat org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:111)\r\n\tat org.apache.spark.sql.execution.command.DataWritingCommandExec.executeCollect(commands.scala:125)\r\n\tat org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec.$anonfun$executeCollect$1(AdaptiveSparkPlanExec.scala:374)\r\n\tat org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec.withFinalPlanUpdate(AdaptiveSparkPlanExec.scala:402)\r\n\tat org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec.executeCollect(AdaptiveSparkPlanExec.scala:374)\r\n\tat org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:107)\r\n\tat org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)\r\n\tat org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)\r\n\tat org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)\r\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)\r\n\tat org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)\r\n\tat org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:107)\r\n\tat org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)\r\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)\r\n\tat org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)\r\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)\r\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)\r\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)\r\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)\r\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)\r\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)\r\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)\r\n\tat org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:98)\r\n\tat org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:85)\r\n\tat org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:83)\r\n\tat org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:142)\r\n\tat org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:859)\r\n\tat org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:388)\r\n\tat org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:361)\r\n\tat org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:240)\r\n\tat org.apache.spark.sql.DataFrameWriter.parquet(DataFrameWriter.scala:792)\r\n\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\r\n\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:75)\r\n\tat java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:52)\r\n\tat java.base/java.lang.reflect.Method.invoke(Method.java:580)\r\n\tat py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)\r\n\tat py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)\r\n\tat py4j.Gateway.invoke(Gateway.java:282)\r\n\tat py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)\r\n\tat py4j.commands.CallCommand.execute(CallCommand.java:79)\r\n\tat py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)\r\n\tat py4j.ClientServerConnection.run(ClientServerConnection.java:106)\r\n\tat java.base/java.lang.Thread.run(Thread.java:1583)\r\n",
     "output_type": "error",
     "traceback": [
      "\u001b[31m---------------------------------------------------------------------------\u001b[39m",
      "\u001b[31mPy4JJavaError\u001b[39m                             Traceback (most recent call last)",
      "\u001b[36mCell\u001b[39m\u001b[36m \u001b[39m\u001b[32mIn[5]\u001b[39m\u001b[32m, line 7\u001b[39m\n\u001b[32m      4\u001b[39m os.makedirs(\u001b[33m\"\u001b[39m\u001b[33mcurated_data\u001b[39m\u001b[33m\"\u001b[39m, exist_ok=\u001b[38;5;28;01mTrue\u001b[39;00m)\n\u001b[32m      6\u001b[39m \u001b[38;5;66;03m# Save all 4 tables as Parquet\u001b[39;00m\n\u001b[32m----> \u001b[39m\u001b[32m7\u001b[39m \u001b[43mdaily_temp\u001b[49m\u001b[43m.\u001b[49m\u001b[43mwrite\u001b[49m\u001b[43m.\u001b[49m\u001b[43mmode\u001b[49m\u001b[43m(\u001b[49m\u001b[33;43m\"\u001b[39;49m\u001b[33;43moverwrite\u001b[39;49m\u001b[33;43m\"\u001b[39;49m\u001b[43m)\u001b[49m\u001b[43m.\u001b[49m\u001b[43mparquet\u001b[49m\u001b[43m(\u001b[49m\u001b[33;43m\"\u001b[39;49m\u001b[33;43mcurated_data/daily_temp\u001b[39;49m\u001b[33;43m\"\u001b[39;49m\u001b[43m)\u001b[49m\n\u001b[32m      8\u001b[39m hottest_stations.write.mode(\u001b[33m\"\u001b[39m\u001b[33moverwrite\u001b[39m\u001b[33m\"\u001b[39m).parquet(\u001b[33m\"\u001b[39m\u001b[33mcurated_data/hottest_stations\u001b[39m\u001b[33m\"\u001b[39m)\n\u001b[32m      9\u001b[39m rainiest_days.write.mode(\u001b[33m\"\u001b[39m\u001b[33moverwrite\u001b[39m\u001b[33m\"\u001b[39m).parquet(\u001b[33m\"\u001b[39m\u001b[33mcurated_data/rainiest_days\u001b[39m\u001b[33m\"\u001b[39m)\n",
      "\u001b[36mFile \u001b[39m\u001b[32m~\\anaconda3\\envs\\pyspark_env\\Lib\\site-packages\\pyspark\\sql\\readwriter.py:1721\u001b[39m, in \u001b[36mDataFrameWriter.parquet\u001b[39m\u001b[34m(self, path, mode, partitionBy, compression)\u001b[39m\n\u001b[32m   1719\u001b[39m     \u001b[38;5;28mself\u001b[39m.partitionBy(partitionBy)\n\u001b[32m   1720\u001b[39m \u001b[38;5;28mself\u001b[39m._set_opts(compression=compression)\n\u001b[32m-> \u001b[39m\u001b[32m1721\u001b[39m \u001b[38;5;28;43mself\u001b[39;49m\u001b[43m.\u001b[49m\u001b[43m_jwrite\u001b[49m\u001b[43m.\u001b[49m\u001b[43mparquet\u001b[49m\u001b[43m(\u001b[49m\u001b[43mpath\u001b[49m\u001b[43m)\u001b[49m\n",
      "\u001b[36mFile \u001b[39m\u001b[32m~\\anaconda3\\envs\\pyspark_env\\Lib\\site-packages\\py4j\\java_gateway.py:1322\u001b[39m, in \u001b[36mJavaMember.__call__\u001b[39m\u001b[34m(self, *args)\u001b[39m\n\u001b[32m   1316\u001b[39m command = proto.CALL_COMMAND_NAME +\\\n\u001b[32m   1317\u001b[39m     \u001b[38;5;28mself\u001b[39m.command_header +\\\n\u001b[32m   1318\u001b[39m     args_command +\\\n\u001b[32m   1319\u001b[39m     proto.END_COMMAND_PART\n\u001b[32m   1321\u001b[39m answer = \u001b[38;5;28mself\u001b[39m.gateway_client.send_command(command)\n\u001b[32m-> \u001b[39m\u001b[32m1322\u001b[39m return_value = \u001b[43mget_return_value\u001b[49m\u001b[43m(\u001b[49m\n\u001b[32m   1323\u001b[39m \u001b[43m    \u001b[49m\u001b[43manswer\u001b[49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;28;43mself\u001b[39;49m\u001b[43m.\u001b[49m\u001b[43mgateway_client\u001b[49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;28;43mself\u001b[39;49m\u001b[43m.\u001b[49m\u001b[43mtarget_id\u001b[49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;28;43mself\u001b[39;49m\u001b[43m.\u001b[49m\u001b[43mname\u001b[49m\u001b[43m)\u001b[49m\n\u001b[32m   1325\u001b[39m \u001b[38;5;28;01mfor\u001b[39;00m temp_arg \u001b[38;5;129;01min\u001b[39;00m temp_args:\n\u001b[32m   1326\u001b[39m     \u001b[38;5;28;01mif\u001b[39;00m \u001b[38;5;28mhasattr\u001b[39m(temp_arg, \u001b[33m\"\u001b[39m\u001b[33m_detach\u001b[39m\u001b[33m\"\u001b[39m):\n",
      "\u001b[36mFile \u001b[39m\u001b[32m~\\anaconda3\\envs\\pyspark_env\\Lib\\site-packages\\pyspark\\errors\\exceptions\\captured.py:179\u001b[39m, in \u001b[36mcapture_sql_exception.<locals>.deco\u001b[39m\u001b[34m(*a, **kw)\u001b[39m\n\u001b[32m    177\u001b[39m \u001b[38;5;28;01mdef\u001b[39;00m\u001b[38;5;250m \u001b[39m\u001b[34mdeco\u001b[39m(*a: Any, **kw: Any) -> Any:\n\u001b[32m    178\u001b[39m     \u001b[38;5;28;01mtry\u001b[39;00m:\n\u001b[32m--> \u001b[39m\u001b[32m179\u001b[39m         \u001b[38;5;28;01mreturn\u001b[39;00m \u001b[43mf\u001b[49m\u001b[43m(\u001b[49m\u001b[43m*\u001b[49m\u001b[43ma\u001b[49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[43m*\u001b[49m\u001b[43m*\u001b[49m\u001b[43mkw\u001b[49m\u001b[43m)\u001b[49m\n\u001b[32m    180\u001b[39m     \u001b[38;5;28;01mexcept\u001b[39;00m Py4JJavaError \u001b[38;5;28;01mas\u001b[39;00m e:\n\u001b[32m    181\u001b[39m         converted = convert_exception(e.java_exception)\n",
      "\u001b[36mFile \u001b[39m\u001b[32m~\\anaconda3\\envs\\pyspark_env\\Lib\\site-packages\\py4j\\protocol.py:326\u001b[39m, in \u001b[36mget_return_value\u001b[39m\u001b[34m(answer, gateway_client, target_id, name)\u001b[39m\n\u001b[32m    324\u001b[39m value = OUTPUT_CONVERTER[\u001b[38;5;28mtype\u001b[39m](answer[\u001b[32m2\u001b[39m:], gateway_client)\n\u001b[32m    325\u001b[39m \u001b[38;5;28;01mif\u001b[39;00m answer[\u001b[32m1\u001b[39m] == REFERENCE_TYPE:\n\u001b[32m--> \u001b[39m\u001b[32m326\u001b[39m     \u001b[38;5;28;01mraise\u001b[39;00m Py4JJavaError(\n\u001b[32m    327\u001b[39m         \u001b[33m\"\u001b[39m\u001b[33mAn error occurred while calling \u001b[39m\u001b[38;5;132;01m{0}\u001b[39;00m\u001b[38;5;132;01m{1}\u001b[39;00m\u001b[38;5;132;01m{2}\u001b[39;00m\u001b[33m.\u001b[39m\u001b[38;5;130;01m\\n\u001b[39;00m\u001b[33m\"\u001b[39m.\n\u001b[32m    328\u001b[39m         \u001b[38;5;28mformat\u001b[39m(target_id, \u001b[33m\"\u001b[39m\u001b[33m.\u001b[39m\u001b[33m\"\u001b[39m, name), value)\n\u001b[32m    329\u001b[39m \u001b[38;5;28;01melse\u001b[39;00m:\n\u001b[32m    330\u001b[39m     \u001b[38;5;28;01mraise\u001b[39;00m Py4JError(\n\u001b[32m    331\u001b[39m         \u001b[33m\"\u001b[39m\u001b[33mAn error occurred while calling \u001b[39m\u001b[38;5;132;01m{0}\u001b[39;00m\u001b[38;5;132;01m{1}\u001b[39;00m\u001b[38;5;132;01m{2}\u001b[39;00m\u001b[33m. Trace:\u001b[39m\u001b[38;5;130;01m\\n\u001b[39;00m\u001b[38;5;132;01m{3}\u001b[39;00m\u001b[38;5;130;01m\\n\u001b[39;00m\u001b[33m\"\u001b[39m.\n\u001b[32m    332\u001b[39m         \u001b[38;5;28mformat\u001b[39m(target_id, \u001b[33m\"\u001b[39m\u001b[33m.\u001b[39m\u001b[33m\"\u001b[39m, name, value))\n",
      "\u001b[31mPy4JJavaError\u001b[39m: An error occurred while calling o149.parquet.\n: java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'\r\n\tat org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Native Method)\r\n\tat org.apache.hadoop.io.nativeio.NativeIO$Windows.access(NativeIO.java:793)\r\n\tat org.apache.hadoop.fs.FileUtil.canRead(FileUtil.java:1249)\r\n\tat org.apache.hadoop.fs.FileUtil.list(FileUtil.java:1454)\r\n\tat org.apache.hadoop.fs.RawLocalFileSystem.listStatus(RawLocalFileSystem.java:601)\r\n\tat org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:1972)\r\n\tat org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:2014)\r\n\tat org.apache.hadoop.fs.ChecksumFileSystem.listStatus(ChecksumFileSystem.java:761)\r\n\tat org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:1972)\r\n\tat org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:2014)\r\n\tat org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.getAllCommittedTaskPaths(FileOutputCommitter.java:334)\r\n\tat org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.commitJobInternal(FileOutputCommitter.java:404)\r\n\tat org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.commitJob(FileOutputCommitter.java:377)\r\n\tat org.apache.parquet.hadoop.ParquetOutputCommitter.commitJob(ParquetOutputCommitter.java:48)\r\n\tat org.apache.spark.internal.io.HadoopMapReduceCommitProtocol.commitJob(HadoopMapReduceCommitProtocol.scala:192)\r\n\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$writeAndCommit$3(FileFormatWriter.scala:275)\r\n\tat scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)\r\n\tat org.apache.spark.util.Utils$.timeTakenMs(Utils.scala:552)\r\n\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.writeAndCommit(FileFormatWriter.scala:275)\r\n\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeWrite(FileFormatWriter.scala:304)\r\n\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:190)\r\n\tat org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:190)\r\n\tat org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:113)\r\n\tat org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:111)\r\n\tat org.apache.spark.sql.execution.command.DataWritingCommandExec.executeCollect(commands.scala:125)\r\n\tat org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec.$anonfun$executeCollect$1(AdaptiveSparkPlanExec.scala:374)\r\n\tat org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec.withFinalPlanUpdate(AdaptiveSparkPlanExec.scala:402)\r\n\tat org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec.executeCollect(AdaptiveSparkPlanExec.scala:374)\r\n\tat org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:107)\r\n\tat org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)\r\n\tat org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)\r\n\tat org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)\r\n\tat org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)\r\n\tat org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)\r\n\tat org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:107)\r\n\tat org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)\r\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)\r\n\tat org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)\r\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)\r\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)\r\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)\r\n\tat org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)\r\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)\r\n\tat org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)\r\n\tat org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)\r\n\tat org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:98)\r\n\tat org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:85)\r\n\tat org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:83)\r\n\tat org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:142)\r\n\tat org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:859)\r\n\tat org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:388)\r\n\tat org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:361)\r\n\tat org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:240)\r\n\tat org.apache.spark.sql.DataFrameWriter.parquet(DataFrameWriter.scala:792)\r\n\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\r\n\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:75)\r\n\tat java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:52)\r\n\tat java.base/java.lang.reflect.Method.invoke(Method.java:580)\r\n\tat py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)\r\n\tat py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)\r\n\tat py4j.Gateway.invoke(Gateway.java:282)\r\n\tat py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)\r\n\tat py4j.commands.CallCommand.execute(CallCommand.java:79)\r\n\tat py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)\r\n\tat py4j.ClientServerConnection.run(ClientServerConnection.java:106)\r\n\tat java.base/java.lang.Thread.run(Thread.java:1583)\r\n"
     ]
    }
   ],
   "source": [
    "import os\n",
    "\n",
    "# Create output folder\n",
    "os.makedirs(\"curated_data\", exist_ok=True)\n",
    "\n",
    "# Save all 4 tables as Parquet\n",
    "daily_temp.write.mode(\"overwrite\").parquet(\"curated_data/daily_temp\")\n",
    "hottest_stations.write.mode(\"overwrite\").parquet(\"curated_data/hottest_stations\")\n",
    "rainiest_days.write.mode(\"overwrite\").parquet(\"curated_data/rainiest_days\")\n",
    "daily_humidity.write.mode(\"overwrite\").parquet(\"curated_data/daily_humidity\")\n",
    "\n",
    "print(\"✅ All curated tables saved as Parquet!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "5922f25e-4c8c-4042-9714-4880c1c898f2",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "✅ All curated tables saved as CSV!\n"
     ]
    }
   ],
   "source": [
    "import os\n",
    "\n",
    "# Create output folder\n",
    "os.makedirs(\"curated_data\", exist_ok=True)\n",
    "\n",
    "# Save all 4 tables as CSV instead of Parquet\n",
    "daily_temp.toPandas().to_csv(\"curated_data/daily_temp.csv\", index=False)\n",
    "hottest_stations.toPandas().to_csv(\"curated_data/hottest_stations.csv\", index=False)\n",
    "rainiest_days.toPandas().to_csv(\"curated_data/rainiest_days.csv\", index=False)\n",
    "daily_humidity.toPandas().to_csv(\"curated_data/daily_humidity.csv\", index=False)\n",
    "\n",
    "print(\"✅ All curated tables saved as CSV!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "7d8ae01f-ed6d-4473-b09f-dd377ac0f5e3",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "✅ daily_humidity.csv — 199 bytes\n",
      "✅ daily_temp — 0 bytes\n",
      "✅ daily_temp.csv — 4197 bytes\n",
      "✅ hottest_stations.csv — 336 bytes\n",
      "✅ rainiest_days.csv — 136 bytes\n"
     ]
    }
   ],
   "source": [
    "import os\n",
    "\n",
    "files = os.listdir(\"curated_data\")\n",
    "for f in files:\n",
    "    size = os.path.getsize(f\"curated_data/{f}\")\n",
    "    print(f\"✅ {f} — {size} bytes\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "7a144be6-e9c2-4062-8823-086da49a74f7",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "✅ Uploaded raw_data/air_temperature_raw.json → s3://dataprojrect/raw/weather/air_temperature_raw.json\n",
      "✅ Uploaded raw_data/relative_humidity_raw.json → s3://dataprojrect/raw/weather/relative_humidity_raw.json\n",
      "✅ Uploaded raw_data/rainfall_raw.json → s3://dataprojrect/raw/weather/rainfall_raw.json\n",
      "✅ Uploaded raw_data/wind_speed_raw.json → s3://dataprojrect/raw/weather/wind_speed_raw.json\n",
      "✅ Uploaded raw_data/wind_direction_raw.json → s3://dataprojrect/raw/weather/wind_direction_raw.json\n",
      "✅ Uploaded daily_temp.csv → s3://dataprojrect/curated/weather/daily_temp.csv\n",
      "✅ Uploaded hottest_stations.csv → s3://dataprojrect/curated/weather/hottest_stations.csv\n",
      "✅ Uploaded rainiest_days.csv → s3://dataprojrect/curated/weather/rainiest_days.csv\n",
      "✅ Uploaded daily_humidity.csv → s3://dataprojrect/curated/weather/daily_humidity.csv\n",
      "\n",
      "✅ All files uploaded to S3!\n"
     ]
    }
   ],
   "source": [
    "import boto3\n",
    "import os\n",
    "\n",
    "# Replace these with your actual keys from the CSV file you downloaded\n",
    "AWS_ACCESS_KEY = \"ACCESS_KEY\"\n",
    "AWS_SECRET_KEY = \"SECRET_KEY\"\n",
    "AWS_BUCKET = \"bucket name\"\n",
    "AWS_REGION = \"ap-south-1\"\n",
    "\n",
    "# Connect to S3\n",
    "s3 = boto3.client(\n",
    "    \"s3\",\n",
    "    aws_access_key_id=AWS_ACCESS_KEY,\n",
    "    aws_secret_access_key=AWS_SECRET_KEY,\n",
    "    region_name=AWS_REGION\n",
    ")\n",
    "\n",
    "# Upload raw JSON files\n",
    "for metric in [\"air_temperature\", \"relative_humidity\", \"rainfall\", \"wind_speed\", \"wind_direction\"]:\n",
    "    filename = f\"raw_data/{metric}_raw.json\"\n",
    "    s3_key = f\"raw/weather/{metric}_raw.json\"\n",
    "    s3.upload_file(filename, AWS_BUCKET, s3_key)\n",
    "    print(f\"✅ Uploaded {filename} → s3://{AWS_BUCKET}/{s3_key}\")\n",
    "\n",
    "# Upload curated CSV files\n",
    "for filename in [\"daily_temp.csv\", \"hottest_stations.csv\", \"rainiest_days.csv\", \"daily_humidity.csv\"]:\n",
    "    s3_key = f\"curated/weather/{filename}\"\n",
    "    s3.upload_file(f\"curated_data/{filename}\", AWS_BUCKET, s3_key)\n",
    "    print(f\"✅ Uploaded {filename} → s3://{AWS_BUCKET}/{s3_key}\")\n",
    "\n",
    "print(\"\\n✅ All files uploaded to S3!\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "34153ab7-e0e2-4e62-a0e0-dea89679f75f",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "📁 Files in S3 bucket:\n",
      "\n",
      "✅ curated/weather/daily_humidity.csv — 0.19 KB\n",
      "✅ curated/weather/daily_temp.csv — 4.1 KB\n",
      "✅ curated/weather/hottest_stations.csv — 0.33 KB\n",
      "✅ curated/weather/rainiest_days.csv — 0.13 KB\n",
      "✅ raw/weather/air_temperature_raw.json — 118.11 KB\n",
      "✅ raw/weather/rainfall_raw.json — 427.28 KB\n",
      "✅ raw/weather/relative_humidity_raw.json — 117.73 KB\n",
      "✅ raw/weather/wind_direction_raw.json — 113.86 KB\n",
      "✅ raw/weather/wind_speed_raw.json — 115.66 KB\n"
     ]
    }
   ],
   "source": [
    "# List all files in S3 bucket\n",
    "response = s3.list_objects_v2(Bucket=AWS_BUCKET)\n",
    "\n",
    "print(\"📁 Files in S3 bucket:\\n\")\n",
    "for obj in response[\"Contents\"]:\n",
    "    size_kb = round(obj[\"Size\"] / 1024, 2)\n",
    "    print(f\"✅ {obj['Key']} — {size_kb} KB\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d9c669ff-4ffb-43f4-8f45-e52ed85cee5f",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.15"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
