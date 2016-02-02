```
                               _               _             _                    _           _ 
                              | |             | |           | |                  (_)         | |
  ___   _ __     __ _   _ __  | | __  ______  | |_   _   _  | |_    ___    _ __   _    __ _  | |
 / __| | '_ \   / _` | | '__| | |/ / |______| | __| | | | | | __|  / _ \  | '__| | |  / _` | | |
 \__ \ | |_) | | (_| | | |    |   <           | |_  | |_| | | |_  | (_) | | |    | | | (_| | | |
 |___/ | .__/   \__,_| |_|    |_|\_\           \__|  \__,_|  \__|  \___/  |_|    |_|  \__,_| |_|
       | |                                                                                      
       |_|                                                                                      
```

This tutorial provides a quick introduction to using Spark. It demonstrates the basic functionality of RDD and DataFrame API

#### Initializing Spark

```scala
val conf = new SparkConf().setAppName(appName).setMaster(master)
new SparkContext(conf)
```

Check [SparkCommon](src/main/scala/com/tutorial/utils/SparkCommon.scala)

`Note:` Only one SparkContext may be active per JVM. You must stop() the active SparkContext before creating a new one.



