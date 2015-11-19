Conditional Mutation Performance Test
===================================

Running Accumulo
----------------

```
mvn exec:java -Dexec.mainClass=cmpt.MiniRunner -Dexec.args="/tmp/mac accumulo.properties" -Dexec.classpathScope=test
```

Running Test
------------

```
mvn exec:java -Dexec.mainClass=cmpt.Test1 -Dexec.args=accumulo.properties
```