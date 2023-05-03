# Developer Guide

## Scala Formatting Guidelines


For Scala code, flint use [spark scalastyle](https://github.com/apache/spark/blob/master/scalastyle-config.xml). Before submitting the PR, 
make sure to use "scalafmtAll" to format the code. read more in [scalafmt sbt](https://scalameta.org/scalafmt/docs/installation.html#sbt)
```
sbt scalafmtAll
```
The code style is automatically checked, but users can also manually check it.
```
sbt sbt scalastyle
```
For IntelliJ user, read more in [scalafmt IntelliJ](https://scalameta.org/scalafmt/docs/installation.html#intellij) to integrate 
scalafmt with IntelliJ
