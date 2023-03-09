package com.wiredforcode.gradle.spawn

import org.gradle.api.GradleException
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskAction

class SpawnProcessTask extends DefaultSpawnTask {
    @Input
    String command
    @Input
    String ready
    @Internal
    List<Closure> outputActions = new ArrayList<Closure>()

    @Input
    Map<String, String> environmentVariables = new HashMap<String, String>()

    void environmentVariable(String key, Object value) {
        environmentVariables.put(key, String.valueOf(value))
    }

    SpawnProcessTask() {
        description = "Spawn a new server process in the background."
    }

    void withOutput(Closure outputClosure) {
        outputActions.add(outputClosure)
    }

    @TaskAction
    void spawn() {
        if (!(command && ready)) {
            throw new GradleException("Ensure that mandatory fields command and ready are set.")
        }

        def pidFile = getPidFile()
        if (pidFile.exists()) throw new GradleException("Server already running!")

        def process = buildProcess(directory, command)
        waitToProcessReadyOrClosed(process)
    }

    private void waitToProcessReadyOrClosed(Process process) {
        boolean isReady = waitUntilIsReadyOrEnd(process)
        if (isReady) {
            stampLockFile(pidFile, process)
        } else {
            checkForAbnormalExit(process)
        }
    }

    private void checkForAbnormalExit(Process process) {
        try {
            process.waitFor()
            def exitValue = process.exitValue()
            if (exitValue) {
                throw new GradleException("The process terminated unexpectedly - status code ${exitValue}")
            }
        } catch (IllegalThreadStateException ignored) {
        }
    }

    private boolean waitUntilIsReadyOrEnd(Process process) {
        def line
        def reader = new BufferedReader(new InputStreamReader(process.getInputStream()))
        boolean isReady = false
        while (!isReady && (line = reader.readLine()) != null) {
            logger.quiet line
            runOutputActions(line)
            if (line.contains(ready)) {
                logger.quiet "$command is ready."
                isReady = true
            }
        }
        isReady
    }

    def runOutputActions(String line) {
        outputActions.each { Closure<String> outputAction ->
            outputAction.call(line)
        }
    }

    private Process buildProcess(String directory, String command) {
        def builder = new ProcessBuilder(command.split(' '))
        builder.redirectErrorStream(true)
        builder.environment().putAll(environmentVariables)
        builder.directory(new File(directory))
        builder.start()
    }

    private File stampLockFile(File pidFile, Process process) {
        pidFile << extractPidFromProcess(process)
    }

    private int extractPidFromProcess(Process process) {
        def pid
        try {
            // works since java 9
            def pidMethod = process.class.getDeclaredMethod('pid')
            pidMethod.setAccessible(true)
            pid = pidMethod.invoke(process)
        } catch (ignored) {
            // fallback to UNIX-only implementation
            def pidField = process.class.getDeclaredField('pid')
            pidField.accessible = true
            pid = pidField.getInt(process)
        }
        return pid
    }
}
